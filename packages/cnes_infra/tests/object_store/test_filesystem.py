"""Conformidade do armazenamento de objetos no filesystem."""

import errno
import fcntl
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from threading import Event, Thread
from typing import Any
from unittest.mock import MagicMock

import pytest

from cnes_domain.control_plane.errors import Conflict
from cnes_infra.object_store.filesystem import FilesystemObjectStore
from packages.cnes_infra.tests.contracts.clock import MutableClock
from packages.cnes_infra.tests.contracts.object_store_contract import (
    ObjectStoreCase,
    object_store_cases,
)

_DURABLE_BOUNDARIES = ("temporary_created", "file_fsynced", "destination_linked") + (
    "directory_fsynced",
    "temporary_unlinked",
    "directory_final_fsynced",
)
_OWNER_XATTR = "user.cnes_object_store_destination"


class _SimulatedCrash(RuntimeError):
    pass


@dataclass(slots=True)
class _CrashAt:
    boundary: str
    triggered: bool = False

    def __call__(self, boundary: str) -> None:
        if boundary == self.boundary and not self.triggered:
            self.triggered = True
            raise _SimulatedCrash(f"boundary={boundary}")


def _adapter_temporaries(root: Path) -> tuple[Path, ...]:
    return tuple(root.rglob(".cnes-object-store-*.tmp"))


def _process_put(root: str, body: bytes, controls: tuple[Any, ...]) -> None:
    barrier, results, destination_linked, reader_done = controls
    expected = sha256(body).hexdigest()

    def wait_for_reader(boundary: str) -> None:
        if boundary == "destination_linked":
            destination_linked.set()
            if not reader_done.wait(timeout=5):
                raise TimeoutError("reader=timeout")

    barrier.wait()
    try:
        stat = FilesystemObjectStore(root, fault_injector=wait_for_reader).put(
            "raw/race", BytesIO(body), expected
        )
        results.put(("ok", stat.sha256))
    except Conflict:
        results.put(("conflict", expected))


def _process_paused_put(root: str, body: bytes, controls: tuple[Any, ...]) -> None:
    reached, release, results = controls

    def pause(boundary: str) -> None:
        if boundary == "temporary_created":
            reached.set()
            if not release.wait(timeout=5):
                raise TimeoutError("release=timeout")

    try:
        digest = sha256(body).hexdigest()
        FilesystemObjectStore(root, fault_injector=pause).put("raw/locked", BytesIO(body), digest)
        results.put("ok")
    except Exception as error:
        results.put(type(error).__name__)


def _read_during_publication(root: str, controls: tuple[Any, ...]) -> None:
    reader_ready, destination_linked, reader_done, observed = controls
    reader_ready.set()
    if not destination_linked.wait(timeout=5):
        reader_done.set()
        return
    try:
        with FilesystemObjectStore(root).open("raw/race") as stream:
            observed.append(stream.read())
    except FileNotFoundError:
        pass
    finally:
        reader_done.set()


@pytest.mark.parametrize("case", object_store_cases(), ids=lambda case: case.name)
def test_cumpre_contrato_compartilhado(
    case: ObjectStoreCase, tmp_path_factory: pytest.TempPathFactory
) -> None:
    root = tmp_path_factory.mktemp(case.name)
    clock = MutableClock(datetime(2026, 7, 15, tzinfo=UTC))
    adapter = FilesystemObjectStore(root)
    case.run(adapter, clock)
    adapter.delete("raw/ausente")
    assert adapter.stat("raw/ausente") is None


@pytest.mark.linux_only
def test_fsynca_pai_de_cada_diretorio_criado(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    adapter = FilesystemObjectStore(tmp_path)
    observed: list[Path] = []
    real_fsync = os.fsync

    def observe_fsync(descriptor: int) -> None:
        target = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        if target.is_dir():
            observed.append(target)
        real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", observe_fsync)
    body = b"conteudo"
    adapter.put("nivel-1/nivel-2/dados.parquet", BytesIO(body), sha256(body).hexdigest())
    assert observed[:3] == [tmp_path, tmp_path / "nivel-1", tmp_path / "nivel-1/nivel-2"]


@pytest.mark.parametrize("operation", ["put", "promote"])
@pytest.mark.parametrize("boundary", _DURABLE_BOUNDARIES)
def test_recupera_cada_fronteira_duravel_sem_apagar_arquivo_alheio(
    boundary: str, operation: str, tmp_path: Path
) -> None:
    body = b"conteudo-completo"
    expected = sha256(body).hexdigest()
    unrelated = tmp_path / ".arquivo-do-usuario"
    unrelated.write_bytes(b"preservar")
    source_key = "staging/dados.parquet"
    FilesystemObjectStore(tmp_path).put(source_key, BytesIO(body), expected)
    crashing = FilesystemObjectStore(tmp_path, fault_injector=_CrashAt(boundary))

    def perform() -> None:
        if operation == "put":
            crashing.put("raw/dados.parquet", BytesIO(body), expected)
        else:
            crashing.promote(source_key, "raw/dados.parquet", expected)

    with pytest.raises(_SimulatedCrash, match=f"boundary={boundary}"):
        perform()
    recovered = FilesystemObjectStore(tmp_path)
    assert _adapter_temporaries(tmp_path) == ()
    current = recovered.stat("raw/dados.parquet")
    assert current is None or (current.size_bytes, current.sha256) == (len(body), expected)
    assert recovered.put("raw/dados.parquet", BytesIO(body), expected).sha256 == expected
    with recovered.open("raw/dados.parquet") as stream:
        assert stream.read() == body
    with recovered.open(source_key) as stream:
        assert stream.read() == body
    assert unrelated.read_bytes() == b"preservar"


def test_reabertura_ignora_temporario_removido_durante_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    crashing = FilesystemObjectStore(tmp_path, fault_injector=_CrashAt("file_fsynced"))
    with pytest.raises(_SimulatedCrash, match="boundary=file_fsynced"):
        crashing.put("raw/dados.parquet", BytesIO(b"abandonado"), sha256(b"abandonado").hexdigest())
    temporary = _adapter_temporaries(tmp_path)[0]
    real_lstat = Path.lstat
    removed: list[Path] = []

    def vanish_before_lstat(path: Path) -> os.stat_result:
        if path == temporary:
            removed.append(path)
            path.unlink()
        return real_lstat(path)

    monkeypatch.setattr(Path, "lstat", vanish_before_lstat)
    FilesystemObjectStore(tmp_path)
    assert removed == [temporary]


def test_crash_pre_marker_nao_deixa_temporario(tmp_path: Path) -> None:
    key = "raw/dados.parquet"
    crashing = FilesystemObjectStore(
        tmp_path, fault_injector=_CrashAt("temporary_created_before_ownership")
    )
    body = b"conteudo"

    with pytest.raises(_SimulatedCrash, match="boundary=temporary_created_before_ownership"):
        crashing.put(key, BytesIO(body), sha256(body).hexdigest())
    assert _adapter_temporaries(tmp_path) == ()


def test_nova_escrita_recupera_temporario_abandonado_no_mesmo_adapter(tmp_path: Path) -> None:
    body = b"conteudo"
    digest = sha256(body).hexdigest()
    adapter = FilesystemObjectStore(tmp_path, fault_injector=_CrashAt("file_fsynced"))
    with pytest.raises(_SimulatedCrash, match="boundary=file_fsynced"):
        adapter.put("raw/dados.parquet", BytesIO(body), digest)
    assert _adapter_temporaries(tmp_path)
    adapter.put("raw/dados.parquet", BytesIO(body), digest)
    assert _adapter_temporaries(tmp_path) == ()
    with adapter.open("raw/dados.parquet") as stream:
        assert stream.read() == body


@pytest.mark.parametrize("alias_kind", ["valid_key", "hard_link", "symlink"])
@pytest.mark.parametrize("recovery", ["startup", "pre_write"])
def test_recuperacao_preserva_alias_legal(alias_kind: str, recovery: str, tmp_path: Path) -> None:
    key = "raw/destino.parquet"
    body = b"objeto-valido"
    digest = sha256(body).hexdigest()
    namespace = sha256(key.encode()).hexdigest()
    alias = tmp_path / "raw" / f".cnes-object-store-{namespace}-writer.tmp"
    adapter = FilesystemObjectStore(tmp_path)
    if alias_kind == "valid_key":
        adapter.put(alias.relative_to(tmp_path).as_posix(), BytesIO(body), digest)
    elif alias_kind == "hard_link":
        adapter.put(key, BytesIO(body), digest)
        os.link(tmp_path / key, alias)
    else:
        crashing = FilesystemObjectStore(tmp_path, fault_injector=_CrashAt("destination_linked"))
        with pytest.raises(_SimulatedCrash, match="boundary=destination_linked"):
            crashing.put(key, BytesIO(body), digest)
        alias = _adapter_temporaries(tmp_path)[0]
        alias.unlink()
        alias.symlink_to(Path(key).name)
    if recovery == "startup":
        FilesystemObjectStore(tmp_path)
    else:
        adapter.put(key, BytesIO(body), digest)
    assert alias.is_symlink() == (alias_kind == "symlink")
    assert alias.read_bytes() == body


def test_reabertura_preserva_lookalikes_sem_ownership_valido(tmp_path: Path) -> None:
    directory = tmp_path / "raw"
    directory.mkdir()
    different_parent_owner = "other/owner.parquet"
    different_parent_digest = sha256(different_parent_owner.encode()).hexdigest()
    mismatch_owner = "raw/owner.parquet"
    candidates = {
        directory / ".cnes-object-store-curto.tmp": None,
        directory / f".cnes-object-store-{'b' * 64}-writer.tmp": None,
        directory / f".cnes-object-store-{'c' * 64}-writer.tmp": b"\xff",
        directory / f".cnes-object-store-{'d' * 64}-writer.tmp": b"../invalid\0writer",
        directory / f".cnes-object-store-{different_parent_digest}-writer.tmp": (
            f"{different_parent_owner}\0writer".encode()
        ),
        directory / f".cnes-object-store-{'e' * 64}-writer.tmp": (
            f"{mismatch_owner}\0writer".encode()
        ),
    }
    for candidate, owner in candidates.items():
        candidate.write_bytes(b"preservar")
        if owner is not None:
            os.setxattr(candidate, _OWNER_XATTR, owner)

    FilesystemObjectStore(tmp_path)

    assert all(candidate.read_bytes() == b"preservar" for candidate in candidates)


def test_reabertura_propaga_erro_ao_ler_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate = tmp_path / f".cnes-object-store-{'f' * 64}-writer.tmp"
    candidate.write_bytes(b"preservar")
    monkeypatch.setattr(
        os, "getxattr", MagicMock(side_effect=PermissionError(errno.EACCES, "xattr=denied"))
    )
    with pytest.raises(PermissionError, match="xattr=denied"):
        FilesystemObjectStore(tmp_path)

    assert candidate.read_bytes() == b"preservar"


@pytest.mark.parametrize("failure", ["ownership", "read", "write", "flush", "content_fsync"])
def test_falha_ordinaria_de_staging_remove_temporario_e_fsynca_diretorio(
    failure: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    adapter = FilesystemObjectStore(tmp_path)
    body = MagicMock(wraps=BytesIO(b"conteudo"))
    if failure == "ownership":
        monkeypatch.setattr(
            os,
            "setxattr",
            MagicMock(side_effect=OSError(errno.ENOTSUP, "staging=failed")),
        )
    if failure == "read":
        body.read.side_effect = OSError(errno.EIO, "staging=failed")
    real_fdopen = os.fdopen

    def failing_fdopen(*args: Any, **kwargs: Any) -> MagicMock:
        stream = real_fdopen(*args, **kwargs)
        writer = MagicMock(wraps=stream)
        writer.__enter__.return_value = writer
        writer.__exit__.side_effect = stream.__exit__
        writer.fileno.side_effect = stream.fileno
        getattr(writer, failure).side_effect = OSError(errno.EIO, "staging=failed")
        return writer

    if failure in {"write", "flush"}:
        monkeypatch.setattr(os, "fdopen", failing_fdopen)
    real_fsync = os.fsync
    regular_fsyncs = 0
    directory_fsyncs = 0

    def failing_fsync(descriptor: int) -> None:
        nonlocal directory_fsyncs, regular_fsyncs
        target = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        if target.is_dir():
            directory_fsyncs += target == tmp_path / "raw"
        else:
            regular_fsyncs += 1
            if failure == "content_fsync" and regular_fsyncs == 2:
                raise OSError(errno.EIO, "staging=failed")
        real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", failing_fsync)
    with pytest.raises(OSError, match="staging=failed"):
        adapter.put("raw/dados.parquet", body, sha256(b"conteudo").hexdigest())

    assert _adapter_temporaries(tmp_path) == ()
    assert directory_fsyncs == (0 if failure == "ownership" else 2)


@pytest.mark.parametrize("destination_kind", ["file", "directory"])
def test_recuperacao_preserva_destino_existente(destination_kind: str, tmp_path: Path) -> None:
    losing = b"perdedor"
    winner = b"vencedor"
    key = "raw/dados.parquet"
    crashing = FilesystemObjectStore(tmp_path, fault_injector=_CrashAt("file_fsynced"))
    with pytest.raises(_SimulatedCrash):
        crashing.put(key, BytesIO(losing), sha256(losing).hexdigest())
    destination = tmp_path / key
    if destination_kind == "file":
        destination.write_bytes(winner)
        message = "object=immutable"
    else:
        destination.mkdir()
        message = "destination=invalid"
    with pytest.raises(Conflict, match=message):
        FilesystemObjectStore(tmp_path).put(key, BytesIO(losing), sha256(losing).hexdigest())
    assert (
        destination.read_bytes() == winner if destination_kind == "file" else destination.is_dir()
    )
    if destination_kind == "file":
        assert _adapter_temporaries(tmp_path) == ()


def test_remove_staging_quando_sha256_diverge(tmp_path: Path) -> None:
    adapter = FilesystemObjectStore(tmp_path)

    with pytest.raises(ValueError, match="sha256=mismatch"):
        adapter.put("raw/invalido", BytesIO(b"conteudo"), sha256(b"outro").hexdigest())

    assert adapter.stat("raw/invalido") is None
    assert _adapter_temporaries(tmp_path) == ()


def test_rejeita_chave_invalida_com_erro_estruturado(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="object_key=invalid"):
        FilesystemObjectStore(tmp_path).stat("../fora")


@pytest.mark.parametrize("operation", ["put", "open", "stat", "delete"])
def test_rejeita_componente_symlink_sem_acessar_fora_do_root(
    operation: str, tmp_path: Path
) -> None:
    root = tmp_path / "store"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    external = outside / "dados.parquet"
    external.write_bytes(b"externo")
    (root / "link").symlink_to(outside, target_is_directory=True)
    adapter = FilesystemObjectStore(root)
    body = b"novo"
    actions = {
        "put": lambda: adapter.put("link/dados.parquet", BytesIO(body), sha256(body).hexdigest()),
        "open": lambda: adapter.open("link/dados.parquet").close(),
        "stat": lambda: adapter.stat("link/dados.parquet"),
        "delete": lambda: adapter.delete("link/dados.parquet"),
    }

    with pytest.raises(ValueError, match="object_path=symlink"):
        actions[operation]()

    assert external.read_bytes() == b"externo"
    assert tuple(outside.glob(".cnes-object-store-*")) == ()


@pytest.mark.parametrize("link_kind", ["staging", "publication"])
def test_nao_faz_fallback_quando_link_e_incompativel(
    link_kind: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    body = b"conteudo"
    adapter = FilesystemObjectStore(tmp_path)
    if link_kind == "staging":
        directory = tmp_path / "raw"
        directory.mkdir()
        namespace = sha256(b"raw/sem-fallback").hexdigest()
        attacker = directory / f".cnes-object-store-{namespace}-attacker.tmp"
        attacker.write_bytes(b"preservar")
        monkeypatch.setattr("cnes_infra.object_store.filesystem.token_hex", lambda size: "attacker")
    else:
        attacker = None
        error = OSError(errno.EPERM, "hard_link=incompatible")
        monkeypatch.setattr(os, "link", MagicMock(side_effect=error))
    with pytest.raises(OSError):
        adapter.put("raw/sem-fallback", BytesIO(body), sha256(body).hexdigest())
    assert adapter.stat("raw/sem-fallback") is None
    assert _adapter_temporaries(tmp_path) == (() if attacker is None else (attacker,))
    assert attacker is None or attacker.read_bytes() == b"preservar"


@pytest.mark.linux_only
def test_lock_do_destino_bloqueia_outro_processo_durante_staging(tmp_path: Path) -> None:
    import multiprocessing

    context = multiprocessing.get_context("spawn")
    reached = context.Event()
    release = context.Event()
    results = context.Queue()
    process = context.Process(
        target=_process_paused_put,
        args=(str(tmp_path), b"conteudo", (reached, release, results)),
    )
    process.start()
    assert reached.wait(timeout=5)
    lock_path = next(tmp_path.rglob(".cnes-object-store-*.lock"))

    with lock_path.open("a+b") as lock:
        with pytest.raises(BlockingIOError):
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    release.set()
    process.join(timeout=10)
    assert process.exitcode == 0
    assert results.get(timeout=2) == "ok"


@pytest.mark.linux_only
@pytest.mark.parametrize("identical", [True, False], ids=["identicos", "conflitantes"])
def test_corrida_entre_processos_publica_um_destino_completo(
    identical: bool, tmp_path: Path
) -> None:
    import multiprocessing

    context = multiprocessing.get_context("spawn")
    bodies = (b"a" * (2 * 1024 * 1024),) * 2 if identical else (b"a" * 1024, b"b" * 2048)
    barrier = context.Barrier(3)
    results = context.Queue()
    destination_linked = context.Event()
    reader_done = context.Event()
    reader_ready = Event()
    observed: list[bytes] = []
    reader = Thread(
        target=_read_during_publication,
        args=(str(tmp_path), (reader_ready, destination_linked, reader_done, observed)),
    )
    reader.start()
    assert reader_ready.wait(timeout=5)
    processes = [
        context.Process(
            target=_process_put,
            args=(str(tmp_path), body, (barrier, results, destination_linked, reader_done)),
        )
        for body in bodies
    ]
    for process in processes:
        process.start()
    barrier.wait()
    reader.join(timeout=10)
    assert not reader.is_alive()
    assert observed
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    outcomes = [results.get(timeout=2)[0] for _ in processes]
    with FilesystemObjectStore(tmp_path).open("raw/race") as stream:
        published = stream.read()
    assert published in bodies
    assert outcomes.count("ok") == (2 if identical else 1)
    assert outcomes.count("conflict") == (0 if identical else 1)
    assert all(content in bodies for content in observed)
    assert sha256(published).hexdigest() == FilesystemObjectStore(tmp_path).stat("raw/race").sha256
    assert _adapter_temporaries(tmp_path) == ()
