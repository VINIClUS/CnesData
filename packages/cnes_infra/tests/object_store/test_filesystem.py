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

import pytest

from cnes_domain.control_plane.errors import Conflict
from cnes_infra.object_store.filesystem import FilesystemObjectStore
from packages.cnes_infra.tests.contracts.clock import MutableClock
from packages.cnes_infra.tests.contracts.object_store_contract import (
    ObjectStoreCase,
    object_store_cases,
)

_DURABLE_BOUNDARIES = (
    "temporary_created",
    "file_fsynced",
    "destination_linked",
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

    case.run(FilesystemObjectStore(root), clock)


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
    if operation == "promote":
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
    current = recovered.stat("raw/dados.parquet")
    assert current is None or (current.size_bytes, current.sha256) == (len(body), expected)
    assert recovered.put("raw/dados.parquet", BytesIO(body), expected).sha256 == expected
    with recovered.open("raw/dados.parquet") as stream:
        assert stream.read() == body
    assert _adapter_temporaries(tmp_path) == ()
    assert unrelated.read_bytes() == b"preservar"


@pytest.mark.parametrize("boundary", ["temporary_created", "file_fsynced"])
def test_reabre_e_remove_temporario_pre_publicacao_sem_operar_na_chave(
    boundary: str, tmp_path: Path
) -> None:
    valid_key = "raw/valido.parquet"
    valid_body = b"destino-valido"
    adapter = FilesystemObjectStore(tmp_path)
    adapter.put(valid_key, BytesIO(valid_body), sha256(valid_body).hexdigest())
    unrelated = tmp_path / ".arquivo-do-usuario"
    unrelated.write_bytes(b"preservar")
    malformed = (
        tmp_path / ".cnes-object-store-nao-relacionado.tmp",
        tmp_path / f".cnes-object-store-{'a' * 64}-.tmp",
        tmp_path / f".cnes-object-store-{'A' * 64}-token.tmp",
    )
    for hidden in malformed:
        hidden.write_bytes(b"preservar")
    crashing = FilesystemObjectStore(tmp_path, fault_injector=_CrashAt(boundary))
    body = b"abandonado"
    digest = sha256(body).hexdigest()

    with pytest.raises(_SimulatedCrash, match=f"boundary={boundary}"):
        crashing.put("raw/abandonado.parquet", BytesIO(body), digest)

    recoverable = tuple(path for path in _adapter_temporaries(tmp_path) if path not in malformed)
    assert recoverable
    FilesystemObjectStore(tmp_path)

    assert all(not path.exists() for path in recoverable)
    assert (tmp_path / valid_key).read_bytes() == valid_body
    assert unrelated.read_bytes() == b"preservar"
    assert all(hidden.read_bytes() == b"preservar" for hidden in malformed)


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


def test_reabertura_preserva_objeto_valido_com_nome_de_temporario(tmp_path: Path) -> None:
    other_key = "raw/outro.parquet"
    digest = sha256(other_key.encode()).hexdigest()
    valid_key = f"raw/.cnes-object-store-{digest}-writer.tmp"
    body = b"objeto-valido"
    adapter = FilesystemObjectStore(tmp_path)
    adapter.put(valid_key, BytesIO(body), sha256(body).hexdigest())

    reopened = FilesystemObjectStore(tmp_path)

    with reopened.open(valid_key) as stream:
        assert stream.read() == body


def test_pre_write_preserva_objeto_valido_com_nome_de_temporario(tmp_path: Path) -> None:
    destination_key = "raw/destino.parquet"
    digest = sha256(destination_key.encode()).hexdigest()
    valid_key = f"raw/.cnes-object-store-{digest}-writer.tmp"
    valid_body = b"objeto-valido"
    destination_body = b"novo-destino"
    adapter = FilesystemObjectStore(tmp_path)
    adapter.put(valid_key, BytesIO(valid_body), sha256(valid_body).hexdigest())

    adapter.put(
        destination_key,
        BytesIO(destination_body),
        sha256(destination_body).hexdigest(),
    )

    with adapter.open(valid_key) as stream:
        assert stream.read() == valid_body


def test_reabertura_preserva_lookalikes_sem_ownership_valido(tmp_path: Path) -> None:
    directory = tmp_path / "raw"
    directory.mkdir()
    different_parent_owner = "other/owner.parquet"
    different_parent_digest = sha256(different_parent_owner.encode()).hexdigest()
    mismatch_owner = "raw/owner.parquet"
    candidates = {
        directory / f".cnes-object-store-{'b' * 64}-writer.tmp": None,
        directory / f".cnes-object-store-{'c' * 64}-writer.tmp": b"\xff",
        directory / f".cnes-object-store-{'d' * 64}-writer.tmp": b"../invalid",
        directory / f".cnes-object-store-{different_parent_digest}-writer.tmp": (
            different_parent_owner.encode()
        ),
        directory / f".cnes-object-store-{'e' * 64}-writer.tmp": mismatch_owner.encode(),
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

    def deny_xattr(path: os.PathLike[str], attribute: str) -> bytes:
        raise PermissionError(errno.EACCES, "xattr=denied")

    monkeypatch.setattr(os, "getxattr", deny_xattr)

    with pytest.raises(PermissionError, match="xattr=denied"):
        FilesystemObjectStore(tmp_path)

    assert candidate.read_bytes() == b"preservar"


def test_falha_ao_marcar_ownership_remove_temporario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    adapter = FilesystemObjectStore(tmp_path)

    def reject_xattr(descriptor: int, attribute: str, value: bytes) -> None:
        raise OSError(errno.ENOTSUP, "xattr=unsupported")

    monkeypatch.setattr(os, "setxattr", reject_xattr)

    with pytest.raises(OSError, match="xattr=unsupported"):
        adapter.put("raw/dados.parquet", BytesIO(b"conteudo"), sha256(b"conteudo").hexdigest())

    assert adapter.stat("raw/dados.parquet") is None
    assert _adapter_temporaries(tmp_path) == ()


def test_remove_temporario_perdedor_sem_tocar_destino_valido(tmp_path: Path) -> None:
    losing = b"perdedor"
    winner = b"vencedor"
    key = "raw/dados.parquet"
    crashing = FilesystemObjectStore(tmp_path, fault_injector=_CrashAt("file_fsynced"))

    with pytest.raises(_SimulatedCrash):
        crashing.put(key, BytesIO(losing), sha256(losing).hexdigest())
    destination = tmp_path / key
    destination.write_bytes(winner)

    recovered = FilesystemObjectStore(tmp_path)
    with pytest.raises(Conflict, match="object=immutable"):
        recovered.put(key, BytesIO(losing), sha256(losing).hexdigest())

    assert destination.read_bytes() == winner
    assert _adapter_temporaries(tmp_path) == ()


def test_rejeita_destino_invalido_sem_remove_lo_na_recuperacao(tmp_path: Path) -> None:
    body = b"conteudo"
    key = "raw/dados.parquet"
    crashing = FilesystemObjectStore(tmp_path, fault_injector=_CrashAt("file_fsynced"))

    with pytest.raises(_SimulatedCrash):
        crashing.put(key, BytesIO(body), sha256(body).hexdigest())
    destination = tmp_path / key
    destination.mkdir()

    with pytest.raises(Conflict, match="destination=invalid"):
        FilesystemObjectStore(tmp_path).put(key, BytesIO(body), sha256(body).hexdigest())

    assert destination.is_dir()


def test_remove_staging_quando_sha256_diverge(tmp_path: Path) -> None:
    adapter = FilesystemObjectStore(tmp_path)

    with pytest.raises(ValueError, match="sha256=mismatch"):
        adapter.put("raw/invalido", BytesIO(b"conteudo"), sha256(b"outro").hexdigest())

    assert adapter.stat("raw/invalido") is None
    assert _adapter_temporaries(tmp_path) == ()


def test_rejeita_chave_invalida_com_erro_estruturado(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="object_key=invalid"):
        FilesystemObjectStore(tmp_path).stat("../fora")


def test_nao_faz_fallback_quando_hard_link_e_incompativel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    body = b"conteudo"
    adapter = FilesystemObjectStore(tmp_path)

    def reject_link(source: Path, destination: Path) -> None:
        raise OSError(errno.EPERM, "hard_link=incompatible")

    monkeypatch.setattr(os, "link", reject_link)

    with pytest.raises(OSError, match="hard_link=incompatible"):
        adapter.put("raw/sem-fallback", BytesIO(body), sha256(body).hexdigest())

    assert adapter.stat("raw/sem-fallback") is None
    assert _adapter_temporaries(tmp_path) == ()


def test_delete_de_chave_ausente_e_idempotente(tmp_path: Path) -> None:
    adapter = FilesystemObjectStore(tmp_path)

    adapter.delete("raw/ausente")

    assert adapter.stat("raw/ausente") is None


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
