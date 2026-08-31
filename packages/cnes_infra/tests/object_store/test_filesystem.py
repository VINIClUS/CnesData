"""Conformidade do armazenamento de objetos no filesystem."""

import errno
import fcntl
import multiprocessing
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
from packages.cnes_infra.tests.contracts import object_store_contract as contract
from packages.cnes_infra.tests.contracts.clock import MutableClock

_DURABLE_BOUNDARIES = (
    "temporary_created", "file_fsynced", "destination_linked", "directory_fsynced",
    "temporary_unlinked", "directory_final_fsynced",
)
_OWNER_XATTR = "user.cnes_object_store_destination"


class _SimulatedCrash(RuntimeError): ...


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


def _objects_directory(root: Path) -> Path:
    return next(path for path in root.rglob("objects") if path.is_dir())


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
            raise _SimulatedCrash("writer=crashed")
    try:
        digest = sha256(body).hexdigest()
        FilesystemObjectStore(root, fault_injector=pause).put("raw/locked", BytesIO(body), digest)
        results.put("ok")
    except Exception as error:
        results.put(type(error).__name__)


def _process_final(root: str, results: Any) -> None:
    key, adapter = "raw/dados.parquet", FilesystemObjectStore(root)
    operations = (("stat", b""), ("open", b""), ("delete", b""), ("put", b"a"), ("put", b"b"))
    for operation, body in operations:
        try:
            if operation == "put":
                adapter.put(key, BytesIO(body), sha256(body).hexdigest())
            else:
                result = getattr(adapter, operation)(key)
                if operation == "open":
                    result.close()
        except Exception as error:
            results.put((type(error).__name__, str(error)))
        else:
            results.put(("ok", ""))


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


@pytest.mark.parametrize("case", contract.object_store_cases(), ids=lambda case: case.name)
def test_contrato(case: contract.ObjectStoreCase, tmp_path_factory: pytest.TempPathFactory) -> None:
    root = tmp_path_factory.mktemp(case.name)
    with pytest.MonkeyPatch.context() as patch:
        patch.chdir(root.parent)
        adapter = FilesystemObjectStore(root.name)
        patch.chdir(root)
        case.run(adapter, MutableClock(datetime(2026, 7, 15, tzinfo=UTC)))
        adapter.delete("raw/ausente")


@pytest.mark.linux_only
def test_fsynca_ancestral(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "store"
    observed: list[Path] = []
    real_fsync = os.fsync
    def observe_fsync(descriptor: int) -> None:
        target = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        if target.is_dir():
            observed.append(target)
        real_fsync(descriptor)
    monkeypatch.setattr(os, "fsync", observe_fsync)
    FilesystemObjectStore(root)
    assert observed[:4] == [tmp_path.parent, tmp_path, root, next(root.iterdir())]
    observed.clear()
    FilesystemObjectStore(root)
    assert observed == [tmp_path, root, next(root.iterdir()), next(root.iterdir())]
    observed.clear()
    FilesystemObjectStore(root / "nested")
    assert observed[:2] == [tmp_path, root]


@pytest.mark.parametrize("operation", ["put", "promote"])
@pytest.mark.parametrize("boundary", _DURABLE_BOUNDARIES)
def test_recupera_fronteira_duravel(boundary: str, operation: str, tmp_path: Path) -> None:
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


def test_scan_ignora_temp_removido(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    crashing = FilesystemObjectStore(tmp_path, fault_injector=_CrashAt("file_fsynced"))
    with pytest.raises(_SimulatedCrash, match="boundary=file_fsynced"):
        crashing.put("raw/dados.parquet", BytesIO(b"abandonado"), sha256(b"abandonado").hexdigest())
    temporary = _adapter_temporaries(tmp_path)[0]
    removed: list[Path] = []
    real_open = os.open
    def vanish_before_open(
        path: Any, flags: int, mode: int = 0o777, *, dir_fd: int | None = None
    ) -> int:
        if path == temporary.name and not removed:
            removed.append(temporary)
            temporary.unlink()
        return real_open(path, flags, mode, dir_fd=dir_fd)
    monkeypatch.setattr(os, "open", vanish_before_open)
    FilesystemObjectStore(tmp_path)
    assert removed == [temporary]


def test_crash_pre_marker_nao_deixa_temporario(tmp_path: Path) -> None:
    crashing = FilesystemObjectStore(
        tmp_path, fault_injector=_CrashAt("temporary_created_before_ownership")
    )
    body = b"conteudo"
    with pytest.raises(_SimulatedCrash, match="boundary=temporary_created_before_ownership"):
        crashing.put("raw/dados.parquet", BytesIO(body), sha256(body).hexdigest())
    assert _adapter_temporaries(tmp_path) == ()


def test_nova_escrita_recupera_temporario_abandonado_no_mesmo_adapter(tmp_path: Path) -> None:
    body, digest = b"conteudo", sha256(b"conteudo").hexdigest()
    adapter = FilesystemObjectStore(tmp_path, fault_injector=_CrashAt("file_fsynced"))
    with pytest.raises(_SimulatedCrash, match="boundary=file_fsynced"):
        adapter.put("raw/dados.parquet", BytesIO(body), digest)
    adapter.put("raw/dados.parquet", BytesIO(body), digest)
    assert _adapter_temporaries(tmp_path) == ()
    with adapter.open("raw/dados.parquet") as stream:
        assert stream.read() == body


@pytest.mark.parametrize("alias_kind", ["hard_link", "symlink"])
@pytest.mark.parametrize("recovery", ["startup", "pre_write"])
def test_recuperacao_preserva_alias_legal(alias_kind: str, recovery: str, tmp_path: Path) -> None:
    key, body = "raw/destino.parquet", b"objeto-valido"
    digest = sha256(body).hexdigest()
    namespace = sha256(key.encode()).hexdigest()
    adapter = FilesystemObjectStore(tmp_path)
    alias = _objects_directory(tmp_path) / f".cnes-object-store-{namespace}-writer.tmp"
    if alias_kind == "hard_link":
        adapter.put(key, BytesIO(body), digest)
        os.link(_objects_directory(tmp_path) / namespace, alias)
    else:
        adapter.put(key, BytesIO(body), digest)
        alias.symlink_to(namespace)
    if recovery == "startup":
        adapter = FilesystemObjectStore(tmp_path)
    else:
        adapter.put(key, BytesIO(body), digest)
    assert (alias.is_symlink(), alias.read_bytes()) == (alias_kind == "symlink", body)


def test_reabertura_preserva_lookalikes_sem_ownership_valido(tmp_path: Path) -> None:
    FilesystemObjectStore(tmp_path)
    directory = _objects_directory(tmp_path)
    mismatch_owner, mismatch_digest = "raw/owner.parquet", sha256(b"raw/owner.parquet").hexdigest()
    candidates = {
        directory / f".cnes-object-store-{'b' * 64}-writer.tmp": None,
        directory / f".cnes-object-store-{'c' * 64}-writer.tmp": b"\xff",
        directory / f".cnes-object-store-{'d' * 64}-writer.tmp": b"../invalid\0writer",
        directory / f".cnes-object-store-{mismatch_digest}-.tmp": b"raw/owner.parquet\0writer",
    }
    for candidate, owner in candidates.items():
        candidate.write_bytes(b"preservar")
        if owner is not None:
            os.setxattr(candidate, _OWNER_XATTR, owner)
    fifo = directory / f".cnes-object-store-{'e' * 64}-writer.tmp"
    os.mkfifo(fifo)
    context = multiprocessing.get_context("spawn")
    process = context.Process(target=FilesystemObjectStore, args=(tmp_path,))
    process.start()
    process.join(timeout=1)
    if process.is_alive():
        process.terminate()
    process.join(timeout=1)
    assert process.exitcode == 0
    adapter = FilesystemObjectStore(tmp_path)
    adapter.put(mismatch_owner, BytesIO(b"novo"), sha256(b"novo").hexdigest())
    assert all(candidate.read_bytes() == b"preservar" for candidate in candidates)
    assert fifo.exists()


def test_propaga_erro_de_ownership(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    FilesystemObjectStore(tmp_path)
    candidate = _objects_directory(tmp_path) / f".cnes-object-store-{'f' * 64}-writer.tmp"
    candidate.write_bytes(b"preservar")
    real_open = os.open
    def deny_candidate(path: Any, *args: Any, **kwargs: Any) -> int:
        if path == candidate.name:
            raise PermissionError(errno.EACCES, "xattr=denied")
        return real_open(path, *args, **kwargs)
    monkeypatch.setattr(os, "open", deny_candidate)
    with pytest.raises(PermissionError, match="xattr=denied"):
        FilesystemObjectStore(tmp_path)
    monkeypatch.setattr(os, "open", real_open)
    monkeypatch.setattr(
        os, "getxattr", MagicMock(side_effect=PermissionError(errno.EACCES, "xattr=denied"))
    )
    with pytest.raises(PermissionError, match="xattr=denied"):
        FilesystemObjectStore(tmp_path)
    assert candidate.read_bytes() == b"preservar"


@pytest.mark.parametrize("mode", ["owner", "read", "write", "flush", "fsync", "sha", "dir", "dst"])
def test_falha_remove_temp_e_fsynca_dir(
    mode: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    adapter, body = FilesystemObjectStore(tmp_path), MagicMock(wraps=BytesIO(b"conteudo"))
    if mode == "owner":
        monkeypatch.setattr(
            os,
            "setxattr",
            MagicMock(side_effect=OSError(errno.ENOTSUP, "staging=failed")),
        )
    if mode == "read":
        body.read.side_effect = OSError(errno.EIO, "staging=failed")
    real_fdopen = os.fdopen
    def failing_fdopen(*args: Any, **kwargs: Any) -> MagicMock:
        stream = real_fdopen(*args, **kwargs)
        writer = MagicMock(wraps=stream)
        writer.__enter__.return_value = writer
        writer.__exit__.side_effect = stream.__exit__
        writer.fileno.side_effect = stream.fileno
        getattr(writer, mode).side_effect = OSError(errno.EIO, "staging=failed")
        return writer
    if mode in {"write", "flush"}:
        monkeypatch.setattr(os, "fdopen", failing_fdopen)
    real_fsync = os.fsync
    regular_fsyncs = directory_fsyncs = 0
    def failing_fsync(descriptor: int) -> None:
        nonlocal directory_fsyncs, regular_fsyncs
        target = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        if target.is_dir():
            directory_fsyncs += target == _objects_directory(tmp_path)
            if mode in {"dir", "dst"} and directory_fsyncs == (2 if mode == "dst" else 1):
                raise OSError(errno.EIO, "staging=failed")
        else:
            regular_fsyncs += 1
            if mode == "fsync" and regular_fsyncs == 2:
                raise OSError(errno.EIO, "staging=failed")
        real_fsync(descriptor)
    monkeypatch.setattr(os, "fsync", failing_fsync)
    expected = sha256(b"outro" if mode == "sha" else b"conteudo").hexdigest()
    error = ValueError if mode == "sha" else OSError
    message = "sha256=mismatch" if mode == "sha" else "staging=failed"
    with pytest.raises(error, match=message):
        adapter.put("raw/dados.parquet", body, expected)
    assert _adapter_temporaries(tmp_path) == ()
    assert directory_fsyncs == (0 if mode == "owner" else 3 if mode == "dst" else 2)


@pytest.mark.parametrize("kind", ["file", "directory"])
def test_recuperacao_preserva_destino_existente(kind: str, tmp_path: Path) -> None:
    losing, winner, key = b"perdedor", b"vencedor", "raw/dados.parquet"
    crashing = FilesystemObjectStore(tmp_path, fault_injector=_CrashAt("file_fsynced"))
    with pytest.raises(_SimulatedCrash):
        crashing.put(key, BytesIO(losing), sha256(losing).hexdigest())
    destination = _objects_directory(tmp_path) / sha256(key.encode()).hexdigest()
    if kind == "file":
        destination.write_bytes(winner)
    else:
        destination.mkdir()
    message = "object=immutable" if kind == "file" else "destination=invalid"
    with pytest.raises(Conflict, match=message):
        FilesystemObjectStore(tmp_path).put(key, BytesIO(losing), sha256(losing).hexdigest())
    assert destination.read_bytes() == winner if kind == "file" else destination.is_dir()
    if kind == "file":
        assert _adapter_temporaries(tmp_path) == ()


@pytest.mark.linux_only
def test_fifo_final_nao_bloqueia(tmp_path: Path) -> None:
    key, adapter = "raw/dados.parquet", FilesystemObjectStore(tmp_path)
    fifo = _objects_directory(tmp_path) / sha256(key.encode()).hexdigest()
    os.mkfifo(fifo)
    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    process = context.Process(target=_process_final, args=(str(tmp_path), results))
    process.start()
    process.join(timeout=1)
    if process.is_alive():
        process.terminate()
    process.join(timeout=1)
    assert process.exitcode == 0
    assert [results.get(timeout=1) for _ in range(5)] == [("Conflict", "destination=invalid")] * 5
    with pytest.raises(Conflict, match="destination=invalid"):
        adapter.stat(key)
    assert fifo.is_fifo()
    assert _adapter_temporaries(tmp_path) == ()


@pytest.mark.parametrize("order", [("a", "a/b"), ("a/b", "a")])
def test_chaves_prefixadas_coexistem(order: tuple[str, str], tmp_path: Path) -> None:
    adapter, bodies = FilesystemObjectStore(tmp_path), {"a": b"pai", "a/b": b"filho"}
    for key in order:
        adapter.put(key, BytesIO(bodies[key]), sha256(bodies[key]).hexdigest())
    with adapter.open("a") as parent, adapter.open("a/b") as child:
        assert (parent.read(), child.read()) == (b"pai", b"filho")
    lock_key = f".cnes-object-store-{sha256(b'a').hexdigest()}.lock"
    adapter.delete("a")
    assert adapter.stat(lock_key) is None
    adapter.put(lock_key, BytesIO(b"lock"), sha256(b"lock").hexdigest())
    with adapter.open(lock_key) as stream:
        assert stream.read() == b"lock"


@pytest.mark.parametrize("ancestor", [False, True], ids=["root", "ancestor"])
def test_root_aberto_nao_segue_path_substituido(ancestor: bool, tmp_path: Path) -> None:
    parent = tmp_path / "original"
    root = parent / "store"
    adapter = FilesystemObjectStore(root)
    target = parent if ancestor else root
    target.rename(tmp_path / "moved")
    replacement = tmp_path / "replacement"
    replacement.mkdir()
    target.symlink_to(replacement, target_is_directory=True)
    body = b"conteudo"
    adapter.put("raw/objeto", BytesIO(body), sha256(body).hexdigest())
    with adapter.open("raw/objeto") as stream:
        assert stream.read() == body
    assert adapter.stat("raw/objeto") is not None
    adapter.delete("raw/objeto")
    assert adapter.stat("raw/objeto") is None
    assert tuple(replacement.iterdir()) == ()


@pytest.mark.parametrize("link_kind", ["staging", "publication"])
def test_link_incompativel_sem_fallback(
    link_kind: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body, adapter = b"conteudo", FilesystemObjectStore(tmp_path)
    if link_kind == "staging":
        directory = _objects_directory(tmp_path)
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
def test_startup_recupera_apos_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    context = multiprocessing.get_context("spawn")
    reached, release, results = context.Event(), context.Event(), context.Queue()
    process = context.Process(
        target=_process_paused_put,
        args=(str(tmp_path), b"conteudo", (reached, release, results)),)
    process.start()
    assert reached.wait(timeout=5)
    lock_path = next(next(tmp_path.rglob("locks")).iterdir())
    with lock_path.open("a+b") as lock:
        with pytest.raises(BlockingIOError):
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    real_flock = fcntl.flock
    def release_writer(descriptor: int, operation: int) -> None:
        if operation & fcntl.LOCK_NB:
            try:
                return real_flock(descriptor, operation)
            finally:
                release.set()
        release.set()
        return real_flock(descriptor, operation)
    monkeypatch.setattr(fcntl, "flock", release_writer)
    FilesystemObjectStore(tmp_path)
    process.join(timeout=10)
    assert process.exitcode == 0
    assert results.get(timeout=2) == "_SimulatedCrash"
    assert _adapter_temporaries(tmp_path) == ()


@pytest.mark.linux_only
@pytest.mark.parametrize("identical", [True, False], ids=["identicos", "conflitantes"])
def test_corrida_publica_destino_completo(identical: bool, tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    bodies = (b"a" * (2 * 1024 * 1024),) * 2 if identical else (b"a" * 1024, b"b" * 2048)
    barrier, results = context.Barrier(3), context.Queue()
    destination_linked, reader_done = context.Event(), context.Event()
    reader_ready, observed = Event(), []
    reader = Thread(
        target=_read_during_publication,
        args=(str(tmp_path), (reader_ready, destination_linked, reader_done, observed)),)
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
