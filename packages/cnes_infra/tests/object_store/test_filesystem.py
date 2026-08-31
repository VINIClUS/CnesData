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
from cnes_infra.object_store import filesystem
from cnes_infra.object_store.filesystem import FilesystemObjectStore, _open_or_create_directory
from packages.cnes_infra.tests.contracts import object_store_contract as contract
from packages.cnes_infra.tests.contracts.clock import MutableClock

_DURABLE_BOUNDARIES = ("temporary_created_before_ownership", "temporary_created", "file_fsynced",
    "destination_linked", "directory_fsynced",
    "temporary_unlinked", "directory_final_fsynced",)
_INTERNAL = ".cnes-object-store-internal"
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
        adapter = FilesystemObjectStore(root, fault_injector=wait_for_reader)
        stat = adapter.put("raw/race", BytesIO(body), expected)
        results.put(("ok", stat.sha256))
    except Conflict:
        results.put(("conflict", expected))


def _paused_put(root: str, body: bytes, controls: tuple[Any, ...]) -> None:
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


@pytest.mark.linux_only
def test_fsynca_ancestral(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root, observed, real_fsync = tmp_path / "store", [], os.fsync
    def observe_fsync(descriptor: int) -> None:
        target = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        if target.is_dir():
            observed.append(target)
        real_fsync(descriptor)
    monkeypatch.setattr(os, "fsync", observe_fsync)
    adapter = FilesystemObjectStore(root)
    internal = next(root.iterdir())
    expected = [*reversed(root.parents), root, internal, internal]
    assert observed == expected
    observed.clear()
    FilesystemObjectStore(root)
    assert observed == expected
    observed.clear()
    FilesystemObjectStore(root / "nested")
    assert observed[:len(root.parents) + 1] == [*reversed(root.parents), root]
    destination = _objects_directory(root) / sha256(b"raw/ausente").hexdigest()
    destination.touch()
    destination.unlink()
    observed.clear()
    adapter.delete("raw/ausente")
    assert observed == [destination.parent]


def test_fecha_diretorio_se_fsync_do_parent_falha(monkeypatch: pytest.MonkeyPatch) -> None:
    close = MagicMock()
    monkeypatch.setattr(os, "mkdir", MagicMock())
    monkeypatch.setattr(os, "open", MagicMock(return_value=7))
    monkeypatch.setattr(os, "fsync", MagicMock(side_effect=OSError(errno.EIO, "fsync=failed")))
    monkeypatch.setattr(os, "close", close)
    with pytest.raises(OSError, match="fsync=failed"):
        _open_or_create_directory(3, "objects")
    close.assert_called_once_with(7)


@pytest.mark.parametrize("operation", ["put", "promote"])
@pytest.mark.parametrize("boundary", _DURABLE_BOUNDARIES)
def test_recupera_fronteira_duravel(boundary: str, operation: str, tmp_path: Path) -> None:
    body, expected = b"conteudo-completo", sha256(b"conteudo-completo").hexdigest()
    unrelated, source_key = tmp_path / ".arquivo-do-usuario", "staging/dados.parquet"
    unrelated.write_bytes(b"preservar")
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
    temporary, removed, real_open = _adapter_temporaries(tmp_path)[0], [], os.open
    def vanish_before_open(
        path: Any, flags: int, mode: int = 0o777, *, dir_fd: int | None = None) -> int:
        if path == temporary.name and not removed:
            removed.append(temporary)
            temporary.unlink()
        return real_open(path, flags, mode, dir_fd=dir_fd)
    monkeypatch.setattr(os, "open", vanish_before_open)
    FilesystemObjectStore(tmp_path)
    assert removed == [temporary]


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
    digest, namespace = sha256(body).hexdigest(), sha256(key.encode()).hexdigest()
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


def test_reabertura_ignora_owner_invalido(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    FilesystemObjectStore(tmp_path)
    directory = _objects_directory(tmp_path)
    mismatch_owner, mismatch_digest = "raw/owner.parquet", sha256(b"raw/owner.parquet").hexdigest()
    nul_key, real_owner = "raw/\0invalida", filesystem._temporary_owner
    nul_digest = sha256(nul_key.encode()).hexdigest()
    nul_candidate = directory / f".cnes-object-store-{nul_digest}-writer.tmp"
    candidates = {
        directory / f".cnes-object-store-{'b' * 64}-writer.tmp": None,
        directory / f".cnes-object-store-{'c' * 64}-writer.tmp": b"\xff",
        directory / f".cnes-object-store-{'d' * 64}-writer.tmp": b"../invalid\0writer",
        directory / f".cnes-object-store-{mismatch_digest}-.tmp": b"raw/owner.parquet\0writer",
        nul_candidate: None,
    }
    for candidate, owner in candidates.items():
        candidate.write_bytes(b"preservar")
        if owner is not None:
            os.setxattr(candidate, _OWNER_XATTR, owner)
    fifo = directory / f".cnes-object-store-{'e' * 64}-writer.tmp"
    os.mkfifo(fifo)
    def owner(descriptor: int) -> tuple[str, str] | None:
        matches = os.fstat(descriptor).st_ino == nul_candidate.stat().st_ino
        return (nul_key, "writer") if matches else real_owner(descriptor)
    monkeypatch.setattr(filesystem, "_temporary_owner", owner)
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
    with pytest.raises(ValueError, match="object_key=invalid"):
        adapter.stat(nul_key)


@pytest.mark.parametrize("caller", ["startup", "pre_write"])
@pytest.mark.parametrize("failure", ["open", "fstat", "xattr"])
def test_inspecao_de_ownership_fecha_fd(
    caller: str, failure: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    key, body = "raw/owner.parquet", b"preservar"
    adapter, digest = FilesystemObjectStore(tmp_path), sha256(key.encode()).hexdigest()
    candidate = _objects_directory(tmp_path) / f".cnes-object-store-{digest}-writer.tmp"
    candidate.write_bytes(body)
    os.setxattr(candidate, _OWNER_XATTR, f"{key}\0writer".encode())
    if failure == "open":
        real_open = os.open
        def fail_open(path: Any, *args: Any, **kwargs: Any) -> int:
            if path == candidate.name:
                raise PermissionError(errno.EACCES, "inspection=failed")
            return real_open(path, *args, **kwargs)
        monkeypatch.setattr(os, "open", fail_open)
    else:
        syscall = "fstat" if failure == "fstat" else "getxattr"
        error = OSError(errno.EIO, "inspection=failed")
        monkeypatch.setattr(os, syscall, MagicMock(side_effect=error))
    descriptor_count = len(os.listdir("/proc/self/fd"))
    target, args = (
        (FilesystemObjectStore, (tmp_path,)) if caller == "startup"
        else (adapter.put, (key, BytesIO(body), sha256(body).hexdigest())))
    with pytest.raises(OSError, match="inspection=failed"):
        target(*args)
    assert (len(os.listdir("/proc/self/fd")), candidate.read_bytes()) == (descriptor_count, body)


@pytest.mark.parametrize("mode", ["owner", "read", "write", "flush", "fsync", "sha", "dir", "dst"])
def test_falha_staging(mode: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    adapter, body = FilesystemObjectStore(tmp_path), MagicMock(wraps=BytesIO(b"conteudo"))
    if mode == "owner":
        monkeypatch.setattr(
            os, "setxattr", MagicMock(side_effect=OSError(errno.ENOTSUP, "staging=failed")))
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
    real_fsync, regular_fsyncs, directory_fsyncs = os.fsync, 0, 0
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
    expected_fsyncs = 0 if mode == "owner" else 3 if mode == "dst" else 2
    assert (_adapter_temporaries(tmp_path), directory_fsyncs) == ((), expected_fsyncs)


@pytest.mark.parametrize("kind", ["file", "directory"])
def test_recuperacao_preserva_destino_existente(kind: str, tmp_path: Path) -> None:
    losing, winner, key = b"perdedor", b"vencedor", "raw/dados.parquet"
    crashing = FilesystemObjectStore(tmp_path, fault_injector=_CrashAt("file_fsynced"))
    with pytest.raises(_SimulatedCrash):
        crashing.put(key, BytesIO(losing), sha256(losing).hexdigest())
    destination = _objects_directory(tmp_path) / sha256(key.encode()).hexdigest()
    destination.write_bytes(winner) if kind == "file" else destination.mkdir()
    message = "object=immutable" if kind == "file" else "destination=invalid"
    with pytest.raises(Conflict, match=message):
        FilesystemObjectStore(tmp_path).put(key, BytesIO(losing), sha256(losing).hexdigest())
    assert destination.read_bytes() == winner if kind == "file" else destination.is_dir()
    assert kind != "file" or _adapter_temporaries(tmp_path) == ()


@pytest.mark.linux_only
def test_fifo_final_nao_bloqueia(tmp_path: Path) -> None:
    key = "raw/dados.parquet"
    FilesystemObjectStore(tmp_path)
    fifo = _objects_directory(tmp_path) / sha256(key.encode()).hexdigest()
    os.mkfifo(fifo)
    results = (context := multiprocessing.get_context("spawn")).Queue()
    process = context.Process(target=_process_final, args=(str(tmp_path), results))
    process.start()
    process.join(timeout=1)
    if process.is_alive():
        process.terminate()
    process.join(timeout=1)
    assert (process.exitcode, fifo.is_fifo()) == (0, True)
    assert [results.get(timeout=1) for _ in range(5)] == [("Conflict", "destination=invalid")] * 5
    assert _adapter_temporaries(tmp_path) == ()


@pytest.mark.parametrize("order", [("a", "a/b"), ("a/b", "a")])
def test_chaves_prefixadas_coexistem(order: tuple[str, str], tmp_path: Path) -> None:
    adapter, bodies = FilesystemObjectStore(tmp_path), {"a": b"pai", "a/b": b"filho"}
    for key in order:
        adapter.put(key, BytesIO(bodies[key]), sha256(bodies[key]).hexdigest())
    with adapter.open("a") as parent, adapter.open("a/b") as child:
        assert (parent.read(), child.read()) == (b"pai", b"filho")
@pytest.mark.parametrize("relative", ["", _INTERNAL, f"{_INTERNAL}/objects", f"{_INTERNAL}/locks"])
def test_descritores_nao_seguem_layout_substituido(relative: str, tmp_path: Path) -> None:
    descriptor_count, parent = len(os.listdir("/proc/self/fd")), tmp_path / "original"
    adapter = FilesystemObjectStore(root := parent / "store")
    target = root / relative if relative else parent
    target.rename(tmp_path / "moved")
    target.mkdir()
    key = f".cnes-object-store-{sha256(b'a').hexdigest()}.lock"
    adapter.put(key, BytesIO(b"conteudo"), sha256(b"conteudo").hexdigest())
    assert tuple(target.iterdir()) == ()
    assert adapter.stat(key) is not None
    del adapter
    assert len(os.listdir("/proc/self/fd")) == descriptor_count


def test_construtor_rejeita_symlink(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    parent, attacker = tmp_path / "original", tmp_path / "attacker"
    root = parent / "store"
    root.mkdir(parents=True)
    (attacker / "store").mkdir(parents=True)
    real_open = os.open
    def swap_before_open(path: Any, *args: Any, **kwargs: Any) -> int:
        component_open = path == parent.name and kwargs.get("dir_fd") is not None
        full_path_open = os.fspath(path) == os.fspath(root)
        if not parent.is_symlink() and (full_path_open or component_open):
            parent.rename(tmp_path / "moved")
            parent.symlink_to(attacker, target_is_directory=True)
        return real_open(path, *args, **kwargs)
    monkeypatch.setattr(os, "open", swap_before_open)
    with pytest.raises(OSError):
        FilesystemObjectStore(root)
    assert (parent.is_symlink(), tuple((attacker / "store").iterdir())) == (True, ())


@pytest.mark.parametrize("link_kind", ["staging", "publication"])
def test_sem_fallback(link_kind: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
        target=_paused_put, args=(str(tmp_path), b"conteudo", (reached, release, results)))
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
    assert (process.exitcode, results.get(timeout=2)) == (0, "_SimulatedCrash")
    assert _adapter_temporaries(tmp_path) == ()


@pytest.mark.linux_only
@pytest.mark.parametrize("identical", [True, False], ids=["identicos", "conflitantes"])
def test_corrida_publica_destino_completo(identical: bool, tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    bodies = (b"a" * (2 * 1024 * 1024),) * 2 if identical else (b"a" * 1024, b"b" * 2048)
    barrier, results = context.Barrier(3), context.Queue()
    destination_linked, reader_done = context.Event(), context.Event()
    reader_ready, observed = Event(), []
    reader_controls = reader_ready, destination_linked, reader_done, observed
    reader = Thread(target=_read_during_publication, args=(str(tmp_path), reader_controls))
    reader.start()
    assert reader_ready.wait(timeout=5)
    writer_controls = barrier, results, destination_linked, reader_done
    processes = [context.Process(target=_process_put, args=(str(tmp_path), body, writer_controls))
                 for body in bodies]
    for process in processes:
        process.start()
    barrier.wait()
    reader.join(timeout=10)
    assert (reader.is_alive(), bool(observed)) == (False, True)
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    outcomes = [results.get(timeout=2)[0] for _ in processes]
    with FilesystemObjectStore(tmp_path).open("raw/race") as stream:
        published = stream.read()
    assert (published in bodies, all(content in bodies for content in observed)) == (True, True)
    assert outcomes.count("ok") == (2 if identical else 1)
    assert outcomes.count("conflict") == (0 if identical else 1)
    assert sha256(published).hexdigest() == FilesystemObjectStore(tmp_path).stat("raw/race").sha256
    assert _adapter_temporaries(tmp_path) == ()
