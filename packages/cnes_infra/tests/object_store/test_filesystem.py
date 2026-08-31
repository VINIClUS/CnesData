"""Conformidade do armazenamento de objetos no filesystem."""

import errno
import fcntl
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
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


class _SimulatedCrash(RuntimeError):
    pass


@dataclass(slots=True)
class _CrashAt:
    boundary: str
    triggered: bool = False

    def __call__(self, boundary: str) -> None:
        if boundary == self.boundary and not self.triggered:
            self.triggered = True
            raise _SimulatedCrash(boundary)


def _adapter_temporaries(root: Path) -> tuple[Path, ...]:
    return tuple(root.rglob(".cnes-object-store-*.tmp"))


def _process_put(root: str, body: bytes, barrier: Any, results: Any) -> None:
    expected = sha256(body).hexdigest()
    barrier.wait()
    try:
        stat = FilesystemObjectStore(root).put("raw/race", BytesIO(body), expected)
        results.put(("ok", stat.sha256))
    except Conflict:
        results.put(("conflict", expected))


def _process_paused_put(root: str, body: bytes, controls: tuple[Any, ...]) -> None:
    reached, release, results = controls

    def pause(boundary: str) -> None:
        if boundary == "temporary_created":
            reached.set()
            if not release.wait(timeout=5):
                raise TimeoutError("release_timeout")

    try:
        digest = sha256(body).hexdigest()
        FilesystemObjectStore(root, fault_injector=pause).put("raw/locked", BytesIO(body), digest)
        results.put("ok")
    except Exception as error:
        results.put(type(error).__name__)


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

    with pytest.raises(_SimulatedCrash, match=boundary):
        perform()

    recovered = FilesystemObjectStore(tmp_path)
    current = recovered.stat("raw/dados.parquet")
    assert current is None or (current.size_bytes, current.sha256) == (len(body), expected)
    assert recovered.put("raw/dados.parquet", BytesIO(body), expected).sha256 == expected
    with recovered.open("raw/dados.parquet") as stream:
        assert stream.read() == body
    assert _adapter_temporaries(tmp_path) == ()
    assert unrelated.read_bytes() == b"preservar"


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
    with pytest.raises(Conflict, match="immutable_object"):
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

    with pytest.raises(Conflict, match="invalid_destination"):
        FilesystemObjectStore(tmp_path).put(key, BytesIO(body), sha256(body).hexdigest())

    assert destination.is_dir()


def test_remove_staging_quando_sha256_diverge(tmp_path: Path) -> None:
    adapter = FilesystemObjectStore(tmp_path)

    with pytest.raises(ValueError, match="sha256_mismatch"):
        adapter.put("raw/invalido", BytesIO(b"conteudo"), sha256(b"outro").hexdigest())

    assert adapter.stat("raw/invalido") is None
    assert _adapter_temporaries(tmp_path) == ()


def test_nao_faz_fallback_quando_hard_link_e_incompativel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    body = b"conteudo"
    adapter = FilesystemObjectStore(tmp_path)

    def reject_link(source: Path, destination: Path) -> None:
        raise OSError(errno.EPERM, "hard_link_incompativel")

    monkeypatch.setattr(os, "link", reject_link)

    with pytest.raises(OSError, match="hard_link_incompativel"):
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
    processes = [
        context.Process(target=_process_put, args=(str(tmp_path), body, barrier, results))
        for body in bodies
    ]
    for process in processes:
        process.start()
    barrier.wait()
    observed = []
    while any(process.is_alive() for process in processes):
        try:
            with FilesystemObjectStore(tmp_path).open("raw/race") as stream:
                observed.append(stream.read())
        except FileNotFoundError:
            pass
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
