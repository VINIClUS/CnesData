"""Disputa de publishers e falha pre-CAS sobre o ponteiro de dataset local."""
from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import BytesIO
from threading import Barrier
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_domain.control_plane.entities import ManifestRef, Run, RunDependency, RunUnit
from cnes_domain.control_plane.enums import RunStage, RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict, ControlPlaneErrorCode
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.object_store.filesystem import FilesystemObjectStore
from data_processor.orchestration.attempt_store import attempt_object_key, unit_attempt_prefix
from data_processor.orchestration.publisher import DatasetPublisher, PublishRequest, PublishResult

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = [pytest.mark.chaos]

_TENANT = "354130"
_COMPETENCIA = "2026-01"
_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)
_REPETICOES = 100


@dataclass
class _CrashOnceInjector:
    boundary: str
    fail_at_call: int
    calls: int = field(default=0, init=False)
    triggered: bool = field(default=False, init=False)

    def __call__(self, boundary: str) -> None:
        if self.triggered or boundary != self.boundary:
            return
        self.calls += 1
        if self.calls == self.fail_at_call:
            self.triggered = True
            raise OSError("simulated_crash")


@dataclass(frozen=True, slots=True)
class _Publicacao:
    run: Run
    units: tuple[RunUnit, ...]


@dataclass(frozen=True, slots=True)
class _AmbientePublicacao:
    setup: SQLiteControlPlane
    writers: tuple[SQLiteControlPlane, SQLiteControlPlane]
    stores: tuple[FilesystemObjectStore, FilesystemObjectStore]


@dataclass(frozen=True, slots=True)
class _Resultado:
    vencedores: int
    perdedores: int
    invariantes_ok: bool


def _capture(action: Any, barrier: Barrier) -> Any:
    barrier.wait()
    try:
        return action()
    except Exception as error:
        return error


def _race(first: Any, second: Any) -> tuple[Any, Any]:
    barrier = Barrier(3)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (
            executor.submit(_capture, first, barrier),
            executor.submit(_capture, second, barrier),
        )
        barrier.wait()
        return tuple(future.result() for future in futures)


def _control_plane(root: Path) -> SQLiteControlPlane:
    adapter = SQLiteControlPlane(root / "cp.sqlite3", lambda: _NOW)
    adapter.initialize()
    return adapter


def _chave_run_manifest(run_id: str) -> str:
    return f"reconciliation/{_TENANT}/{_COMPETENCIA}/{run_id}/run-manifest.json"


def _semear(store: FilesystemObjectStore, run_id: str) -> _Publicacao:
    run = Run(
        tenant_id=_TENANT, run_id=run_id, competencia=_COMPETENCIA, dataset_name="gold",
        state=RunState.PUBLISHING,
        dependencies=(RunDependency(source_type="CNES", file_subtype="ST", required=True),),
        missing_sources=(), created_at=_NOW,
    )
    body = f"payload-{run_id}".encode()
    digest = hashlib.sha256(body).hexdigest()
    object_key = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{run_id}/part-a.parquet"
    manifest = OutputManifest(
        manifest_version=1, manifest_id=f"manifest-{run_id}", tenant_id=_TENANT,
        layer="reconciliation", source_type=None, competencia=_COMPETENCIA,
        run_id=run_id, unit_id="unit-m", attempt=1,
        schema_version="gold-v1", object_key=object_key, object_sha256=digest,
        row_count=10, created_at=_NOW,
    )
    prefix = unit_attempt_prefix(
        SimpleNamespace(tenant_id=_TENANT, run_id=run_id, unit_id="unit-m", attempt=1)
    )
    source_key = attempt_object_key(prefix, object_key)
    store.put(source_key, BytesIO(body), digest)
    sidecar_key = attempt_object_key(prefix, f"manifests/{manifest.manifest_id}/manifest.json")
    payload = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    store.put(sidecar_key, BytesIO(payload), hashlib.sha256(payload).hexdigest())
    ref = ManifestRef(manifest_id=manifest.manifest_id, manifest_key=sidecar_key)
    unit = RunUnit(
        tenant_id=_TENANT, run_id=run_id, unit_id="unit-m", stage=RunStage.MATERIALIZE,
        source_type=None, file_subtype=None, partition="all",
        depends_on_unit_ids=("unit-upstream",), input_manifests=(),
        state=RunUnitState.SUCCEEDED, attempt=1, fencing_token=1, lease_owner=None,
        lease_until=None, dispatch_id=None, output_manifests=(ref,), error_code=None,
    )
    return _Publicacao(run=run, units=(unit,))


def _ambiente_publicacao(tmp_path: Path) -> _AmbientePublicacao:
    setup = _control_plane(tmp_path)
    database_path = tmp_path / "cp.sqlite3"
    writers = (
        SQLiteControlPlane(database_path, lambda: _NOW),
        SQLiteControlPlane(database_path, lambda: _NOW),
    )
    object_root = tmp_path / "objects"
    stores = (FilesystemObjectStore(object_root), FilesystemObjectStore(object_root))
    return _AmbientePublicacao(setup=setup, writers=writers, stores=stores)


def _corrida_publisher(ambiente: _AmbientePublicacao, rep: int) -> _Resultado:
    dataset_name = f"gold-{rep:03d}"
    run_ids = (f"run-pub-a-{rep:03d}", f"run-pub-b-{rep:03d}")
    publicacoes = tuple(
        _semear(store, run_id) for store, run_id in zip(ambiente.stores, run_ids, strict=True)
    )
    runs = tuple(
        publicacao.run.model_copy(update={"dataset_name": dataset_name})
        for publicacao in publicacoes
    )
    for run in runs:
        ambiente.setup.put_run(run)
    publishers = (
        DatasetPublisher(store=ambiente.stores[0], control_plane=ambiente.writers[0]),
        DatasetPublisher(store=ambiente.stores[1], control_plane=ambiente.writers[1]),
    )
    requests = tuple(
        PublishRequest(run=run, units=publicacao.units, expected_version_id=None, now=_NOW)
        for run, publicacao in zip(runs, publicacoes, strict=True)
    )
    resultados = _race(
        lambda: publishers[0].publish(requests[0]),
        lambda: publishers[1].publish(requests[1]),
    )
    vencedores = [item for item in resultados if isinstance(item, PublishResult)]
    perdedores = [
        item for item in resultados
        if isinstance(item, Conflict) and item.code is ControlPlaneErrorCode.POINTER_CAS
    ]
    pointer = ambiente.setup.get_dataset_pointer(_TENANT, dataset_name)
    vencedor_run_id = vencedores[0].pointer.version_id if vencedores else None
    perdedor_run_id = next((rid for rid in run_ids if rid != vencedor_run_id), run_ids[0])
    perdedor_versao = ambiente.setup.get_dataset_version(_TENANT, dataset_name, perdedor_run_id)
    invariantes_ok = (
        bool(vencedores)
        and pointer == vencedores[0].pointer
        and perdedor_versao is None
        and ambiente.stores[0].stat(_chave_run_manifest(perdedor_run_id)) is not None
    )
    return _Resultado(
        vencedores=len(vencedores), perdedores=len(perdedores), invariantes_ok=invariantes_ok
    )


def test_dois_publishers_disputando_o_mesmo_ponteiro_elegem_um_vencedor(tmp_path: Path) -> None:
    ambiente = _ambiente_publicacao(tmp_path)
    resultados = [_corrida_publisher(ambiente, rep) for rep in range(_REPETICOES)]
    assert len(resultados) == _REPETICOES
    assert set(resultados) == {_Resultado(vencedores=1, perdedores=1, invariantes_ok=True)}


def test_falha_antes_do_cas_preserva_o_ponteiro_ativo(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path / "objects")
    adapter = _control_plane(tmp_path)
    run_id_a, run_id_b = "run-pub-a", "run-pub-b"
    publicacao_a = _semear(store, run_id_a)
    publicacao_b = _semear(store, run_id_b)
    adapter.put_run(publicacao_a.run)
    adapter.put_run(publicacao_b.run)
    publisher = DatasetPublisher(store=store, control_plane=adapter)
    request_a = PublishRequest(
        run=publicacao_a.run, units=publicacao_a.units, expected_version_id=None, now=_NOW
    )
    resultado_a = publisher.publish(request_a)

    store._fault_injector = _CrashOnceInjector(boundary="destination_linked", fail_at_call=1)
    request_b = PublishRequest(
        run=publicacao_b.run, units=publicacao_b.units, expected_version_id=run_id_a, now=_NOW
    )
    with pytest.raises(OSError):
        publisher.publish(request_b)

    assert adapter.get_dataset_pointer(_TENANT, "gold") == resultado_a.pointer
    assert adapter.get_run(_TENANT, run_id_b).state is RunState.PUBLISHING
    assert adapter.get_dataset_version(_TENANT, "gold", run_id_b) is None
    pending = adapter.pending_outbox(10)
    assert len(pending) == 1
    assert pending[0].aggregate_id == run_id_a

    resultado_b = publisher.publish(request_b)

    assert resultado_b.pointer.version_id == run_id_b
    assert adapter.get_run(_TENANT, run_id_b).state is RunState.PUBLISHED
