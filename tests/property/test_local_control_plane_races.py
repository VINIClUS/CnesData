"""Corridas de dois atores no control plane local: claim, fence e ponteiro."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from io import BytesIO
from threading import Barrier
from typing import TYPE_CHECKING, Any

import pytest

from cnes_domain.control_plane.commands import (
    ClaimRunUnit,
    CommitRunUnit,
    PublicationPermit,
    PublishDataset,
    PutRunUnits,
    ReserveRunDispatch,
)
from cnes_domain.control_plane.entities import (
    DatasetPointer,
    DatasetVersion,
    ManifestRef,
    OutboxEvent,
    Run,
    RunDependency,
    RunUnit,
)
from cnes_domain.control_plane.enums import RunStage, RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict, ControlPlaneErrorCode
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.object_store.filesystem import FilesystemObjectStore

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

pytestmark = [pytest.mark.race]

_TENANT = "354130"
_COMPETENCIA = "2026-01"
_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)
_REPETICOES = 100
_WAVE_A = "a" * 16
_WAVE_B = "b" * 16


@dataclass(frozen=True, slots=True)
class _Ambiente:
    setup: SQLiteControlPlane
    writers: tuple[SQLiteControlPlane, SQLiteControlPlane]


@dataclass(frozen=True, slots=True)
class _Fence:
    dispatch_stale: str
    dispatch_fresh: str
    fence_stale: int
    fence_fresh: int


@dataclass(frozen=True, slots=True)
class _Resultado:
    vencedores: int
    perdedores: int
    invariantes_ok: bool


@dataclass(slots=True)
class _Relogio:
    instante: datetime

    def now(self) -> datetime:
        return self.instante

    def avancar(self, segundos: int) -> None:
        self.instante += timedelta(seconds=segundos)


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


def _ambiente(database_path: Path, clock: Callable[[], datetime]) -> _Ambiente:
    setup = SQLiteControlPlane(database_path, clock)
    setup.initialize()
    writers = (SQLiteControlPlane(database_path, clock), SQLiteControlPlane(database_path, clock))
    return _Ambiente(setup=setup, writers=writers)


def _run(run_id: str, state: RunState = RunState.PROCESSING) -> Run:
    return Run(
        tenant_id=_TENANT, run_id=run_id, competencia=_COMPETENCIA, dataset_name="gold",
        state=state,
        dependencies=(RunDependency(source_type="CNES", file_subtype="ST", required=True),),
        missing_sources=(), created_at=_NOW,
    )


def _unit(run_id: str) -> RunUnit:
    input_ref = ManifestRef(
        manifest_id="input-a",
        manifest_key=f"raw/{_TENANT}/CNES/{_COMPETENCIA}/input-a/manifest.json",
    )
    return RunUnit(
        tenant_id=_TENANT, run_id=run_id, unit_id="unit-a", stage=RunStage.NORMALIZE,
        source_type="CNES", file_subtype="ST", partition="all", depends_on_unit_ids=(),
        input_manifests=(input_ref,), state=RunUnitState.PENDING, attempt=0, fencing_token=0,
        lease_owner=None, lease_until=None, dispatch_id=None, output_manifests=(),
        error_code=None,
    )


def _outbox_event(event_id: str, run_id: str) -> OutboxEvent:
    return OutboxEvent(
        tenant_id=_TENANT, event_id=event_id, event_type="control_plane.race",
        aggregate_id=run_id, payload={}, created_at=_NOW, delivered_at=None,
    )


def _semear_claim(setup: SQLiteControlPlane, store: FilesystemObjectStore, run_id: str) -> str:
    setup.put_run(_run(run_id))
    setup.put_run_units(PutRunUnits(
        tenant_id=_TENANT, run_id=run_id, expected_run_state=RunState.PROCESSING,
        units=(_unit(run_id),),
    ))
    dispatch = setup.reserve_run_dispatch(ReserveRunDispatch(
        tenant_id=_TENANT, run_id=run_id, wave_id=_WAVE_A, unit_ids=("unit-a",),
        now=_NOW, lease_seconds=30,
    ))
    for owner in ("owner-x", "owner-y"):
        body = f"staged-{owner}".encode()
        store.put(f"tmp/{run_id}/{owner}.bin", BytesIO(body), sha256(body).hexdigest())
    return dispatch.dispatch_id


def _corrida_claim(ambiente: _Ambiente, store: FilesystemObjectStore, rep: int) -> _Resultado:
    run_id = f"run-claim-{rep:03d}"
    dispatch_id = _semear_claim(ambiente.setup, store, run_id)
    claims = tuple(
        ClaimRunUnit(
            tenant_id=_TENANT, run_id=run_id, unit_id="unit-a", dispatch_id=dispatch_id,
            owner=owner, now=_NOW, lease_seconds=30,
        )
        for owner in ("owner-x", "owner-y")
    )
    resultados = _race(
        lambda: ambiente.writers[0].claim_run_unit(claims[0]),
        lambda: ambiente.writers[1].claim_run_unit(claims[1]),
    )
    vencedores = sum(isinstance(item, RunUnit) for item in resultados)
    perdedores = sum(item is None for item in resultados)
    invariantes_ok = all(
        store.stat(f"tmp/{run_id}/{owner}.bin") is not None for owner in ("owner-x", "owner-y")
    )
    return _Resultado(vencedores=vencedores, perdedores=perdedores, invariantes_ok=invariantes_ok)


def test_claim_duplo_da_mesma_unidade_elege_um_unico_vencedor(tmp_path: Path) -> None:
    ambiente = _ambiente(tmp_path / "cp-claim.sqlite3", lambda: _NOW)
    store = FilesystemObjectStore(tmp_path / "objects")
    resultados = [_corrida_claim(ambiente, store, rep) for rep in range(_REPETICOES)]
    assert len(resultados) == _REPETICOES
    assert set(resultados) == {_Resultado(vencedores=1, perdedores=1, invariantes_ok=True)}


def _saida(run_id: str, sufixo: str) -> tuple[ManifestRef, ...]:
    key = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{run_id}/output-{sufixo}/manifest.json"
    return (ManifestRef(manifest_id=f"output-{run_id}-{sufixo}", manifest_key=key),)


def _semear_fence(setup: SQLiteControlPlane, relogio: _Relogio, run_id: str) -> _Fence:
    setup.put_run(_run(run_id))
    setup.put_run_units(PutRunUnits(
        tenant_id=_TENANT, run_id=run_id, expected_run_state=RunState.PROCESSING,
        units=(_unit(run_id),),
    ))
    dispatch_a = setup.reserve_run_dispatch(ReserveRunDispatch(
        tenant_id=_TENANT, run_id=run_id, wave_id=_WAVE_A, unit_ids=("unit-a",),
        now=relogio.now(), lease_seconds=30,
    ))
    stale = setup.claim_run_unit(ClaimRunUnit(
        tenant_id=_TENANT, run_id=run_id, unit_id="unit-a", dispatch_id=dispatch_a.dispatch_id,
        owner="worker-stale", now=relogio.now(), lease_seconds=30,
    ))
    relogio.avancar(60)
    dispatch_b = setup.reserve_run_dispatch(ReserveRunDispatch(
        tenant_id=_TENANT, run_id=run_id, wave_id=_WAVE_B, unit_ids=("unit-a",),
        now=relogio.now(), lease_seconds=30,
    ))
    fresh = setup.claim_run_unit(ClaimRunUnit(
        tenant_id=_TENANT, run_id=run_id, unit_id="unit-a", dispatch_id=dispatch_b.dispatch_id,
        owner="worker-fresh", now=relogio.now(), lease_seconds=30,
    ))
    return _Fence(
        dispatch_stale=dispatch_a.dispatch_id, dispatch_fresh=dispatch_b.dispatch_id,
        fence_stale=stale.fencing_token, fence_fresh=fresh.fencing_token,
    )


def _corrida_fence(ambiente: _Ambiente, relogio: _Relogio, rep: int) -> _Resultado:
    run_id = f"run-fence-{rep:03d}"
    fence = _semear_fence(ambiente.setup, relogio, run_id)
    stale_commit = CommitRunUnit(
        tenant_id=_TENANT, run_id=run_id, unit_id="unit-a", dispatch_id=fence.dispatch_stale,
        owner="worker-stale", fencing_token=fence.fence_stale,
        output_manifests=_saida(run_id, "stale"),
    )
    fresh_commit = CommitRunUnit(
        tenant_id=_TENANT, run_id=run_id, unit_id="unit-a", dispatch_id=fence.dispatch_fresh,
        owner="worker-fresh", fencing_token=fence.fence_fresh,
        output_manifests=_saida(run_id, "fresh"),
    )
    resultados = _race(
        lambda: ambiente.writers[0].commit_run_unit(
            stale_commit, _outbox_event(f"commit-stale-{run_id}", run_id)),
        lambda: ambiente.writers[1].commit_run_unit(
            fresh_commit, _outbox_event(f"commit-fresh-{run_id}", run_id)),
    )
    vencedores = sum(isinstance(item, RunUnit) for item in resultados)
    perdedores = sum(isinstance(item, Conflict) for item in resultados)
    persisted = ambiente.setup.list_run_units(_TENANT, run_id)[0]
    invariantes_ok = persisted.output_manifests == _saida(run_id, "fresh")
    return _Resultado(vencedores=vencedores, perdedores=perdedores, invariantes_ok=invariantes_ok)


def test_fence_obsoleto_nunca_commita_saida_apos_supersede(tmp_path: Path) -> None:
    relogio = _Relogio(_NOW)
    ambiente = _ambiente(tmp_path / "cp-fence.sqlite3", relogio.now)
    resultados = [_corrida_fence(ambiente, relogio, rep) for rep in range(_REPETICOES)]
    assert len(resultados) == _REPETICOES
    assert set(resultados) == {_Resultado(vencedores=1, perdedores=1, invariantes_ok=True)}


def _publish_command(run_id: str, dataset_name: str) -> PublishDataset:
    manifest_key = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{run_id}/run-manifest.json"
    return PublishDataset(
        version=DatasetVersion(
            tenant_id=_TENANT, dataset_name=dataset_name, version_id=run_id, run_id=run_id,
            run_manifest_key=manifest_key, created_at=_NOW,
        ),
        pointer_name="current", expected_version_id=None,
        final_state=RunState.PUBLISHED, missing_sources=(),
        publication_permit=PublicationPermit(
            tenant_id=_TENANT, run_id=run_id, policy_version=1, fencing_token=1,
        ),
        event=_outbox_event(f"published-{run_id}", run_id),
    )


def _corrida_publicacao(ambiente: _Ambiente, rep: int) -> _Resultado:
    dataset_name = f"gold-{rep:03d}"
    run_id_a, run_id_b = f"run-pub-a-{rep:03d}", f"run-pub-b-{rep:03d}"
    for run_id in (run_id_a, run_id_b):
        run = _run(run_id, RunState.PUBLISHING).model_copy(update={"dataset_name": dataset_name})
        ambiente.setup.put_run(run)
    comandos = (_publish_command(run_id_a, dataset_name), _publish_command(run_id_b, dataset_name))
    resultados = _race(
        lambda: ambiente.writers[0].publish_dataset(comandos[0]),
        lambda: ambiente.writers[1].publish_dataset(comandos[1]),
    )
    vencedores = [item for item in resultados if isinstance(item, DatasetPointer)]
    perdedores = [
        item for item in resultados
        if isinstance(item, Conflict) and item.code is ControlPlaneErrorCode.POINTER_CAS
    ]
    pointer = ambiente.setup.get_dataset_pointer(_TENANT, dataset_name)
    invariantes_ok = bool(vencedores) and pointer == vencedores[0]
    return _Resultado(
        vencedores=len(vencedores), perdedores=len(perdedores), invariantes_ok=invariantes_ok
    )


def test_publicacao_concorrente_no_mesmo_ponteiro_elege_um_unico_vencedor(tmp_path: Path) -> None:
    ambiente = _ambiente(tmp_path / "cp-pub.sqlite3", lambda: _NOW)
    resultados = [_corrida_publicacao(ambiente, rep) for rep in range(_REPETICOES)]
    assert len(resultados) == _REPETICOES
    assert set(resultados) == {_Resultado(vencedores=1, perdedores=1, invariantes_ok=True)}
