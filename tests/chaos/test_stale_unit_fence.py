"""Worker atrasado (lease expirada + dispatch superado) nao pode commitar output."""
from __future__ import annotations

import hashlib
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import BinaryIO
    from contextlib import AbstractContextManager as ContextManager

_TENANT = "354130"
_RUN_ID = "run-chaos"
_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)


@dataclass(slots=True)
class _MutableClock:
    instant: datetime

    def now(self) -> datetime:
        return self.instant

    def advance(self, delta: timedelta) -> None:
        self.instant += delta


@dataclass
class _FakeObjectStore:
    objects: dict[str, bytes] = field(default_factory=dict)

    def put(self, key: str, body: BinaryIO, expected_sha256: str):
        from cnes_domain.ports.object_store import ObjectStat

        data = body.read()
        self.objects[key] = data
        return ObjectStat(key=key, size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

    def open(self, key: str) -> ContextManager[BinaryIO]:
        return nullcontext(BytesIO(self.objects[key]))

    def stat(self, key: str):
        from cnes_domain.ports.object_store import ObjectStat

        data = self.objects.get(key)
        if data is None:
            return None
        return ObjectStat(key=key, size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

    def delete(self, key: str) -> None:
        self.objects.pop(key, None)

    def promote(self, source_key: str, destination_key: str, expected_sha256: str):
        raise RuntimeError("promote_forbidden")


@pytest.mark.chaos
def test_worker_atrasado_nao_sobrescreve_output_apos_supersede(tmp_path):
    """Invariant: a stale worker whose dispatch is superseded mid-flight never
    commits into RunUnit.output_manifests — its writes stay confined to tmp/."""
    try:
        from cnes_contracts.manifests.outputs import OutputManifest
        from cnes_contracts.manifests.raw import SourceType
        from cnes_domain.control_plane.commands import (
            ClaimRunUnit,
            PutRunUnits,
            ReserveRunDispatch,
        )
        from cnes_domain.control_plane.entities import ManifestRef, Run, RunDependency, RunUnit
        from cnes_domain.control_plane.enums import RunStage, RunState, RunUnitState
        from cnes_domain.control_plane.errors import FenceRejected, LeaseLost
        from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
        from data_processor.orchestration.unit_worker import UnitWorker, UnitWorkerDependencies
    except ImportError:
        pytest.skip("control plane / orchestration modules not available")

    clock = _MutableClock(_NOW)
    adapter = SQLiteControlPlane(tmp_path / "cp.db", clock.now)
    adapter.initialize()
    adapter.put_run(Run(
        tenant_id=_TENANT, run_id=_RUN_ID, competencia="2026-01", dataset_name="gold",
        state=RunState.PROCESSING,
        dependencies=(RunDependency(source_type="CNES", file_subtype="ST", required=True),),
        missing_sources=(), created_at=_NOW,
    ))
    input_ref = ManifestRef(
        manifest_id="input-a", manifest_key=f"raw/{_TENANT}/CNES/2026-01/input-a/manifest.json"
    )
    unit = RunUnit(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id="unit-a", stage=RunStage.NORMALIZE,
        source_type="CNES", file_subtype="ST", partition="all", depends_on_unit_ids=(),
        input_manifests=(input_ref,), state=RunUnitState.PENDING, attempt=0, fencing_token=0,
        lease_owner=None, lease_until=None, dispatch_id=None, output_manifests=(),
        error_code=None,
    )
    adapter.put_run_units(PutRunUnits(
        tenant_id=_TENANT, run_id=_RUN_ID, expected_run_state=RunState.PROCESSING, units=(unit,),
    ))
    dispatch = adapter.reserve_run_dispatch(ReserveRunDispatch(
        tenant_id=_TENANT, run_id=_RUN_ID, wave_id="a" * 16, unit_ids=("unit-a",),
        now=clock.now(), lease_seconds=30,
    ))

    def _stale_processor(claimed_unit: RunUnit, attempt_store) -> tuple[OutputManifest, ...]:
        clock.advance(timedelta(seconds=60))
        new_dispatch = adapter.reserve_run_dispatch(ReserveRunDispatch(
            tenant_id=_TENANT, run_id=_RUN_ID, wave_id="b" * 16, unit_ids=("unit-a",),
            now=clock.now(), lease_seconds=30,
        ))
        adapter.claim_run_unit(ClaimRunUnit(
            tenant_id=_TENANT, run_id=_RUN_ID, unit_id="unit-a",
            dispatch_id=new_dispatch.dispatch_id, owner="worker-fresh",
            now=clock.now(), lease_seconds=30,
        ))
        body = b"stale-output"
        object_key = (
            f"normalized/{_TENANT}/{SourceType.CNES_LOCAL.value}/2026-01/"
            f"{_RUN_ID}/stale.parquet"
        )
        stat = attempt_store.put(object_key, BytesIO(body), hashlib.sha256(body).hexdigest())
        return (OutputManifest(
            manifest_version=1, manifest_id="manifest-stale", tenant_id=_TENANT,
            layer="normalized", source_type=SourceType.CNES_LOCAL, competencia="2026-01",
            run_id=_RUN_ID, unit_id="unit-a", attempt=claimed_unit.attempt,
            schema_version="etl-v1", object_key=stat.key, object_sha256=stat.sha256,
            row_count=1, created_at=_NOW,
        ),)

    store = _FakeObjectStore()
    dependencies = UnitWorkerDependencies(
        control_plane=adapter, store=store, processor=_stale_processor, clock=clock.now,
    )
    worker = UnitWorker(dependencies)
    claim = ClaimRunUnit(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id="unit-a", dispatch_id=dispatch.dispatch_id,
        owner="worker-stale", now=clock.now(), lease_seconds=30,
    )

    with pytest.raises((FenceRejected, LeaseLost)):
        worker.execute(claim)

    reclaimed = adapter.list_run_units(_TENANT, _RUN_ID)[0]
    assert reclaimed.output_manifests == ()
    assert all(key.startswith("tmp/") for key in store.objects)
