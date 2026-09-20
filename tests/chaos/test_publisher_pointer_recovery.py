"""Crash mid-promotion or after pointer CAS must leave dataset publication recoverable."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import BytesIO
from types import SimpleNamespace

import pytest

_TENANT = "354130"
_RUN_ID = "run-chaos-pub"
_COMPETENCIA = "2026-01"
_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)


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


@pytest.mark.chaos
@pytest.mark.local_profile
def test_crash_durante_promocao_nao_avanca_pointer(tmp_path):
    """Invariant: a crash mid-promotion never advances the dataset pointer; retrying
    after the fault clears republishes cleanly because promote is content-addressed
    (idempotent) and the control-plane CAS was never attempted."""
    try:
        from cnes_contracts.manifests.outputs import OutputManifest
        from cnes_domain.control_plane.entities import ManifestRef, Run, RunDependency, RunUnit
        from cnes_domain.control_plane.enums import RunStage, RunState, RunUnitState
        from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
        from cnes_infra.object_store.filesystem import FilesystemObjectStore
        from data_processor.orchestration.attempt_store import (
            attempt_object_key,
            unit_attempt_prefix,
        )
        from data_processor.orchestration.publisher import DatasetPublisher, PublishRequest
    except ImportError:
        pytest.skip("control plane / orchestration modules not available")

    try:
        store = FilesystemObjectStore(tmp_path / "objects")
    except OSError:
        pytest.skip("filesystem object store unsupported")

    adapter = SQLiteControlPlane(tmp_path / "cp.db", lambda: _NOW)
    adapter.initialize()
    run = Run(
        tenant_id=_TENANT, run_id=_RUN_ID, competencia=_COMPETENCIA, dataset_name="gold",
        state=RunState.PUBLISHING,
        dependencies=(RunDependency(source_type="CNES", file_subtype="ST", required=True),),
        missing_sources=(), created_at=_NOW,
    )
    adapter.put_run(run)

    units = []
    for suffix, unit_id, stage in (
        ("a", "unit-r", RunStage.RECONCILE), ("b", "unit-m", RunStage.MATERIALIZE)
    ):
        body = f"payload-{suffix}".encode()
        digest = hashlib.sha256(body).hexdigest()
        object_key = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/part-{suffix}.parquet"
        manifest = OutputManifest(
            manifest_version=1, manifest_id=f"manifest-{suffix}", tenant_id=_TENANT,
            layer="reconciliation", source_type=None, competencia=_COMPETENCIA,
            run_id=_RUN_ID, unit_id=unit_id, attempt=1,
            schema_version="gold-v1", object_key=object_key, object_sha256=digest,
            row_count=10, created_at=_NOW,
        )
        prefix = unit_attempt_prefix(
            SimpleNamespace(tenant_id=_TENANT, run_id=_RUN_ID, unit_id=unit_id, attempt=1)
        )
        source_key = attempt_object_key(prefix, object_key)
        store.put(source_key, BytesIO(body), digest)
        sidecar_key = attempt_object_key(prefix, f"manifests/{manifest.manifest_id}/manifest.json")
        payload = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
        store.put(sidecar_key, BytesIO(payload), hashlib.sha256(payload).hexdigest())
        ref = ManifestRef(manifest_id=manifest.manifest_id, manifest_key=sidecar_key)
        units.append(RunUnit(
            tenant_id=_TENANT, run_id=_RUN_ID, unit_id=unit_id, stage=stage,
            source_type=None, file_subtype=None, partition="all",
            depends_on_unit_ids=("unit-upstream",), input_manifests=(),
            state=RunUnitState.SUCCEEDED, attempt=1, fencing_token=1, lease_owner=None,
            lease_until=None, dispatch_id=None, output_manifests=(ref,), error_code=None,
        ))

    publisher = DatasetPublisher(store=store, control_plane=adapter)
    request = PublishRequest(run=run, units=tuple(units), expected_version_id=None, now=_NOW)

    store._fault_injector = _CrashOnceInjector(boundary="destination_linked", fail_at_call=2)
    with pytest.raises(OSError):
        publisher.publish(request)

    assert adapter.get_dataset_pointer(_TENANT, "gold") is None
    assert adapter.get_run(_TENANT, _RUN_ID).state is RunState.PUBLISHING

    result = publisher.publish(request)

    assert result.pointer.version_id == _RUN_ID
    assert adapter.get_run(_TENANT, _RUN_ID).state is RunState.PUBLISHED


@pytest.mark.chaos
@pytest.mark.local_profile
def test_crash_apos_cas_permite_replay_sem_republicar(tmp_path):
    """Invariant: retrying an identical publish request after the caller lost the
    response (e.g. crash right after CAS committed) replays the stored pointer
    via the control plane's replay path instead of advancing the dataset again."""
    try:
        from cnes_contracts.manifests.outputs import OutputManifest
        from cnes_domain.control_plane.entities import ManifestRef, Run, RunDependency, RunUnit
        from cnes_domain.control_plane.enums import RunStage, RunState, RunUnitState
        from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
        from cnes_infra.object_store.filesystem import FilesystemObjectStore
        from data_processor.orchestration.attempt_store import (
            attempt_object_key,
            unit_attempt_prefix,
        )
        from data_processor.orchestration.publisher import DatasetPublisher, PublishRequest
    except ImportError:
        pytest.skip("control plane / orchestration modules not available")

    try:
        store = FilesystemObjectStore(tmp_path / "objects")
    except OSError:
        pytest.skip("filesystem object store unsupported")

    adapter = SQLiteControlPlane(tmp_path / "cp.db", lambda: _NOW)
    adapter.initialize()
    run = Run(
        tenant_id=_TENANT, run_id=_RUN_ID, competencia=_COMPETENCIA, dataset_name="gold",
        state=RunState.PUBLISHING,
        dependencies=(RunDependency(source_type="CNES", file_subtype="ST", required=True),),
        missing_sources=(), created_at=_NOW,
    )
    adapter.put_run(run)

    body = b"payload-a"
    digest = hashlib.sha256(body).hexdigest()
    object_key = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/part-a.parquet"
    manifest = OutputManifest(
        manifest_version=1, manifest_id="manifest-a", tenant_id=_TENANT,
        layer="reconciliation", source_type=None, competencia=_COMPETENCIA,
        run_id=_RUN_ID, unit_id="unit-m", attempt=1,
        schema_version="gold-v1", object_key=object_key, object_sha256=digest,
        row_count=10, created_at=_NOW,
    )
    prefix = unit_attempt_prefix(
        SimpleNamespace(tenant_id=_TENANT, run_id=_RUN_ID, unit_id="unit-m", attempt=1)
    )
    source_key = attempt_object_key(prefix, object_key)
    store.put(source_key, BytesIO(body), digest)
    sidecar_key = attempt_object_key(prefix, f"manifests/{manifest.manifest_id}/manifest.json")
    payload = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    store.put(sidecar_key, BytesIO(payload), hashlib.sha256(payload).hexdigest())
    ref = ManifestRef(manifest_id=manifest.manifest_id, manifest_key=sidecar_key)
    unit = RunUnit(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id="unit-m", stage=RunStage.MATERIALIZE,
        source_type=None, file_subtype=None, partition="all",
        depends_on_unit_ids=("unit-upstream",), input_manifests=(),
        state=RunUnitState.SUCCEEDED, attempt=1, fencing_token=1, lease_owner=None,
        lease_until=None, dispatch_id=None, output_manifests=(ref,), error_code=None,
    )

    publisher = DatasetPublisher(store=store, control_plane=adapter)
    request = PublishRequest(run=run, units=(unit,), expected_version_id=None, now=_NOW)

    first = publisher.publish(request)
    second = publisher.publish(request)

    assert second.pointer == first.pointer
    assert adapter.get_run(_TENANT, _RUN_ID).state is RunState.PUBLISHED
