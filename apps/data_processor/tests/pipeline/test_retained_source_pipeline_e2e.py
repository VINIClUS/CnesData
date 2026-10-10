"""E2E das fontes retidas: três ondas até PUBLISHED sobre SQLite e filesystem reais."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest

from apps.data_processor.tests.sources.sihd import raw_rows as sihd_rows
from apps.data_processor.tests.sources.sihd import raw_spec as sihd_spec
from central_api.services.run_planning import RunPlanningDependencies, RunPlanningService
from cnes_contracts.manifests.outputs import RunManifest
from cnes_contracts.manifests.raw import RawManifest, SourceType
from cnes_domain.control_plane.entities import RawManifestRecord, Run, RunUnit, Tenant
from cnes_domain.control_plane.enums import RunStage, RunState, RunUnitState
from cnes_domain.orchestration.planner import PlanRequest, RawManifestRef, plan_run
from cnes_domain.orchestration.source_catalog import build_source_catalog
from cnes_domain.ports.processing import (
    CancelRunExecution,
    ExecutionPolicyConfig,
    ExecutionStatus,
    RunUnitMessage,
    StartRunExecution,
)
from cnes_infra.billing import (
    LOCAL_BILLING_SETTINGS,
    BillingGateResources,
    build_execution_callbacks,
)
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.object_store import FilesystemObjectStore
from data_processor.composition import build_source_registry
from data_processor.orchestration.coordinator import (
    CoordinatorDependencies,
    PipelineCoordinator,
    noop_execution_started,
)
from data_processor.orchestration.publisher import DatasetPublisher
from data_processor.orchestration.unit_handler import RunUnitCommandHandler
from data_processor.orchestration.unit_worker import (
    UnitWorker,
    UnitWorkerDependencies,
    UnitWorkerPolicy,
)
from data_processor.pipeline.stage_processor import StageProcessor

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
_TENANT = "354130"
_COMPETENCIA = "2026-01"
_RUN_ID = "run-e2e"
_LIMIT = 4
_LEASE = 300
_MAX_WAVES = 6
_WAVE_STAGES = ((RunStage.NORMALIZE,), (RunStage.RECONCILE,), (RunStage.MATERIALIZE,))
_NOW = datetime(2026, 10, 1, 12, tzinfo=UTC)
_STRINGS = ("prd_uid", "prd_cmp", "prd_apanum", "prd_pa", "prd_cbo", "prd_cidpri")
_BPI_STRINGS = ("bpi_uid", "bpi_cmp", "bpi_cnsmed", "bpi_cbo", "bpi_flh", "bpi_seq",
                "bpi_pa", "bpi_cid", "bpi_dtaten")
_SIA_BPI = {**dict.fromkeys(_BPI_STRINGS, pl.String), "bpi_qt_p": pl.Int64, "bpi_qt_a": pl.Int64}
_SIA_DTYPES: dict[str, dict[str, Any]] = {
    "SIA_APA": {
        **dict.fromkeys(_STRINGS, pl.String),
        **dict.fromkeys(("prd_qt_p", "prd_qt_a", "prd_vl_p", "prd_vl_a"), pl.Int64),
        **dict.fromkeys(("apa_dtinic", "apa_dtfim", "apa_cnsexe"), pl.String),
    },
    "SIA_BPI": _SIA_BPI,
    "SIA_BPIHST": _SIA_BPI,
    "DIM_SIGTAP": dict.fromkeys(
        ("co_procedimento", "no_procedimento", "tp_complexidade", "co_financiamento",
         "dt_competencia"), pl.String),
    "DIM_MUNICIPIO": {
        "coduf": pl.String, "codmunic": pl.String, "nome": pl.String, "condic": pl.String,
        "tetopab": pl.Float64, "calcpab": pl.Float64,
    },
}


@dataclass(slots=True)
class _MutableClock:
    instant: datetime

    def now(self) -> datetime:
        return self.instant


@dataclass
class _FakeExecutor:
    statuses: dict[str, ExecutionStatus] = field(default_factory=dict)
    started: list[StartRunExecution] = field(default_factory=list)

    def start(self, request: StartRunExecution) -> str:
        reference = f"exec-{request.dispatch_id}"
        self.started.append(request)
        self.statuses.setdefault(reference, ExecutionStatus.RUNNING)
        return reference

    def cancel(self, request: CancelRunExecution) -> None:
        raise AssertionError(f"cancel_unexpected run_id={request.run_id}")

    def status(self, execution_ref: str) -> ExecutionStatus:
        return self.statuses[execution_ref]


@dataclass(frozen=True, slots=True)
class _Runtime:
    control_plane: SQLiteControlPlane
    store: FilesystemObjectStore
    executor: _FakeExecutor
    service: RunPlanningService
    coordinator: PipelineCoordinator
    handler: RunUnitCommandHandler
    clock: _MutableClock


@dataclass(frozen=True, slots=True)
class _Wave:
    wave_id: str
    dispatch_id: str
    stages: tuple[RunStage, ...]


def _build_runtime(tmp_path: Path) -> _Runtime:
    clock = _MutableClock(_NOW)
    control_plane = SQLiteControlPlane(tmp_path / "state.db", clock.now)
    control_plane.initialize()
    control_plane.put_tenant(Tenant(
        tenant_id=_TENANT, municipality_name="tenant-354130", created_at=_NOW,
    ))
    store = FilesystemObjectStore(tmp_path / "objects")
    executor = _FakeExecutor()
    processor = StageProcessor(control_plane, store, build_source_registry(), clock.now)
    publisher = DatasetPublisher(store=store, control_plane=control_plane)
    execution = ExecutionPolicyConfig(_LIMIT, _LEASE, build_execution_callbacks(
        LOCAL_BILLING_SETTINGS, control_plane, BillingGateResources(clock.now, _LIMIT),
        noop_execution_started,
    ))
    coordinator = PipelineCoordinator(
        CoordinatorDependencies(control_plane, executor, publisher, clock.now), execution,
    )
    worker = UnitWorker(
        UnitWorkerDependencies(
            control_plane=control_plane, store=store, processor=processor, clock=clock.now,
        ),
        UnitWorkerPolicy(after_persist=lambda unit: coordinator.resume(
            unit.tenant_id, unit.run_id,
        )),
    )
    service = RunPlanningService(
        RunPlanningDependencies(control_plane, store, executor, build_source_catalog()),
        execution, clock.now,
    )
    return _Runtime(
        control_plane, store, executor, service, coordinator, RunUnitCommandHandler(worker),
        clock,
    )


def _fixture(source: str, name: str) -> Any:
    return json.loads((_FIXTURES / source / name).read_text(encoding="utf-8"))


def _sihd_inputs() -> list[tuple[dict[str, Any], pl.DataFrame]]:
    subtypes = ("SIHD_INTERNACAO", "SIHD_PROC_AIH")
    return [
        ({k: v for k, v in sihd_spec(s).items() if k != "rows_file"},
         pl.DataFrame(sihd_rows(s)))
        for s in subtypes
    ]


def _bpa_inputs() -> list[tuple[dict[str, Any], pl.DataFrame]]:
    rows = _fixture("bpa", "raw_rows.json")
    return [
        (_fixture("bpa", f"raw_manifest_{s.lower()}.json"),
         pl.DataFrame(rows[s], schema_overrides={"prd_qt_p": pl.Float64}))
        for s in ("BPA_C", "BPA_I")
    ]


def _sia_inputs() -> list[tuple[dict[str, Any], pl.DataFrame]]:
    rows = _fixture("sia", "raw_rows.json")
    templates = _fixture("sia", "raw_manifests.json")
    return [
        (t, pl.DataFrame(rows[t["file_subtype"]], schema=_SIA_DTYPES[t["file_subtype"]]))
        for t in templates
    ]


_RAW_INPUTS = {"sihd": _sihd_inputs, "bpa": _bpa_inputs, "sia": _sia_inputs}


def _seed_raw(
    runtime: _Runtime, spec: dict[str, Any], frame: pl.DataFrame,
) -> RawManifestRef:
    output = BytesIO()
    frame.write_parquet(output, compression="zstd", compression_level=3)
    body = output.getvalue()
    digest = sha256(body).hexdigest()
    object_key = spec["object_key"].replace(spec["competencia"], _COMPETENCIA)
    runtime.store.put(object_key, BytesIO(body), digest)
    manifest = RawManifest.model_validate_json(json.dumps({
        **spec, "competencia": _COMPETENCIA, "object_key": object_key,
        "object_sha256": digest, "size_bytes": len(body), "row_count": frame.height,
    }))
    key = (f"raw/{_TENANT}/{manifest.source_type.value}/{_COMPETENCIA}/"
           f"{manifest.snapshot_id}/manifest.json")
    payload = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    runtime.store.put(key, BytesIO(payload), sha256(payload).hexdigest())
    _register(runtime.control_plane, manifest, key, sha256(payload).hexdigest())
    return RawManifestRef(
        manifest_id=manifest.manifest_id, manifest_key=key,
        source_type=manifest.source_type.value, file_subtype=manifest.file_subtype,
        partition=_COMPETENCIA,
    )


def _register(
    control_plane: SQLiteControlPlane, manifest: RawManifest, key: str, digest: str,
) -> None:
    record = RawManifestRecord(
        tenant_id=_TENANT, manifest_id=manifest.manifest_id, manifest_key=key,
        agent_id=manifest.agent_id, source_type=manifest.source_type.value,
        file_subtype=manifest.file_subtype, competencia=_COMPETENCIA,
        snapshot_mode="FULL", snapshot_id=manifest.snapshot_id, base_snapshot_id=None,
        sequence=manifest.sequence, previous_manifest_sha256=None, manifest_sha256=digest,
        created_at=_NOW,
    )
    with control_plane.write_transaction() as connection:
        control_plane.put_manifest_record(connection, record)


def _planned_run(runtime: _Runtime, pipeline_id: str) -> tuple[Run, tuple[RawManifestRef, ...]]:
    definition = build_source_catalog().for_pipeline(pipeline_id)
    refs = tuple(_seed_raw(runtime, spec, frame) for spec, frame in _RAW_INPUTS[pipeline_id]())
    assert len(refs) == len(definition.dependencies)
    run = Run(
        tenant_id=_TENANT, run_id=_RUN_ID, competencia=_COMPETENCIA, dataset_name=pipeline_id,
        state=RunState.PLANNED, dependencies=definition.dependencies, missing_sources=(),
        created_at=_NOW,
    )
    runtime.control_plane.put_run(run)
    return run, refs


def _message(runtime: _Runtime, dispatch: Any, unit_id: str) -> RunUnitMessage:
    return RunUnitMessage(
        tenant_id=dispatch.tenant_id, run_id=dispatch.run_id, wave_id=dispatch.wave_id,
        dispatch_id=dispatch.dispatch_id, unit_id=unit_id, owner=dispatch.execution_ref,
        now=runtime.clock.now(), lease_seconds=_LEASE,
    )


def _drive_wave(runtime: _Runtime) -> _Wave:
    control_plane = runtime.control_plane
    assert control_plane.get_dataset_pointer(_TENANT, _pointer_dataset(runtime)) is None
    dispatch = control_plane.get_active_run_dispatch(_TENANT, _RUN_ID)
    assert dispatch is not None
    by_id = {u.unit_id: u.stage for u in control_plane.list_run_units(_TENANT, _RUN_ID)}
    for unit_id in dispatch.unit_ids:
        unit = runtime.handler.handle(_message(runtime, dispatch, unit_id))
        assert unit.state is RunUnitState.SUCCEEDED
        assert (unit.tenant_id, unit.run_id) == (_TENANT, _RUN_ID)
    runtime.executor.statuses[cast("str", dispatch.execution_ref)] = ExecutionStatus.SUCCEEDED
    runtime.coordinator.recover()
    stages = tuple(sorted({by_id[unit_id] for unit_id in dispatch.unit_ids}))
    return _Wave(dispatch.wave_id, dispatch.dispatch_id, stages)


def _pointer_dataset(runtime: _Runtime) -> str:
    run = runtime.control_plane.get_run(_TENANT, _RUN_ID)
    assert run is not None
    assert run.state is RunState.PROCESSING
    return run.dataset_name


def _drain(runtime: _Runtime) -> tuple[_Wave, ...]:
    waves: list[_Wave] = []
    for _ in range(_MAX_WAVES):
        run = runtime.control_plane.get_run(_TENANT, _RUN_ID)
        assert run is not None
        if run.state is not RunState.PROCESSING:
            return tuple(waves)
        waves.append(_drive_wave(runtime))
    raise AssertionError("run_terminal=unreached")


def _by_id(units: tuple[RunUnit, ...]) -> list[str]:
    return [unit.model_dump_json() for unit in sorted(units, key=lambda u: u.unit_id)]


def _assert_dag_replay(
    runtime: _Runtime, planned: Run, refs: tuple[RawManifestRef, ...],
) -> None:
    persisted = runtime.control_plane.list_run_units(_TENANT, _RUN_ID)
    replanned = plan_run(PlanRequest(run=planned, manifests=refs, deployment_limit=_LIMIT))
    assert _by_id(persisted) == _by_id(replanned.units)


def _assert_manifest_integrity(runtime: _Runtime, run: Run, pipeline_id: str) -> None:
    pointer = runtime.control_plane.get_dataset_pointer(_TENANT, pipeline_id)
    assert pointer is not None
    assert (pointer.dataset_name, pointer.version_id) == (pipeline_id, _RUN_ID)
    version = runtime.control_plane.get_dataset_version(_TENANT, pipeline_id, pointer.version_id)
    assert version is not None
    assert version.run_id == run.run_id
    with runtime.store.open(version.run_manifest_key) as stream:
        manifest = RunManifest.model_validate_json(stream.read())
    assert manifest.dataset_name == pipeline_id
    for output in manifest.outputs:
        stat = runtime.store.stat(output.object_key)
        assert stat is not None
        assert stat.sha256 == output.object_sha256
    definition = build_source_catalog().for_pipeline(pipeline_id)
    serving = {o.object_key for o in manifest.outputs if o.layer == "serving"}
    assert serving == {
        f"serving/{_TENANT}/{_RUN_ID}/{doc}.json" for doc in definition.layout.serving_documents
    }


@pytest.mark.parametrize("pipeline_id", ["sihd", "bpa", "sia"])
def test_fonte_retida_publica_em_tres_ondas(tmp_path: Path, pipeline_id: str) -> None:
    runtime = _build_runtime(tmp_path)
    definition = build_source_catalog().for_pipeline(pipeline_id)
    assert all(SourceType(source_type) for source_type in definition.source_types)
    planned, refs = _planned_run(runtime, pipeline_id)

    launched = runtime.service.launch(_TENANT, _RUN_ID)
    assert launched.run.state is RunState.PROCESSING
    assert len(runtime.executor.started) == 1
    _assert_dag_replay(runtime, planned, refs)
    relaunched = runtime.service.launch(_TENANT, _RUN_ID)
    assert relaunched.execution_ref == launched.execution_ref
    assert len(runtime.executor.started) == 1

    waves = _drain(runtime)

    assert tuple(wave.stages for wave in waves) == _WAVE_STAGES
    assert len({wave.wave_id for wave in waves}) == 3
    assert len({wave.dispatch_id for wave in waves}) == 3
    assert len(runtime.executor.started) == 3
    assert {(r.tenant_id, r.run_id) for r in runtime.executor.started} == {(_TENANT, _RUN_ID)}
    final = runtime.control_plane.get_run(_TENANT, _RUN_ID)
    assert final is not None
    assert final.state is RunState.PUBLISHED
    _assert_manifest_integrity(runtime, final, pipeline_id)
    units = runtime.control_plane.list_run_units(_TENANT, _RUN_ID)
    assert {unit.state for unit in units} == {RunUnitState.SUCCEEDED}
