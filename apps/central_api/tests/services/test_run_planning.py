"""TDD do RunPlanningService: matriz de launch, cadeia raw, dispatch e cancelamento."""
from __future__ import annotations

import hashlib
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import TYPE_CHECKING

import pytest

from central_api.services.run_planning import (
    RunPlanningDependencies,
    RunPlanningService,
)
from cnes_contracts.manifests.raw import RawManifest, SnapshotMode, SourceType
from cnes_domain.control_plane.entities import RawManifestRecord, Run, RunDependency
from cnes_domain.control_plane.enums import RunState
from cnes_domain.orchestration.source_catalog import build_source_catalog
from cnes_domain.ports.object_store import ObjectStat
from cnes_domain.ports.processing import (
    CancelRunExecution,
    ExecutionCallbacks,
    ExecutionPolicyConfig,
    ExecutionStatus,
    StartRunExecution,
)
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from data_processor.orchestration.coordinator import allow_execution, noop_execution_started

if TYPE_CHECKING:
    from collections.abc import BinaryIO
    from contextlib import AbstractContextManager as ContextManager

_TENANT = "354130"
_RUN_ID = "run-a"
_COMPETENCIA = "2026-01"
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

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        data = body.read()
        digest = hashlib.sha256(data).hexdigest()
        self.objects[key] = data
        return ObjectStat(key=key, size_bytes=len(data), sha256=digest)

    def open(self, key: str) -> ContextManager[BinaryIO]:
        return nullcontext(BytesIO(self.objects[key]))

    def stat(self, key: str) -> ObjectStat | None:
        data = self.objects.get(key)
        if data is None:
            return None
        return ObjectStat(key=key, size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

    def delete(self, key: str) -> None:
        self.objects.pop(key, None)

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        data = self.objects.pop(source_key)
        self.objects[destination_key] = data
        return ObjectStat(key=destination_key, size_bytes=len(data), sha256=expected_sha256)


@dataclass
class _FakeExecutor:
    statuses: dict[str, ExecutionStatus] = field(default_factory=dict)
    started: list[StartRunExecution] = field(default_factory=list)
    canceled: list[CancelRunExecution] = field(default_factory=list)

    def start(self, request: StartRunExecution) -> str:
        ref = f"exec-{request.dispatch_id}"
        self.started.append(request)
        self.statuses.setdefault(ref, ExecutionStatus.RUNNING)
        return ref

    def cancel(self, request: CancelRunExecution) -> None:
        self.canceled.append(request)

    def status(self, execution_ref: str) -> ExecutionStatus:
        return self.statuses[execution_ref]

    def set_status(self, execution_ref: str, status: ExecutionStatus) -> None:
        self.statuses[execution_ref] = status


@pytest.fixture
def clock() -> _MutableClock:
    return _MutableClock(_NOW)


@pytest.fixture
def adapter(tmp_path, clock: _MutableClock) -> SQLiteControlPlane:
    control_plane = SQLiteControlPlane(tmp_path / "cp.db", clock.now)
    control_plane.initialize()
    return control_plane


@pytest.fixture
def store() -> _FakeObjectStore:
    return _FakeObjectStore()


@pytest.fixture
def executor() -> _FakeExecutor:
    return _FakeExecutor()


def _cnes_dependencies() -> tuple[RunDependency, ...]:
    return build_source_catalog().for_pipeline("cnes").dependencies


def _run(*, state: RunState = RunState.PLANNED, run_id: str = _RUN_ID) -> Run:
    return Run(
        tenant_id=_TENANT, run_id=run_id, competencia=_COMPETENCIA, dataset_name="cnes",
        state=state, dependencies=_cnes_dependencies(), missing_sources=(), created_at=_NOW,
    )


def _seed_raw_manifest(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, *,
    source_type: str, snapshot_id: str, sequence: int = 1,
) -> None:
    key = f"raw/{_TENANT}/{source_type}/{_COMPETENCIA}/{snapshot_id}/manifest.json"
    manifest = RawManifest(
        manifest_version=1, manifest_id=f"manifest-{source_type}-{snapshot_id}",
        tenant_id=_TENANT, source_type=SourceType(source_type), file_subtype="CNES_VINCULO",
        competencia=_COMPETENCIA, agent_id="agent-1", agent_version="1.0", schema_version="v1",
        snapshot_mode=SnapshotMode.FULL, snapshot_id=snapshot_id, base_snapshot_id=None,
        sequence=sequence, previous_manifest_sha256=None,
        object_sha256="a" * 64, row_count=1, size_bytes=10,
        object_key=f"raw/{_TENANT}/{source_type}/{_COMPETENCIA}/{snapshot_id}/data.parquet",
        created_at=_NOW,
    )
    payload = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    store.objects[key] = payload
    record = RawManifestRecord(
        tenant_id=_TENANT, manifest_id=manifest.manifest_id, manifest_key=key,
        agent_id="agent-1", source_type=source_type, file_subtype="CNES_VINCULO",
        competencia=_COMPETENCIA, snapshot_mode="FULL", snapshot_id=snapshot_id,
        base_snapshot_id=None, sequence=sequence, previous_manifest_sha256=None,
        manifest_sha256=hashlib.sha256(payload).hexdigest(), created_at=_NOW,
    )
    with adapter.write_transaction() as connection:
        adapter.put_manifest_record(connection, record)


def _seed_full_chain(adapter: SQLiteControlPlane, store: _FakeObjectStore) -> None:
    _seed_raw_manifest(adapter, store, source_type="CNES_LOCAL", snapshot_id="local-1")
    _seed_raw_manifest(adapter, store, source_type="CNES_NACIONAL", snapshot_id="nacional-1")


def _dependencies(
    adapter: SQLiteControlPlane, executor: _FakeExecutor, store: _FakeObjectStore,
) -> RunPlanningDependencies:
    return RunPlanningDependencies(
        control_plane=adapter, object_store=store, executor=executor,
        source_catalog=build_source_catalog(),
    )


def _execution(
    *, policy=allow_execution, started=noop_execution_started, limit: int = 2,
) -> ExecutionPolicyConfig:
    return ExecutionPolicyConfig(limit, 300, ExecutionCallbacks(policy, started))


def _service(adapter, executor, store, clock, **execution_kwargs) -> RunPlanningService:
    return RunPlanningService(
        _dependencies(adapter, executor, store), _execution(**execution_kwargs), clock.now,
    )


def test_launch_sem_input_necessario_fica_waiting_inputs(adapter, executor, store, clock):
    adapter.put_run(_run())
    service = _service(adapter, executor, store, clock)

    result = service.launch(_TENANT, _RUN_ID)

    assert result.run.state is RunState.WAITING_INPUTS
    assert result.run.missing_sources == ("CNES_LOCAL/CNES_VINCULO",)
    assert result.plan is None
    assert result.execution_ref is None
    assert adapter.list_run_units(_TENANT, _RUN_ID) == ()


def test_launch_relancado_em_waiting_inputs_nao_re_transiciona(adapter, executor, store, clock):
    adapter.put_run(_run())
    service = _service(adapter, executor, store, clock)
    first = service.launch(_TENANT, _RUN_ID)
    assert first.run.state is RunState.WAITING_INPUTS

    second = service.launch(_TENANT, _RUN_ID)

    assert second.run.state is RunState.WAITING_INPUTS
    assert second.run.missing_sources == first.run.missing_sources


def test_launch_reconstroi_cadeia_raw_e_desperta_dispatch(adapter, executor, store, clock):
    adapter.put_run(_run())
    _seed_full_chain(adapter, store)
    service = _service(adapter, executor, store, clock)

    result = service.launch(_TENANT, _RUN_ID)

    assert result.run.state is RunState.PROCESSING
    assert result.execution_ref is not None
    units = adapter.list_run_units(_TENANT, _RUN_ID)
    assert len(units) == 4
    dispatch = adapter.get_active_run_dispatch(_TENANT, _RUN_ID)
    assert dispatch.state.value == "STARTED"


def test_persistencia_imutavel_das_units_antes_do_executor_start(adapter, executor, store, clock):
    adapter.put_run(_run())
    _seed_full_chain(adapter, store)
    order: list[str] = []
    original_put_units = adapter.put_run_units

    def _put_units(command):
        order.append("put_run_units")
        return original_put_units(command)

    def _start(request):
        order.append("start")
        return f"exec-{request.dispatch_id}"

    adapter.put_run_units = _put_units
    executor.start = _start
    service = _service(adapter, executor, store, clock)

    service.launch(_TENANT, _RUN_ID)

    assert order == ["put_run_units", "start"]


def test_launch_congela_fonte_opcional_ausente(adapter, executor, store, clock):
    adapter.put_run(_run())
    _seed_raw_manifest(adapter, store, source_type="CNES_LOCAL", snapshot_id="local-1")
    service = _service(adapter, executor, store, clock)

    result = service.launch(_TENANT, _RUN_ID)

    assert result.run.state is RunState.PROCESSING
    assert result.run.missing_sources == ("CNES_NACIONAL/CNES_VINCULO",)
    units = adapter.list_run_units(_TENANT, _RUN_ID)
    assert len(units) == 3


def test_replay_de_put_run_units_e_idempotente(adapter, executor, store, clock):
    from cnes_domain.control_plane.commands import PutRunUnits
    from cnes_domain.orchestration.planner import PlanRequest, plan_run

    run = _run()
    adapter.put_run(run)
    _seed_full_chain(adapter, store)
    definition = build_source_catalog().for_pipeline("cnes")
    assert definition.dependencies == run.dependencies
    service = _service(adapter, executor, store, clock)
    from central_api.services.run_planning import _raw_chain

    refs = _raw_chain(_dependencies(adapter, executor, store), run)
    plan = plan_run(PlanRequest(run=run, manifests=refs, deployment_limit=2))
    adapter.put_run_units(PutRunUnits(
        tenant_id=_TENANT, run_id=_RUN_ID, expected_run_state=RunState.PLANNED, units=plan.units,
    ))

    result = service.launch(_TENANT, _RUN_ID)

    assert result.run.state is RunState.PROCESSING
    assert len(adapter.list_run_units(_TENANT, _RUN_ID)) == 4


def test_launch_run_inexistente_falha(adapter, executor, store, clock):
    service = _service(adapter, executor, store, clock)

    with pytest.raises(ValueError, match="run_not_found"):
        service.launch(_TENANT, "missing-run")


def test_launch_dependencies_divergentes_do_catalogo_falha(adapter, executor, store, clock):
    mismatched = _run().model_copy(update={
        "dependencies": (RunDependency(source_type="CNES_LOCAL", file_subtype="OTHER",
                                        required=True),)
    })
    adapter.put_run(mismatched)
    service = _service(adapter, executor, store, clock)

    with pytest.raises(ValueError, match="dependency_definition_mismatch"):
        service.launch(_TENANT, _RUN_ID)


@pytest.mark.parametrize("state", [
    RunState.PUBLISHED, RunState.PUBLISHED_DEGRADED, RunState.FAILED, RunState.CANCELED,
])
def test_launch_terminal_e_noop_read_only(adapter, executor, store, clock, state):
    terminal = _run(state=state)
    adapter.put_run(terminal)
    service = _service(adapter, executor, store, clock)

    result = service.launch(_TENANT, _RUN_ID)

    assert result.run.state is state
    assert result.plan is None
    assert result.execution_ref is None


def test_launch_publishing_e_noop_read_only_processador_publica(adapter, executor, store, clock):
    publishing = _run(state=RunState.PUBLISHING)
    adapter.put_run(publishing)
    service = _service(adapter, executor, store, clock)

    result = service.launch(_TENANT, _RUN_ID)

    assert result.run.state is RunState.PUBLISHING
    assert result.plan is None
    assert result.execution_ref is None
    assert len(executor.started) == 0


def test_launch_cancel_requested_finaliza_run(adapter, executor, store, clock):
    adapter.put_run(_run())
    _seed_full_chain(adapter, store)
    service = _service(adapter, executor, store, clock)
    service.launch(_TENANT, _RUN_ID)
    canceling = adapter.get_run(_TENANT, _RUN_ID).model_copy(
        update={"state": RunState.CANCEL_REQUESTED}
    )
    adapter.put_run(canceling)

    result = service.launch(_TENANT, _RUN_ID)

    assert result.run.state is RunState.CANCELED
    assert len(executor.canceled) == 1
    assert executor.canceled[0].execution_ref is None


def test_launch_processing_nunca_replaneja(adapter, executor, store, clock):
    adapter.put_run(_run())
    _seed_full_chain(adapter, store)
    service = _service(adapter, executor, store, clock)
    service.launch(_TENANT, _RUN_ID)
    units_before = adapter.list_run_units(_TENANT, _RUN_ID)

    result = service.launch(_TENANT, _RUN_ID)

    assert result.run.state is RunState.PROCESSING
    assert adapter.list_run_units(_TENANT, _RUN_ID) == units_before


def test_recover_relanca_estados_limitados_incluindo_publishing_replay(
    adapter, executor, store, clock
):
    adapter.put_run(_run())
    _seed_full_chain(adapter, store)
    service = _service(adapter, executor, store, clock)
    service.launch(_TENANT, _RUN_ID)
    publishing = adapter.get_run(_TENANT, _RUN_ID).model_copy(
        update={"state": RunState.PUBLISHING}
    )
    adapter.put_run(publishing)
    waiting = _run(state=RunState.WAITING_INPUTS, run_id="run-waiting")
    adapter.put_run(waiting)

    results = service.recover(limit=10)

    assert {r.run.run_id for r in results} == {_RUN_ID, "run-waiting"}
    replayed = next(r for r in results if r.run.run_id == _RUN_ID)
    assert replayed.run.state is RunState.PUBLISHING
    assert replayed.plan is None
    assert replayed.execution_ref is None
    waiting_result = next(r for r in results if r.run.run_id == "run-waiting")
    assert waiting_result.run.state is RunState.PROCESSING


def test_on_raw_manifest_accepted_lanca_apenas_runs_compativeis_e_retorna_none(
    adapter, executor, store, clock
):
    waiting_match = _run(state=RunState.WAITING_INPUTS, run_id="run-match")
    waiting_other = _run(state=RunState.WAITING_INPUTS, run_id="run-other").model_copy(
        update={"dependencies": (
            RunDependency(source_type="SIHD", file_subtype="SIH", required=True),
        )}
    )
    adapter.put_run(waiting_match)
    adapter.put_run(waiting_other)
    _seed_full_chain(adapter, store)
    service = _service(adapter, executor, store, clock)
    record = RawManifestRecord(
        tenant_id=_TENANT, manifest_id="manifest-trigger", manifest_key=(
            f"raw/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/local-1/manifest.json"
        ),
        agent_id="agent-1", source_type="CNES_LOCAL", file_subtype="CNES_VINCULO",
        competencia=_COMPETENCIA, snapshot_mode="FULL", snapshot_id="local-1",
        base_snapshot_id=None, sequence=1, previous_manifest_sha256=None,
        manifest_sha256="a" * 64, created_at=_NOW,
    )

    outcome = service.on_raw_manifest_accepted(record)

    assert outcome is None
    assert adapter.get_run(_TENANT, "run-match").state is RunState.PROCESSING
    assert adapter.get_run(_TENANT, "run-other").state is RunState.WAITING_INPUTS
