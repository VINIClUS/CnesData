"""TDD do PipelineCoordinator: protocolo reserve->start->bind + fan-in CND-041."""
from __future__ import annotations

import hashlib
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import TYPE_CHECKING

import pytest

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.raw import SourceType
from cnes_domain.control_plane.commands import (
    ClaimRunUnit,
    PutRunUnits,
    ReserveRunDispatch,
)
from cnes_domain.control_plane.entities import Run, RunDependency, RunUnit
from cnes_domain.control_plane.enums import DispatchState, RunStage, RunState, RunUnitState
from cnes_domain.orchestration.planner import (
    PlanRequest,
    RawManifestRef,
    RunPlan,
    logical_wave_id,
    plan_run,
    ready_units,
)
from cnes_domain.ports.object_store import ObjectStat
from cnes_domain.ports.processing import (
    CancelRunExecution,
    ExecutionCallbacks,
    ExecutionPermit,
    ExecutionPolicyConfig,
    ExecutionStatus,
    StartRunExecution,
)
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from data_processor.orchestration.coordinator import (
    CoordinatorDependencies,
    CoordinatorResult,
    PipelineCoordinator,
    allow_execution,
    noop_execution_started,
)
from data_processor.orchestration.publisher import DatasetPublisher
from data_processor.orchestration.unit_worker import (
    UnitWorker,
    UnitWorkerDependencies,
    UnitWorkerPolicy,
)

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
        if digest != expected_sha256:
            raise ValueError(f"sha256_mismatch key={key}")
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


@dataclass
class _RestartedExecutor(_FakeExecutor):
    def status(self, execution_ref: str) -> ExecutionStatus:
        raise ValueError(f"execution_ref=unknown ref={execution_ref}")


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


def _dependencies_cnes() -> tuple[RunDependency, ...]:
    return (
        RunDependency(source_type="CNES_LOCAL", file_subtype="CNES_VINCULO", required=True),
        RunDependency(source_type="CNES_NACIONAL", file_subtype="CNES_VINCULO", required=False),
    )


def _run(
    *, state: RunState = RunState.PROCESSING, missing_sources: tuple[str, ...] = ()
) -> Run:
    return Run(
        tenant_id=_TENANT, run_id=_RUN_ID, competencia=_COMPETENCIA, dataset_name="cnes",
        state=state, dependencies=_dependencies_cnes(), missing_sources=missing_sources,
        created_at=_NOW,
    )


def _raw_ref(source_type: str, suffix: str) -> RawManifestRef:
    return RawManifestRef(
        manifest_id=f"raw-{suffix}",
        manifest_key=f"raw/{_TENANT}/{source_type}/{_COMPETENCIA}/{suffix}/manifest.json",
        source_type=source_type, file_subtype="CNES_VINCULO", partition=_COMPETENCIA,
    )


def _full_manifests() -> tuple[RawManifestRef, ...]:
    return (_raw_ref("CNES_LOCAL", "local"), _raw_ref("CNES_NACIONAL", "nacional"))


def _local_only_manifests() -> tuple[RawManifestRef, ...]:
    return (_raw_ref("CNES_LOCAL", "local"),)


def _seed(
    adapter: SQLiteControlPlane, *, manifests: tuple[RawManifestRef, ...],
    missing_sources: tuple[str, ...] = (), deployment_limit: int = 2,
) -> Run:
    run = _run(missing_sources=missing_sources)
    adapter.put_run(run)
    plan = plan_run(PlanRequest(run=run, manifests=manifests, deployment_limit=deployment_limit))
    adapter.put_run_units(PutRunUnits(
        tenant_id=_TENANT, run_id=_RUN_ID, expected_run_state=RunState.PROCESSING, units=plan.units,
    ))
    return run


def _reconstruct_plan(
    adapter: SQLiteControlPlane, run: Run, deployment_limit: int = 2
) -> RunPlan:
    units = adapter.list_run_units(_TENANT, _RUN_ID)
    return RunPlan(
        run=run, units=units, missing_required=(), missing_optional=run.missing_sources,
        deployment_limit=deployment_limit,
    )


def _object_key(layer: str, unit: RunUnit, suffix: str) -> str:
    if layer == "normalized":
        return f"normalized/{_TENANT}/{unit.source_type}/{_COMPETENCIA}/{unit.run_id}/{suffix}.bin"
    if layer == "reconciliation":
        return f"reconciliation/{_TENANT}/{_COMPETENCIA}/{unit.run_id}/{suffix}.bin"
    return f"serving/{_TENANT}/{unit.run_id}/{suffix}.json"


def _layer_for_stage(stage: RunStage) -> str:
    if stage is RunStage.NORMALIZE:
        return "normalized"
    if stage is RunStage.RECONCILE:
        return "reconciliation"
    return "serving"


def _manifest_for_unit(unit: RunUnit, store: _FakeObjectStore, suffix: str) -> OutputManifest:
    layer = _layer_for_stage(unit.stage)
    body = f"payload-{unit.unit_id}-{suffix}".encode()
    digest = hashlib.sha256(body).hexdigest()
    key = _object_key(layer, unit, suffix)
    store.put(key, BytesIO(body), digest)
    return OutputManifest(
        manifest_version=1, manifest_id=f"manifest-{unit.unit_id}-{suffix}",
        tenant_id=unit.tenant_id, layer=layer,
        source_type=SourceType(unit.source_type) if layer == "normalized" else None,
        competencia=_COMPETENCIA, run_id=unit.run_id, unit_id=unit.unit_id, attempt=unit.attempt,
        schema_version="v1", object_key=key, object_sha256=digest, row_count=1, created_at=_NOW,
    )


def _processor(unit: RunUnit, store: object) -> tuple[OutputManifest, ...]:
    return (_manifest_for_unit(unit, store, "a"),)


def _fails_always(unit: RunUnit, store: object) -> tuple[OutputManifest, ...]:
    raise RuntimeError("boom")


def _dependencies(
    adapter: SQLiteControlPlane, executor: _FakeExecutor, store: _FakeObjectStore,
    clock: _MutableClock,
) -> CoordinatorDependencies:
    return CoordinatorDependencies(
        control_plane=adapter, executor=executor,
        publisher=DatasetPublisher(store=store, control_plane=adapter), clock=clock.now,
    )


def _execution(
    *, policy=allow_execution, started=noop_execution_started, limit: int = 2,
    lease_seconds: int = 300,
) -> ExecutionPolicyConfig:
    return ExecutionPolicyConfig(limit, lease_seconds, ExecutionCallbacks(policy, started))


def _complete_dispatch(
    adapter: SQLiteControlPlane, executor: _FakeExecutor, store: _FakeObjectStore,
    clock: _MutableClock, *, processor=_processor, max_attempts: int = 3,
) -> None:
    dispatch = adapter.get_active_run_dispatch(_TENANT, _RUN_ID)
    policy = UnitWorkerPolicy(max_attempts=max_attempts)
    for unit_id in dispatch.unit_ids:
        claim = ClaimRunUnit(
            tenant_id=_TENANT, run_id=_RUN_ID, unit_id=unit_id, dispatch_id=dispatch.dispatch_id,
            owner="worker-a", now=clock.now(), lease_seconds=300,
        )
        worker = UnitWorker(UnitWorkerDependencies(
            control_plane=adapter, store=store, processor=processor, clock=clock.now,
        ), policy)
        worker.execute(claim)
    executor.set_status(dispatch.execution_ref, ExecutionStatus.SUCCEEDED)


def test_tres_ondas_do_executor_ate_publicar(adapter, executor, store, clock):
    _seed(adapter, manifests=_full_manifests())
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())

    wave_one = coordinator.resume(_TENANT, _RUN_ID)
    assert wave_one.state is RunState.PROCESSING
    assert wave_one.execution_ref is not None
    _complete_dispatch(adapter, executor, store, clock)

    wave_two = coordinator.resume(_TENANT, _RUN_ID)
    assert wave_two.state is RunState.PROCESSING
    reconcile_dispatch = adapter.get_active_run_dispatch(_TENANT, _RUN_ID)
    assert len(reconcile_dispatch.unit_ids) == 1
    _complete_dispatch(adapter, executor, store, clock)

    wave_three = coordinator.resume(_TENANT, _RUN_ID)
    assert wave_three.state is RunState.PROCESSING
    materialize_dispatch = adapter.get_active_run_dispatch(_TENANT, _RUN_ID)
    assert len(materialize_dispatch.unit_ids) == 1
    _complete_dispatch(adapter, executor, store, clock)

    final = coordinator.resume(_TENANT, _RUN_ID)
    assert final.state is RunState.PUBLISHED
    assert final.published is True
    assert adapter.get_run(_TENANT, _RUN_ID).state is RunState.PUBLISHED


def test_reconciliacao_nao_comeca_antes_das_duas_normalizacoes(adapter, executor, store, clock):
    _seed(adapter, manifests=_full_manifests())
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())
    coordinator.resume(_TENANT, _RUN_ID)
    dispatch = adapter.get_active_run_dispatch(_TENANT, _RUN_ID)
    only_unit = dispatch.unit_ids[0]
    claim = ClaimRunUnit(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=only_unit, dispatch_id=dispatch.dispatch_id,
        owner="worker-a", now=clock.now(), lease_seconds=300,
    )
    worker = UnitWorker(UnitWorkerDependencies(
        control_plane=adapter, store=store, processor=_processor, clock=clock.now,
    ))
    worker.execute(claim)
    executor.set_status(dispatch.execution_ref, ExecutionStatus.SUCCEEDED)

    result = coordinator.resume(_TENANT, _RUN_ID)

    assert result.state is RunState.PROCESSING
    next_dispatch = adapter.get_active_run_dispatch(_TENANT, _RUN_ID)
    remaining_normalize = dispatch.unit_ids[1]
    assert next_dispatch.unit_ids == (remaining_normalize,)
    units_by_id = {unit.unit_id: unit for unit in adapter.list_run_units(_TENANT, _RUN_ID)}
    assert units_by_id[remaining_normalize].stage is RunStage.NORMALIZE


def test_protocolo_reserve_start_bind_na_ordem_exata(adapter, executor, store, clock):
    order: list[str] = []
    original_reserve = adapter.reserve_run_dispatch
    original_bind = adapter.bind_run_dispatch

    def _reserve(command):
        order.append("reserve")
        return original_reserve(command)

    def _bind(command):
        order.append("bind")
        return original_bind(command)

    def _start(request):
        order.append("start")
        return f"exec-{request.dispatch_id}"

    adapter.reserve_run_dispatch = _reserve
    adapter.bind_run_dispatch = _bind
    executor.start = _start
    _seed(adapter, manifests=_full_manifests())
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())

    coordinator.resume(_TENANT, _RUN_ID)

    assert order == ["reserve", "start", "bind"]


def test_dispatch_reserved_e_recuperado_sem_nova_reserva(adapter, executor, store, clock):
    run = _seed(adapter, manifests=_full_manifests())
    plan = _reconstruct_plan(adapter, run)
    ready = ready_units(plan, clock.now())
    wave = logical_wave_id(ready)
    pre_reserved = adapter.reserve_run_dispatch(ReserveRunDispatch(
        tenant_id=_TENANT, run_id=_RUN_ID, wave_id=wave,
        unit_ids=tuple(sorted(unit.unit_id for unit in ready)), now=clock.now(), lease_seconds=300,
    ))
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())

    coordinator.resume(_TENANT, _RUN_ID)

    active = adapter.get_active_run_dispatch(_TENANT, _RUN_ID)
    assert active.dispatch_id == pre_reserved.dispatch_id
    assert active.state is DispatchState.STARTED
    assert len(executor.started) == 1


def test_sem_dispatch_sobreposto_para_o_mesmo_run(adapter, executor, store, clock):
    _seed(adapter, manifests=_full_manifests())
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())

    coordinator.resume(_TENANT, _RUN_ID)
    first_dispatch = adapter.get_active_run_dispatch(_TENANT, _RUN_ID)
    coordinator.resume(_TENANT, _RUN_ID)
    second_dispatch = adapter.get_active_run_dispatch(_TENANT, _RUN_ID)

    assert first_dispatch.dispatch_id == second_dispatch.dispatch_id
    assert len(executor.started) == 1


def test_execucao_terminal_sem_unit_reivindicada_avanca_generation(adapter, executor, store, clock):
    _seed(adapter, manifests=_full_manifests())
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())
    coordinator.resume(_TENANT, _RUN_ID)
    first_dispatch = adapter.get_active_run_dispatch(_TENANT, _RUN_ID)
    executor.set_status(first_dispatch.execution_ref, ExecutionStatus.FAILED)

    retry = coordinator.resume(_TENANT, _RUN_ID)

    assert retry.state is RunState.PROCESSING
    second_dispatch = adapter.get_active_run_dispatch(_TENANT, _RUN_ID)
    assert second_dispatch.dispatch_id != first_dispatch.dispatch_id
    assert second_dispatch.generation > first_dispatch.generation
    assert second_dispatch.wave_id == first_dispatch.wave_id


def test_falha_final_transiciona_run_para_failed(adapter, executor, store, clock):
    _seed(adapter, manifests=_full_manifests())
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())
    coordinator.resume(_TENANT, _RUN_ID)
    _complete_dispatch(adapter, executor, store, clock, processor=_fails_always, max_attempts=1)

    result = coordinator.resume(_TENANT, _RUN_ID)

    assert result.state is RunState.FAILED
    assert adapter.get_run(_TENANT, _RUN_ID).state is RunState.FAILED


def test_cancelamento_finaliza_dispatch_ativo_e_run(adapter, executor, store, clock):
    _seed(adapter, manifests=_full_manifests())
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())
    coordinator.resume(_TENANT, _RUN_ID)
    canceling = adapter.get_run(_TENANT, _RUN_ID).model_copy(
        update={"state": RunState.CANCEL_REQUESTED}
    )
    adapter.put_run(canceling)

    result = coordinator.resume(_TENANT, _RUN_ID)

    assert result.state is RunState.CANCELED
    assert adapter.get_run(_TENANT, _RUN_ID).state is RunState.CANCELED
    assert len(executor.canceled) == 1
    assert executor.canceled[0].execution_ref is None
    units = adapter.list_run_units(_TENANT, _RUN_ID)
    assert all(unit.state is RunUnitState.CANCELED for unit in units)


def test_publicacao_unica_sob_cas(adapter, executor, store, clock):
    _seed(adapter, manifests=_full_manifests())
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())
    for _ in range(3):
        coordinator.resume(_TENANT, _RUN_ID)
        _complete_dispatch(adapter, executor, store, clock)
    published = coordinator.resume(_TENANT, _RUN_ID)
    assert published.state is RunState.PUBLISHED
    assert published.published is True

    noop = coordinator.resume(_TENANT, _RUN_ID)

    assert noop.state is RunState.PUBLISHED
    assert noop.published is False
    assert adapter.get_dataset_pointer(_TENANT, "cnes").version_id == _RUN_ID


def test_publicacao_degradada_quando_fonte_opcional_ausente(adapter, executor, store, clock):
    _seed(
        adapter, manifests=_local_only_manifests(),
        missing_sources=("CNES_NACIONAL/CNES_VINCULO",),
    )
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())
    for _ in range(3):
        coordinator.resume(_TENANT, _RUN_ID)
        _complete_dispatch(adapter, executor, store, clock)

    result = coordinator.resume(_TENANT, _RUN_ID)

    assert result.state is RunState.PUBLISHED_DEGRADED
    assert adapter.get_run(_TENANT, _RUN_ID).missing_sources == ("CNES_NACIONAL/CNES_VINCULO",)


def test_resume_de_run_terminal_e_read_only(adapter, executor, store, clock):
    run = _run(state=RunState.PUBLISHED)
    adapter.put_run(run)
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())

    result = coordinator.resume(_TENANT, _RUN_ID)

    assert result.state is RunState.PUBLISHED
    assert result.execution_ref is None
    assert result.published is False
    assert adapter.get_active_run_dispatch(_TENANT, _RUN_ID) is None


def test_permit_e_a_mesma_instancia_entregue_ao_callback(adapter, executor, store, clock):
    run = _seed(adapter, manifests=_full_manifests())
    permit = ExecutionPermit(
        tenant_id=run.tenant_id, run_id=run.run_id, max_concurrency=2,
        policy_version=7, fencing_token=11, binding_context=object(),
    )
    received: list[object] = []

    def _started(run, request, execution_ref, delivered_permit) -> None:
        received.append(delivered_permit)

    coordinator = PipelineCoordinator(
        _dependencies(adapter, executor, store, clock),
        _execution(policy=lambda *args: permit, started=_started),
    )

    coordinator.resume(_TENANT, _RUN_ID)

    assert received[0] is permit


def test_recover_reanima_apenas_estados_do_processor(adapter, executor, store, clock):
    _seed(adapter, manifests=_full_manifests())
    waiting = _run(state=RunState.WAITING_INPUTS)
    waiting_alt = waiting.model_copy(update={"run_id": "run-waiting"})
    adapter.put_run(waiting_alt)
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())

    results = coordinator.recover(limit=10)

    assert len(results) == 1
    assert results[0].state is RunState.PROCESSING


def test_recover_nao_deixa_waiting_runs_bloquearem_runs_do_processor(
    adapter, executor, store, clock
):
    run = _run().model_copy(update={"run_id": "z-processing"})
    adapter.put_run(run)
    plan = plan_run(PlanRequest(run=run, manifests=_full_manifests(), deployment_limit=2))
    adapter.put_run_units(PutRunUnits(
        tenant_id=_TENANT, run_id=run.run_id, expected_run_state=RunState.PROCESSING,
        units=plan.units,
    ))
    for index in range(100):
        waiting = _run(state=RunState.WAITING_INPUTS).model_copy(
            update={"run_id": f"waiting-{index:03d}"}
        )
        adapter.put_run(waiting)
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())

    results = coordinator.recover(limit=1)

    assert [result.state for result in results] == [RunState.PROCESSING]


def test_recover_isola_falha_de_um_run_e_continua_com_os_demais(
    adapter, executor, store, clock, monkeypatch, caplog
):
    failed = _run().model_copy(update={"run_id": "a-failed"})
    healthy = _run().model_copy(update={"run_id": "b-healthy"})
    adapter.put_run(failed)
    adapter.put_run(healthy)
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())
    expected = CoordinatorResult(
        state=RunState.PROCESSING, execution_ref=None, published=False
    )

    def resume(tenant_id: str, run_id: str) -> CoordinatorResult:
        if run_id == failed.run_id:
            raise ValueError("missing_output")
        return expected

    monkeypatch.setattr(coordinator, "resume", resume)

    with caplog.at_level("ERROR"):
        results = coordinator.recover(limit=2)

    assert results == (expected,)
    assert "recover_run_error tenant_id=354130 run_id=a-failed" in caplog.text


def test_recover_reinicia_dispatch_sem_referencia_do_pool_anterior(
    adapter, executor, store, clock
):
    _seed(adapter, manifests=_full_manifests())
    first_coordinator = PipelineCoordinator(
        _dependencies(adapter, executor, store, clock), _execution()
    )
    first_coordinator.resume(_TENANT, _RUN_ID)
    restarted_executor = _RestartedExecutor()
    restarted_coordinator = PipelineCoordinator(
        _dependencies(adapter, restarted_executor, store, clock), _execution()
    )

    results = restarted_coordinator.recover(limit=1)

    assert [result.state for result in results] == [RunState.PROCESSING]
    assert len(restarted_executor.started) == 1


def test_resume_publishing_retoma_cas_sem_re_transicionar(adapter, executor, store, clock):
    run = _seed(adapter, manifests=_full_manifests())
    for _ in range(3):
        coordinator = PipelineCoordinator(
            _dependencies(adapter, executor, store, clock), _execution()
        )
        coordinator.resume(_TENANT, _RUN_ID)
        _complete_dispatch(adapter, executor, store, clock)
    publishing = run.model_copy(update={"state": RunState.PUBLISHING})
    adapter.put_run(publishing)
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())

    result = coordinator.resume(_TENANT, _RUN_ID)

    assert result.state is RunState.PUBLISHED
    assert result.published is True
    assert adapter.get_run(_TENANT, _RUN_ID).state is RunState.PUBLISHED


def test_publicacao_resolve_pointer_existente_para_segundo_run(adapter, executor, store, clock):
    _seed(adapter, manifests=_full_manifests())
    coordinator = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())
    for _ in range(3):
        coordinator.resume(_TENANT, _RUN_ID)
        _complete_dispatch(adapter, executor, store, clock)
    first = coordinator.resume(_TENANT, _RUN_ID)
    assert first.state is RunState.PUBLISHED

    other_run = _run().model_copy(update={"run_id": "run-b"})
    adapter.put_run(other_run)
    plan = plan_run(PlanRequest(run=other_run, manifests=_full_manifests(), deployment_limit=2))
    adapter.put_run_units(PutRunUnits(
        tenant_id=_TENANT, run_id="run-b", expected_run_state=RunState.PROCESSING,
        units=plan.units,
    ))
    for _ in range(3):
        coordinator.resume(_TENANT, "run-b")
        _advance_other_run(adapter, executor, store, clock)

    second = coordinator.resume(_TENANT, "run-b")

    assert second.state is RunState.PUBLISHED
    pointer = adapter.get_dataset_pointer(_TENANT, "cnes")
    assert pointer.version_id == "run-b"


def _advance_other_run(adapter, executor, store, clock) -> None:
    dispatch = adapter.get_active_run_dispatch(_TENANT, "run-b")
    for unit_id in dispatch.unit_ids:
        claim = ClaimRunUnit(
            tenant_id=_TENANT, run_id="run-b", unit_id=unit_id, dispatch_id=dispatch.dispatch_id,
            owner="worker-a", now=clock.now(), lease_seconds=300,
        )
        worker = UnitWorker(UnitWorkerDependencies(
            control_plane=adapter, store=store, processor=_processor, clock=clock.now,
        ))
        worker.execute(claim)
    executor.set_status(dispatch.execution_ref, ExecutionStatus.SUCCEEDED)
