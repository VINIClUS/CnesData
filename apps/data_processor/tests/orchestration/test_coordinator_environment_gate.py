"""Gate de ambiente no PipelineCoordinator: admissão, adiamento e liquidação terminal."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, cast

import boto3
import pytest
from botocore.exceptions import ReadTimeoutError
from moto import mock_aws

from apps.data_processor.tests.orchestration.test_coordinator import (
    _NOW,
    _RUN_ID,
    _TENANT,
    _complete_dispatch,
    _fail_first_bind,
    _fails_always,
    _FakeExecutor,
    _FakeObjectStore,
    _full_manifests,
    _MutableClock,
    _processor,
    _seed,
    _stored_dispatch,
)
from cnes_domain.control_plane.commands import ClaimRunUnit
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState, RunState
from cnes_domain.control_plane.environment_gate import (
    AcquireUnitPermit,
    AcquisitionStatus,
    BindUnitPermit,
    ClosePromotionFence,
    PermitHolder,
    ReleaseUnitPermit,
    UnitPermit,
)
from cnes_domain.control_plane.errors import FenceRejected
from cnes_domain.orchestration.dispatch_admission import (
    ABSENCE_PROOF_GRACE_SECONDS,
    EnvironmentAdmission,
    UnitPermitLost,
)
from cnes_domain.ports.environment_gate import (
    UNGATED_ADMISSION,
    DeferralReason,
    DispatchDeferred,
    ExecutionObservation,
    ExecutionPresence,
    UnitAdmission,
)
from cnes_domain.ports.processing import ExecutionCallbacks, ExecutionPolicyConfig, ExecutionStatus
from cnes_domain.profiles import parse_profile
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from data_processor.composition import build_local_processor_runtime
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
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    _create_table,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

    from cnes_domain.control_plane.entities import Run, RunDispatch
    from cnes_domain.ports.processing import CancelRunExecution, StartRunExecution

_ENVIRONMENT = "prod"
_LEASE_SECONDS = 3600
_GRACE = timedelta(seconds=ABSENCE_PROOF_GRACE_SECONDS)
_ENDPOINT = "https://states.us-east-2.amazonaws.com"
_ORPHAN = PermitHolder(_TENANT, "run-orphan", "fedcba9876543210")
_ORPHAN_REF = f"exec-{_ORPHAN.dispatch_id}"
_NEXT = PermitHolder(_TENANT, "run-next", "00000000000000ff")


@dataclass
class _StepFunctions(_FakeExecutor):
    """Executor e probe na fronteira do Step Functions: a execução tem o nome do dispatch_id."""

    calls: list[str] = field(default_factory=list[str])
    start_error: Exception | None = None
    created_on_error: bool = False
    on_start: Callable[[], object] = lambda: None

    def start(self, request: StartRunExecution) -> str:
        self.calls.append("start")
        self.on_start()
        if self.start_error is None:
            return super().start(request)
        if self.created_on_error:
            super().start(request)
        raise self.start_error

    def cancel(self, request: CancelRunExecution) -> None:
        self.calls.append("cancel")
        super().cancel(request)
        if request.execution_ref is not None:
            self.statuses[request.execution_ref] = ExecutionStatus.CANCELED

    def status(self, execution_ref: str) -> ExecutionStatus:
        self.calls.append("status")
        return super().status(execution_ref)

    def probe(self, dispatch_id: str) -> ExecutionObservation:
        self.calls.append("probe")
        execution_ref = f"exec-{dispatch_id}"
        status = self.statuses.get(execution_ref)
        if status is None:
            return ExecutionObservation(ExecutionPresence.ABSENT, None)
        running = status is ExecutionStatus.RUNNING
        presence = ExecutionPresence.RUNNING if running else ExecutionPresence.TERMINAL
        return ExecutionObservation(presence, execution_ref)


@pytest.fixture
def clock() -> _MutableClock:
    return _MutableClock(_NOW)


@pytest.fixture
def adapter(tmp_path: Path, clock: _MutableClock) -> SQLiteControlPlane:
    control_plane = SQLiteControlPlane(tmp_path / "cp.db", clock.now)
    control_plane.initialize()
    return control_plane


@pytest.fixture
def store() -> _FakeObjectStore:
    return _FakeObjectStore()


@pytest.fixture
def executor() -> _StepFunctions:
    return _StepFunctions()


@pytest.fixture
def gate(clock: _MutableClock) -> Iterator[DynamoDBControlPlane]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        yield DynamoDBControlPlane(client, _TABLE_NAME, clock.now)


@pytest.fixture
def coordinator(
    adapter: SQLiteControlPlane, executor: _StepFunctions, store: _FakeObjectStore,
    clock: _MutableClock, gate: DynamoDBControlPlane,
) -> PipelineCoordinator:
    dependencies = CoordinatorDependencies(
        control_plane=adapter, executor=executor,
        publisher=DatasetPublisher(store=store, control_plane=adapter), clock=clock.now,
    )
    callbacks = ExecutionCallbacks(allow_execution, noop_execution_started)
    admission = EnvironmentAdmission(gate, executor, _ENVIRONMENT, _LEASE_SECONDS)
    return PipelineCoordinator(dependencies, ExecutionPolicyConfig(2, 300, callbacks, admission))


def _permit(gate: DynamoDBControlPlane) -> UnitPermit | None:
    return gate.observe_environment_gate(_ENVIRONMENT).permit


def _held(gate: DynamoDBControlPlane) -> UnitPermit:
    return cast("UnitPermit", _permit(gate))


def _active(adapter: SQLiteControlPlane) -> RunDispatch:
    return cast("RunDispatch", adapter.get_active_run_dispatch(_TENANT, _RUN_ID))


def _holder(dispatch: RunDispatch) -> PermitHolder:
    return PermitHolder(dispatch.tenant_id, dispatch.run_id, dispatch.dispatch_id)


def _hold(
    gate: DynamoDBControlPlane, clock: _MutableClock, execution_ref: str | None = None,
) -> UnitPermit:
    command = AcquireUnitPermit(_ENVIRONMENT, _ORPHAN, clock.now(), _LEASE_SECONDS)
    permit = cast("UnitPermit", gate.acquire_unit_permit(command).permit)
    if execution_ref is not None:
        bind = BindUnitPermit(_ENVIRONMENT, permit.token, execution_ref, clock.now())
        gate.bind_unit_permit(bind)
    return permit


def _next_acquire(gate: DynamoDBControlPlane, clock: _MutableClock) -> AcquisitionStatus:
    command = AcquireUnitPermit(_ENVIRONMENT, _NEXT, clock.now(), _LEASE_SECONDS)
    return gate.acquire_unit_permit(command).status


def _rival(gate: DynamoDBControlPlane, executor: _StepFunctions) -> EnvironmentAdmission:
    return EnvironmentAdmission(gate, executor, _ENVIRONMENT, _LEASE_SECONDS)


def _claim(dispatch: RunDispatch, clock: _MutableClock) -> ClaimRunUnit:
    return ClaimRunUnit(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=dispatch.unit_ids[0],
        dispatch_id=dispatch.dispatch_id, owner="worker-a", now=clock.now(), lease_seconds=300,
    )


def test_fence_fechado_nao_chama_step_functions(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator,
) -> None:
    _seed(adapter, manifests=_full_manifests())
    assert gate.close_promotion_fence(ClosePromotionFence(_ENVIRONMENT, "release-1", _NOW))

    result = coordinator.resume(_TENANT, _RUN_ID)

    assert result == CoordinatorResult(
        state=RunState.PROCESSING, execution_ref=None, published=False,
        deferred=DeferralReason.FENCE_CLOSED,
    )
    assert executor.calls == []
    assert _active(adapter).state is DispatchState.RESERVED
    assert _permit(gate) is None


def test_erro_ambiguo_faz_probe_antes_de_liberar(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock,
) -> None:
    _seed(adapter, manifests=_full_manifests())
    executor.start_error = ReadTimeoutError(endpoint_url=_ENDPOINT)

    with pytest.raises(ReadTimeoutError):
        coordinator.resume(_TENANT, _RUN_ID)

    assert executor.calls == ["start", "probe"]
    assert _held(gate).holder == _holder(_active(adapter))
    clock.advance(_GRACE - timedelta(seconds=1))
    deferred = _rival(gate, executor).admit(_NEXT, clock.now())
    assert deferred == DispatchDeferred(DeferralReason.PERMIT_HELD)
    clock.advance(timedelta(seconds=1))
    assert _rival(gate, executor).admit(_NEXT, clock.now()) == UnitAdmission(_ENVIRONMENT, 2)


def test_erro_ambiguo_com_execucao_criada_vincula_e_retoma_a_mesma_execucao(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator,
) -> None:
    _seed(adapter, manifests=_full_manifests())
    executor.start_error = ReadTimeoutError(endpoint_url=_ENDPOINT)
    executor.created_on_error = True

    with pytest.raises(ReadTimeoutError):
        coordinator.resume(_TENANT, _RUN_ID)

    dispatch = _active(adapter)
    reference = f"exec-{dispatch.dispatch_id}"
    assert (_held(gate).holder, _held(gate).execution_ref) == (_holder(dispatch), reference)
    executor.start_error = None

    retry = coordinator.resume(_TENANT, _RUN_ID)

    assert retry.execution_ref == reference
    assert [request.dispatch_id for request in executor.started] == [dispatch.dispatch_id] * 2
    assert _active(adapter).state is DispatchState.STARTED
    assert _held(gate).token == 1


@pytest.mark.parametrize(
    ("statuses", "elapsed"),
    [({}, _GRACE), ({_ORPHAN_REF: ExecutionStatus.SUCCEEDED}, timedelta(0))],
    ids=["ausencia-provada", "execucao-terminal"],
)
def test_permit_orfao_e_reconciliado_no_acquire(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock,
    statuses: dict[str, ExecutionStatus], elapsed: timedelta,
) -> None:
    _seed(adapter, manifests=_full_manifests())
    _hold(gate, clock)
    executor.statuses.update(statuses)
    clock.advance(elapsed)

    result = coordinator.resume(_TENANT, _RUN_ID)

    dispatch = _active(adapter)
    assert (result.execution_ref, result.deferred) == (f"exec-{dispatch.dispatch_id}", None)
    assert executor.calls == ["probe", "start"]
    assert (_held(gate).holder, _held(gate).token) == (_holder(dispatch), 2)


def test_permit_sem_bind_de_execucao_viva_e_vinculado_e_adia(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock,
) -> None:
    _seed(adapter, manifests=_full_manifests())
    _hold(gate, clock)
    executor.statuses[_ORPHAN_REF] = ExecutionStatus.RUNNING
    clock.advance(_GRACE * 4)

    result = coordinator.resume(_TENANT, _RUN_ID)

    assert result.deferred is DeferralReason.PERMIT_HELD
    assert executor.calls == ["probe"]
    assert (_held(gate).holder, _held(gate).execution_ref) == (_ORPHAN, _ORPHAN_REF)


def test_permit_vivo_com_bind_rejeita_segundo_acquire(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock,
) -> None:
    _seed(adapter, manifests=_full_manifests())
    token = _hold(gate, clock, _ORPHAN_REF).token
    executor.statuses[_ORPHAN_REF] = ExecutionStatus.RUNNING
    clock.advance(_GRACE * 4)

    result = coordinator.resume(_TENANT, _RUN_ID)

    assert result.deferred is DeferralReason.PERMIT_HELD
    assert executor.calls == ["probe"]
    permit = _held(gate)
    assert (permit.holder, permit.execution_ref, permit.token) == (_ORPHAN, _ORPHAN_REF, token)
    assert permit.lease_until == clock.now() + timedelta(seconds=_LEASE_SECONDS)
    assert _active(adapter).state is DispatchState.RESERVED


def test_recover_devolve_adiamento_e_nao_excecao(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock,
) -> None:
    _seed(adapter, manifests=_full_manifests())
    _hold(gate, clock, _ORPHAN_REF)
    executor.statuses[_ORPHAN_REF] = ExecutionStatus.RUNNING

    results = coordinator.recover(limit=10)

    assert [(result.state, result.deferred) for result in results] == [
        (RunState.PROCESSING, DeferralReason.PERMIT_HELD),
    ]


def test_execucao_terminal_liquida_o_permit_e_admite_a_nova_geracao(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock,
) -> None:
    _seed(adapter, manifests=_full_manifests())
    first = coordinator.resume(_TENANT, _RUN_ID)
    failed = _active(adapter)
    adapter.claim_run_unit(_claim(failed, clock))
    executor.set_status(cast("str", first.execution_ref), ExecutionStatus.FAILED)

    idle = coordinator.resume(_TENANT, _RUN_ID)

    assert (idle.execution_ref, _permit(gate)) == (None, None)
    clock.advance(timedelta(seconds=301))
    retry = coordinator.resume(_TENANT, _RUN_ID)
    current = _active(adapter)
    assert current.generation == failed.generation + 1
    assert retry.execution_ref == f"exec-{current.dispatch_id}"
    assert (_held(gate).holder, _held(gate).token) == (_holder(current), 2)


def test_compensacao_do_start_libera_o_permit(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock,
) -> None:
    _seed(adapter, manifests=_full_manifests())
    _fail_first_bind(adapter)

    with pytest.raises(FenceRejected):
        coordinator.resume(_TENANT, _RUN_ID)

    assert executor.calls == ["start", "cancel", "probe"]
    assert _permit(gate) is None
    assert _next_acquire(gate, clock) is AcquisitionStatus.ACQUIRED


def test_permit_perdido_no_confirm_cancela_a_execucao_sem_tocar_o_rival(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock,
) -> None:
    _seed(adapter, manifests=_full_manifests())

    def _steal() -> None:
        lost = _held(gate)
        gate.release_unit_permit(ReleaseUnitPermit(_ENVIRONMENT, lost.token, clock.now()))
        _hold(gate, clock)

    executor.on_start = _steal

    with pytest.raises(UnitPermitLost, match="reason=unit_permit_lost"):
        coordinator.resume(_TENANT, _RUN_ID)

    canceled = _stored_dispatch(adapter)
    assert canceled.terminal_outcome is DispatchOutcome.CANCELED
    assert executor.canceled[-1].execution_ref == f"exec-{canceled.dispatch_id}"
    assert _held(gate).holder == _ORPHAN


def test_falha_final_do_run_libera_o_permit(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, store: _FakeObjectStore, clock: _MutableClock,
) -> None:
    _seed(adapter, manifests=_full_manifests())
    coordinator.resume(_TENANT, _RUN_ID)
    _complete_dispatch(adapter, executor, store, clock, processor=_fails_always, max_attempts=1)

    result = coordinator.resume(_TENANT, _RUN_ID)

    assert result.state is RunState.FAILED
    assert _permit(gate) is None
    assert _next_acquire(gate, clock) is AcquisitionStatus.ACQUIRED


@pytest.mark.parametrize("publishing", [False, True], ids=["fan-in", "retomada-publishing"])
def test_publicacao_libera_o_permit_da_ultima_onda(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, store: _FakeObjectStore, clock: _MutableClock,
    publishing: bool,
) -> None:
    run = _seed(adapter, manifests=_full_manifests())
    for _ in range(3):
        coordinator.resume(_TENANT, _RUN_ID)
        _complete_dispatch(adapter, executor, store, clock)
    if publishing:
        adapter.put_run(run.model_copy(update={"state": RunState.PUBLISHING}))

    result = coordinator.resume(_TENANT, _RUN_ID)

    assert (result.state, result.published) == (RunState.PUBLISHED, True)
    assert _permit(gate) is None
    assert _next_acquire(gate, clock) is AcquisitionStatus.ACQUIRED


def test_cancelamento_sem_execution_ref_libera_o_permit_da_execucao_encerrada(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock,
) -> None:
    _seed(adapter, manifests=_full_manifests())
    started = coordinator.resume(_TENANT, _RUN_ID)
    adapter.claim_run_unit(_claim(_active(adapter), clock))
    canceling = cast("Run", adapter.get_run(_TENANT, _RUN_ID))
    adapter.put_run(canceling.model_copy(update={"state": RunState.CANCEL_REQUESTED}))
    executor.set_status(cast("str", started.execution_ref), ExecutionStatus.FAILED)

    result = coordinator.resume(_TENANT, _RUN_ID)

    assert result.state is RunState.CANCELED
    assert executor.canceled[-1].execution_ref is None
    assert _permit(gate) is None
    assert _next_acquire(gate, clock) is AcquisitionStatus.ACQUIRED


def test_after_persist_dentro_da_execucao_mantem_o_permit_ate_a_prova(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, store: _FakeObjectStore, clock: _MutableClock,
) -> None:
    _seed(adapter, manifests=_full_manifests())
    for _ in range(2):
        coordinator.resume(_TENANT, _RUN_ID)
        _complete_dispatch(adapter, executor, store, clock)
    coordinator.resume(_TENANT, _RUN_ID)
    last = _active(adapter)
    worker = UnitWorker(
        UnitWorkerDependencies(
            control_plane=adapter, store=store, processor=_processor, clock=clock.now,
        ),
        UnitWorkerPolicy(
            after_persist=lambda unit: coordinator.resume(unit.tenant_id, unit.run_id),
        ),
    )

    worker.execute(_claim(last, clock))

    assert cast("Run", adapter.get_run(_TENANT, _RUN_ID)).state is RunState.PUBLISHED
    assert _held(gate).holder == _holder(last)
    executor.set_status(f"exec-{last.dispatch_id}", ExecutionStatus.SUCCEEDED)
    assert _rival(gate, executor).admit(_NEXT, clock.now()) == UnitAdmission(_ENVIRONMENT, 4)


def test_perfil_local_compoe_o_coordinator_sem_gate(
    tmp_path: Path, clock: _MutableClock,
) -> None:
    settings = parse_profile({"TENANT_ID": _TENANT, "DATA_DIR": str(tmp_path)})

    runtime = build_local_processor_runtime(settings, clock.now)

    assert runtime.coordinator._execution.admission is UNGATED_ADMISSION
