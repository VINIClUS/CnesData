"""Gate de ambiente no RunPlanningService: admissão, adiamento e liquidação terminal."""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import TYPE_CHECKING, cast

import boto3
import pytest
from botocore.exceptions import ReadTimeoutError
from moto import mock_aws

from apps.central_api.tests.services.test_run_planning import (
    _NOW,
    _RUN_ID,
    _TENANT,
    _fail_first_bind,
    _FakeExecutor,
    _FakeObjectStore,
    _MutableClock,
    _run,
    _seed_full_chain,
    _stored_dispatch,
)
from central_api.composition import build_local_runtime
from central_api.services.run_planning import (
    RunLaunchResult,
    RunPlanningDependencies,
    RunPlanningService,
)
from cnes_domain.billing.errors import EntitlementDenied
from cnes_domain.control_plane.commands import ClaimRunUnit
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState, RunState
from cnes_domain.control_plane.environment_gate import (
    AcquireUnitPermit,
    AcquisitionStatus,
    BindUnitPermit,
    ClosePromotionFence,
    PermitHolder,
    UnitPermit,
)
from cnes_domain.control_plane.errors import FenceRejected
from cnes_domain.orchestration.dispatch_admission import (
    ABSENCE_PROOF_GRACE_SECONDS,
    EnvironmentAdmission,
)
from cnes_domain.orchestration.source_catalog import build_source_catalog
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
from data_processor.orchestration.coordinator import allow_execution, noop_execution_started
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    _create_table,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from cnes_domain.control_plane.entities import Run, RunDispatch
    from cnes_domain.ports.processing import (
        CancelRunExecution,
        ExecutionPermit,
        ExecutionStarted,
        StartRunExecution,
    )

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

    def start(self, request: StartRunExecution) -> str:
        self.calls.append("start")
        if self.start_error is not None:
            raise self.start_error
        return super().start(request)

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
def service(
    adapter: SQLiteControlPlane, executor: _StepFunctions, store: _FakeObjectStore,
    clock: _MutableClock, gate: DynamoDBControlPlane,
) -> RunPlanningService:
    dependencies = RunPlanningDependencies(
        control_plane=adapter, object_store=store, executor=executor,
        source_catalog=build_source_catalog(),
    )
    callbacks = ExecutionCallbacks(allow_execution, noop_execution_started)
    admission = EnvironmentAdmission(gate, executor, _ENVIRONMENT, _LEASE_SECONDS)
    execution = ExecutionPolicyConfig(2, 300, callbacks, admission)
    return RunPlanningService(dependencies, execution, clock.now)


def _variant(
    service: RunPlanningService, started: ExecutionStarted, dispatch_enabled: bool,
) -> RunPlanningService:
    callbacks = ExecutionCallbacks(allow_execution, started)
    execution = replace(service._execution, callbacks=callbacks)
    return RunPlanningService(
        service._dependencies, execution, service._clock, dispatch_enabled=dispatch_enabled,
    )


def _planned(adapter: SQLiteControlPlane, store: _FakeObjectStore) -> None:
    adapter.put_run(_run())
    _seed_full_chain(adapter, store)


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


def _claim(dispatch: RunDispatch, clock: _MutableClock) -> ClaimRunUnit:
    return ClaimRunUnit(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=dispatch.unit_ids[0],
        dispatch_id=dispatch.dispatch_id, owner="worker-a", now=clock.now(), lease_seconds=300,
    )


def test_fence_fechado_nao_chama_step_functions(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService,
) -> None:
    _planned(adapter, store)
    assert gate.close_promotion_fence(ClosePromotionFence(_ENVIRONMENT, "release-1", _NOW))

    result = service.launch(_TENANT, _RUN_ID)

    assert result == RunLaunchResult(
        run=result.run, plan=result.plan, execution_ref=None,
        deferred=DeferralReason.FENCE_CLOSED,
    )
    assert result.run.state is RunState.PROCESSING
    assert executor.calls == []
    assert _active(adapter).state is DispatchState.RESERVED
    assert _permit(gate) is None


def test_erro_ambiguo_faz_probe_antes_de_liberar(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService, clock: _MutableClock,
) -> None:
    _planned(adapter, store)
    executor.start_error = ReadTimeoutError(endpoint_url=_ENDPOINT)

    with pytest.raises(ReadTimeoutError):
        service.launch(_TENANT, _RUN_ID)

    assert executor.calls == ["start", "probe"]
    assert _held(gate).holder == _holder(_active(adapter))
    rival = EnvironmentAdmission(gate, executor, _ENVIRONMENT, _LEASE_SECONDS)
    clock.advance(_GRACE - timedelta(seconds=1))
    assert rival.admit(_NEXT, clock.now()) == DispatchDeferred(DeferralReason.PERMIT_HELD)
    clock.advance(timedelta(seconds=1))
    assert rival.admit(_NEXT, clock.now()) == UnitAdmission(_ENVIRONMENT, 2)


@pytest.mark.parametrize(
    ("statuses", "elapsed"),
    [({}, _GRACE), ({_ORPHAN_REF: ExecutionStatus.CANCELED}, timedelta(0))],
    ids=["ausencia-provada", "execucao-terminal"],
)
def test_permit_orfao_e_reconciliado_no_acquire(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService, clock: _MutableClock,
    statuses: dict[str, ExecutionStatus], elapsed: timedelta,
) -> None:
    _planned(adapter, store)
    _hold(gate, clock)
    executor.statuses.update(statuses)
    clock.advance(elapsed)

    result = service.launch(_TENANT, _RUN_ID)

    dispatch = _active(adapter)
    assert (result.execution_ref, result.deferred) == (f"exec-{dispatch.dispatch_id}", None)
    assert executor.calls == ["probe", "start"]
    assert (_held(gate).holder, _held(gate).token) == (_holder(dispatch), 2)


def test_permit_vivo_com_bind_rejeita_segundo_acquire(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService, clock: _MutableClock,
) -> None:
    _planned(adapter, store)
    token = _hold(gate, clock, _ORPHAN_REF).token
    executor.statuses[_ORPHAN_REF] = ExecutionStatus.RUNNING
    clock.advance(_GRACE * 4)

    result = service.launch(_TENANT, _RUN_ID)

    assert (result.execution_ref, result.deferred) == (None, DeferralReason.PERMIT_HELD)
    assert executor.calls == ["probe"]
    permit = _held(gate)
    assert (permit.holder, permit.execution_ref, permit.token) == (_ORPHAN, _ORPHAN_REF, token)
    assert _active(adapter).state is DispatchState.RESERVED


def test_recover_devolve_adiamento_e_nao_excecao(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService, clock: _MutableClock,
) -> None:
    adapter.put_run(_run(state=RunState.WAITING_INPUTS))
    _seed_full_chain(adapter, store)
    _hold(gate, clock, _ORPHAN_REF)
    executor.statuses[_ORPHAN_REF] = ExecutionStatus.RUNNING

    results = service.recover(limit=10)

    assert [(result.run.state, result.deferred) for result in results] == [
        (RunState.PROCESSING, DeferralReason.PERMIT_HELD),
    ]


def test_execucao_terminal_liquida_o_permit_e_admite_a_nova_geracao(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService, clock: _MutableClock,
) -> None:
    _planned(adapter, store)
    first = service.launch(_TENANT, _RUN_ID)
    previous = _active(adapter)
    adapter.claim_run_unit(_claim(previous, clock))
    executor.set_status(cast("str", first.execution_ref), ExecutionStatus.SUCCEEDED)

    idle = service.launch(_TENANT, _RUN_ID)

    assert (idle.execution_ref, _permit(gate)) == (None, None)
    clock.advance(timedelta(seconds=301))
    retry = service.launch(_TENANT, _RUN_ID)
    current = _active(adapter)
    assert current.generation == previous.generation + 1
    assert retry.execution_ref == f"exec-{current.dispatch_id}"
    assert (_held(gate).holder, _held(gate).token) == (_holder(current), 2)


def test_billing_negado_nao_adquire_o_permit(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService,
) -> None:
    _planned(adapter, store)

    def _deny(run: Run, dispatch: RunDispatch, requested_limit: int) -> ExecutionPermit:
        raise EntitlementDenied("reason=run_cancel_requested")

    callbacks = ExecutionCallbacks(_deny, noop_execution_started)
    denied = RunPlanningService(
        service._dependencies, replace(service._execution, callbacks=callbacks), service._clock,
    )

    with pytest.raises(EntitlementDenied, match="reason=run_cancel_requested"):
        denied.launch(_TENANT, _RUN_ID)

    assert executor.calls == []
    assert _permit(gate) is None


def test_compensacao_do_bind_libera_o_permit(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService, clock: _MutableClock,
) -> None:
    _planned(adapter, store)
    _fail_first_bind(adapter)

    with pytest.raises(FenceRejected):
        service.launch(_TENANT, _RUN_ID)

    assert executor.calls == ["start", "cancel", "probe"]
    assert _permit(gate) is None
    assert _next_acquire(gate, clock) is AcquisitionStatus.ACQUIRED


def test_compensacao_do_callback_started_libera_o_permit_vinculado(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService, clock: _MutableClock,
) -> None:
    _planned(adapter, store)
    bound: list[str | None] = []

    def _started(
        run: Run, request: StartRunExecution, execution_ref: str, permit: ExecutionPermit,
    ) -> None:
        bound.append(_held(gate).execution_ref)
        raise RuntimeError("callback=down")

    with pytest.raises(RuntimeError, match="callback=down"):
        _variant(service, _started, dispatch_enabled=True).launch(_TENANT, _RUN_ID)

    canceled = _stored_dispatch(adapter)
    assert canceled.terminal_outcome is DispatchOutcome.CANCELED
    assert bound == [f"exec-{canceled.dispatch_id}"]
    assert _permit(gate) is None
    assert _next_acquire(gate, clock) is AcquisitionStatus.ACQUIRED


def test_cancelamento_sem_execution_ref_libera_o_permit_da_execucao_encerrada(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService, clock: _MutableClock,
) -> None:
    _planned(adapter, store)
    started = service.launch(_TENANT, _RUN_ID)
    adapter.claim_run_unit(_claim(_active(adapter), clock))
    canceling = cast("Run", adapter.get_run(_TENANT, _RUN_ID))
    adapter.put_run(canceling.model_copy(update={"state": RunState.CANCEL_REQUESTED}))
    executor.set_status(cast("str", started.execution_ref), ExecutionStatus.FAILED)

    result = service.launch(_TENANT, _RUN_ID)

    assert result.run.state is RunState.CANCELED
    assert executor.canceled[-1].execution_ref is None
    assert _permit(gate) is None
    assert _next_acquire(gate, clock) is AcquisitionStatus.ACQUIRED


def test_cancelamento_mantem_o_permit_da_execucao_ainda_viva(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService, clock: _MutableClock,
) -> None:
    _planned(adapter, store)
    service.launch(_TENANT, _RUN_ID)
    dispatch = _active(adapter)
    adapter.claim_run_unit(_claim(dispatch, clock))
    canceling = cast("Run", adapter.get_run(_TENANT, _RUN_ID))
    adapter.put_run(canceling.model_copy(update={"state": RunState.CANCEL_REQUESTED}))

    result = service.launch(_TENANT, _RUN_ID)

    assert result.run.state is RunState.CANCELED
    assert _held(gate).holder == _holder(dispatch)
    assert _next_acquire(gate, clock) is AcquisitionStatus.HELD


def test_dispatch_desabilitado_nao_consulta_o_gate(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService,
) -> None:
    _planned(adapter, store)
    assert gate.close_promotion_fence(ClosePromotionFence(_ENVIRONMENT, "release-1", _NOW))
    plan_only = _variant(service, noop_execution_started, dispatch_enabled=False)

    launched = plan_only.launch(_TENANT, _RUN_ID)
    canceling = cast("Run", adapter.get_run(_TENANT, _RUN_ID))
    adapter.put_run(canceling.model_copy(update={"state": RunState.CANCEL_REQUESTED}))
    canceled = plan_only.launch(_TENANT, _RUN_ID)

    assert (launched.run.state, launched.execution_ref, launched.deferred) == (
        RunState.PROCESSING, None, None,
    )
    assert canceled.run.state is RunState.CANCELED
    assert executor.calls == []
    assert _permit(gate) is None


def test_perfil_local_compoe_o_planejamento_sem_gate(
    tmp_path: Path, clock: _MutableClock,
) -> None:
    settings = parse_profile({"TENANT_ID": _TENANT, "DATA_DIR": str(tmp_path)})

    runtime = build_local_runtime(settings, clock.now)

    assert runtime.run_planning._execution.admission is UNGATED_ADMISSION
