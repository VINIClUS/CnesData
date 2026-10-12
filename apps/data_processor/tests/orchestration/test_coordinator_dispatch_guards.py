"""Guardas do dispatch no PipelineCoordinator: policy, reentrada, reconcile e perfil sem gate."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, cast

import pytest
from botocore.exceptions import ReadTimeoutError

from apps.data_processor.tests.orchestration.test_coordinator import (
    _RUN_ID,
    _TENANT,
    _complete_dispatch,
    _dependencies,
    _execution,
    _full_manifests,
    _processor,
    _seed,
)
from apps.data_processor.tests.orchestration.test_coordinator_environment_gate import (
    _ENDPOINT,
    _ENVIRONMENT,
    _GRACE,
    _NEXT,
    _active,
    _claim,
    _held,
    _hold,
    _holder,
    _permit,
    _rival,
    adapter,
    clock,
    coordinator,
    executor,
    gate,
    store,
)
from cnes_domain.billing.errors import EntitlementDenied
from cnes_domain.control_plane.commands import FinalizeRunCancellation, FinishRunDispatch
from cnes_domain.control_plane.entities import OutboxEvent
from cnes_domain.control_plane.enums import DispatchOutcome, RunState, RunUnitState
from cnes_domain.control_plane.environment_gate import ClosePromotionFence
from cnes_domain.ports.environment_gate import DeferralReason, DispatchDeferred, UnitAdmission
from cnes_domain.ports.processing import (
    CancelRunExecution,
    ExecutionCallbacks,
    ExecutionPermit,
    ExecutionStatus,
)
from cnes_infra.executor.step_functions import ProcessorExecutionUnavailable
from data_processor.orchestration.coordinator import (
    PipelineCoordinator,
    RecoveryFailed,
    noop_execution_started,
)
from data_processor.orchestration.unit_worker import (
    UnitWorker,
    UnitWorkerDependencies,
    UnitWorkerPolicy,
)

if TYPE_CHECKING:
    from datetime import datetime

    from apps.data_processor.tests.orchestration.test_coordinator import (
        _FakeObjectStore,
        _MutableClock,
    )
    from apps.data_processor.tests.orchestration.test_coordinator_environment_gate import (
        _StepFunctions,
    )
    from cnes_domain.control_plane.entities import Run, RunDispatch, RunUnit
    from cnes_domain.control_plane.environment_gate import PermitHolder
    from cnes_domain.ports.environment_gate import ExecutionObservation
    from cnes_domain.ports.processing import ConcurrencyPolicy
    from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
    from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane

__all__ = ["adapter", "clock", "coordinator", "executor", "gate", "store"]
_PERMIT_HELD = DispatchDeferred(DeferralReason.PERMIT_HELD)
_FENCE_DEFERRED = [(RunState.PROCESSING, None, DeferralReason.FENCE_CLOSED)]


@dataclass
class _Timeline:
    """`DispatchAdmissionPort` que anota cada chamada na linha do tempo da passada."""

    events: list[str]

    def admit(self, holder: PermitHolder, now: datetime) -> UnitAdmission | DispatchDeferred:
        self.events.append("admit")
        return UnitAdmission(None, None)

    def confirm(self, admission: UnitAdmission, execution_ref: str, now: datetime) -> None:
        self.events.append("confirm")

    def settle(self, holder: PermitHolder, now: datetime) -> None:
        self.events.append("settle")

    def reconcile(self, now: datetime) -> None:
        self.events.append("reconcile")


def _deny(run: Run, dispatch: RunDispatch, requested_limit: int) -> ExecutionPermit:
    raise EntitlementDenied("reason=run_cancel_requested")


def _foreign(run: Run, dispatch: RunDispatch, requested_limit: int) -> ExecutionPermit:
    return ExecutionPermit(
        tenant_id=run.tenant_id, run_id="run-foreign", max_concurrency=requested_limit,
        policy_version=0, fencing_token=0,
    )


def _throttled(dispatch_id: str) -> ExecutionObservation:
    raise ProcessorExecutionUnavailable("ThrottlingException")


def _production(coordinator: PipelineCoordinator) -> PipelineCoordinator:
    execution = replace(coordinator._execution, dispatch_lease_seconds=3600)
    return PipelineCoordinator(coordinator._dependencies, execution)


def _ambiguous_start(
    adapter: SQLiteControlPlane, executor: _StepFunctions, production: PipelineCoordinator,
) -> RunDispatch:
    _seed(adapter, manifests=_full_manifests())
    executor.start_error = ReadTimeoutError(endpoint_url=_ENDPOINT)
    with pytest.raises(ReadTimeoutError):
        production.resume(_TENANT, _RUN_ID)
    executor.start_error = None
    return _active(adapter)


def _close_fence(gate: DynamoDBControlPlane, clock: _MutableClock) -> None:
    assert gate.close_promotion_fence(ClosePromotionFence(_ENVIRONMENT, "release-3", clock.now()))


def _listing_into(
    adapter: SQLiteControlPlane, events: list[str], monkeypatch: pytest.MonkeyPatch,
) -> None:
    list_recoverable_runs = adapter.list_recoverable_runs

    def _list(now: datetime, limit: int) -> tuple[Run, ...]:
        events.append("list")
        return list_recoverable_runs(now, limit)

    monkeypatch.setattr(adapter, "list_recoverable_runs", _list)


def _with_policy(
    coordinator: PipelineCoordinator, policy: ConcurrencyPolicy,
) -> PipelineCoordinator:
    callbacks = ExecutionCallbacks(policy, noop_execution_started)
    return PipelineCoordinator(
        coordinator._dependencies, replace(coordinator._execution, callbacks=callbacks),
    )


def _finalize_billing_revocation(
    adapter: SQLiteControlPlane, executor: _StepFunctions, dispatch: RunDispatch,
    clock: _MutableClock,
) -> None:
    now = clock.now()
    run = cast("Run", adapter.get_run(_TENANT, _RUN_ID))
    adapter.put_run(run.model_copy(update={"state": RunState.CANCEL_REQUESTED}))
    executor.cancel(CancelRunExecution(
        tenant_id=_TENANT, run_id=_RUN_ID, execution_ref=dispatch.execution_ref,
    ))
    adapter.finish_run_dispatch(FinishRunDispatch(
        tenant_id=_TENANT, run_id=_RUN_ID, dispatch_id=dispatch.dispatch_id,
        outcome=DispatchOutcome.CANCELED, finished_at=now,
    ))
    adapter.finalize_run_cancellation(FinalizeRunCancellation(
        tenant_id=_TENANT, run_id=_RUN_ID, expected_state=RunState.CANCEL_REQUESTED,
        canceled_at=now,
    ), OutboxEvent(
        tenant_id=_TENANT, event_id=f"run.canceled:{_TENANT}:{_RUN_ID}",
        event_type="run.canceled", aggregate_id=_RUN_ID, payload={"dataset_name": "cnes"},
        created_at=now, delivered_at=None,
    ))


def _reads_failing_after_the_first(
    adapter: SQLiteControlPlane, monkeypatch: pytest.MonkeyPatch,
) -> list[str]:
    reads: list[str] = []
    list_run_units = adapter.list_run_units

    def _list(tenant_id: str, run_id: str) -> tuple[RunUnit, ...]:
        reads.append(run_id)
        if len(reads) > 1:
            raise TimeoutError("list_run_units=throttled")
        return list_run_units(tenant_id, run_id)

    monkeypatch.setattr(adapter, "list_run_units", _list)
    return reads


@pytest.fixture
def last_wave(
    adapter: SQLiteControlPlane, executor: _StepFunctions, store: _FakeObjectStore,
    clock: _MutableClock, coordinator: PipelineCoordinator,
) -> RunDispatch:
    _seed(adapter, manifests=_full_manifests())
    for _ in range(2):
        coordinator.resume(_TENANT, _RUN_ID)
        _complete_dispatch(adapter, executor, store, clock)
    coordinator.resume(_TENANT, _RUN_ID)
    return _active(adapter)


@pytest.fixture
def worker(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, clock: _MutableClock,
    coordinator: PipelineCoordinator,
) -> UnitWorker:
    return UnitWorker(
        UnitWorkerDependencies(
            control_plane=adapter, store=store, processor=_processor, clock=clock.now,
        ),
        UnitWorkerPolicy(after_persist=lambda unit: coordinator.resume(
            unit.tenant_id, unit.run_id, settle=False,
        )),
    )


def test_billing_negado_nao_adquire_o_permit(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator,
) -> None:
    _seed(adapter, manifests=_full_manifests())

    with pytest.raises(EntitlementDenied, match="reason=run_cancel_requested"):
        _with_policy(coordinator, _deny).resume(_TENANT, _RUN_ID)

    assert executor.calls == []
    assert _permit(gate) is None


def test_identidade_divergente_nao_adquire_o_permit(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator,
) -> None:
    _seed(adapter, manifests=_full_manifests())

    with pytest.raises(ValueError, match="execution_permit_identity_mismatch"):
        _with_policy(coordinator, _foreign).resume(_TENANT, _RUN_ID)

    assert executor.calls == []
    assert _permit(gate) is None


def test_erro_ambiguo_na_reentrada_tardia_nao_libera(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock,
) -> None:
    production = _production(coordinator)
    dispatch = _ambiguous_start(adapter, executor, production)
    executor.start_error = ReadTimeoutError(endpoint_url=_ENDPOINT)
    clock.advance(timedelta(minutes=30))

    with pytest.raises(ReadTimeoutError):
        production.resume(_TENANT, _RUN_ID)

    assert _active(adapter).dispatch_id == dispatch.dispatch_id
    assert executor.calls == ["start", "probe", "start", "probe"]
    assert (_held(gate).holder, _held(gate).acquired_at) == (_holder(dispatch), clock.now())
    executor.statuses[f"exec-{dispatch.dispatch_id}"] = ExecutionStatus.RUNNING
    assert _rival(gate, executor).admit(_NEXT, clock.now()) == _PERMIT_HELD


def test_reentrada_sem_bind_com_fence_fechado_nao_inicia_execucao_nova(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock,
) -> None:
    production = _production(coordinator)
    dispatch = _ambiguous_start(adapter, executor, production)
    anchor = _held(gate).acquired_at
    clock.advance(timedelta(minutes=5))
    _close_fence(gate, clock)
    clock.advance(timedelta(minutes=1))

    results = production.recover(limit=10)

    assert [(result.state, result.execution_ref, result.deferred) for result in results] == (
        _FENCE_DEFERRED
    )
    assert (executor.calls.count("start"), executor.statuses) == (1, {})
    assert (_held(gate).holder, _held(gate).acquired_at) == (_holder(dispatch), anchor)
    clock.advance(anchor + _GRACE - clock.now())
    assert [result.deferred for result in production.recover(limit=10)] == [
        DeferralReason.FENCE_CLOSED,
    ]
    assert _permit(gate) is None


def test_reentrada_sem_bind_com_execucao_viva_e_fence_fechado_retoma_a_mesma_execucao(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock,
) -> None:
    production = _production(coordinator)
    dispatch = _ambiguous_start(adapter, executor, production)
    reference = f"exec-{dispatch.dispatch_id}"
    executor.statuses[reference] = ExecutionStatus.RUNNING
    _close_fence(gate, clock)
    clock.advance(timedelta(minutes=1))

    result = production.resume(_TENANT, _RUN_ID)

    assert (result.execution_ref, result.deferred) == (reference, None)
    assert executor.calls == ["start", "probe", "probe", "start"]
    assert executor.statuses == {reference: ExecutionStatus.RUNNING}
    assert (_held(gate).holder, _held(gate).execution_ref) == (_holder(dispatch), reference)


def test_reconcile_abre_a_passada_antes_de_listar_os_runs(
    adapter: SQLiteControlPlane, executor: _StepFunctions, store: _FakeObjectStore,
    clock: _MutableClock, monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _listing_into(adapter, events, monkeypatch)
    _seed(adapter, manifests=_full_manifests())
    execution = replace(_execution(), admission=_Timeline(events))
    timeline = PipelineCoordinator(_dependencies(adapter, executor, store, clock), execution)

    timeline.recover(limit=10)

    assert events == ["reconcile", "list", "admit", "confirm", "list"]


def test_recover_libera_o_permit_da_execucao_publicada_com_fence_fechado(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock, last_wave: RunDispatch,
    worker: UnitWorker,
) -> None:
    worker.execute(_claim(last_wave, clock))
    assert cast("Run", adapter.get_run(_TENANT, _RUN_ID)).state is RunState.PUBLISHED
    executor.set_status(f"exec-{last_wave.dispatch_id}", ExecutionStatus.SUCCEEDED)
    assert gate.close_promotion_fence(ClosePromotionFence(_ENVIRONMENT, "release-2", clock.now()))
    clock.advance(timedelta(minutes=30))
    probes = executor.calls.count("probe")

    assert coordinator.recover(limit=10) == ()

    assert _permit(gate) is None
    assert executor.calls.count("probe") == probes + 1


def test_recover_libera_o_permit_apos_o_finalizador_de_revogacao_do_billing(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock,
) -> None:
    _seed(adapter, manifests=_full_manifests())
    coordinator.resume(_TENANT, _RUN_ID)
    dispatch = _active(adapter)
    _finalize_billing_revocation(adapter, executor, dispatch, clock)
    clock.advance(timedelta(hours=2))
    assert coordinator.resume(_TENANT, _RUN_ID).state is RunState.CANCELED
    assert _held(gate).holder == _holder(dispatch)

    assert coordinator.recover(limit=10) == ()

    assert _permit(gate) is None
    assert executor.calls.count("start") == 1


def test_throttling_no_probe_nao_derruba_o_after_persist(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    clock: _MutableClock, last_wave: RunDispatch, worker: UnitWorker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(executor, "probe", _throttled)

    committed = worker.execute(_claim(last_wave, clock))

    assert committed.state is RunUnitState.SUCCEEDED
    assert cast("Run", adapter.get_run(_TENANT, _RUN_ID)).state is RunState.PUBLISHED
    assert _held(gate).holder == _holder(last_wave)


def test_erro_do_reconcile_nao_impede_a_recuperacao_e_falha_a_passada(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock, monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _seed(adapter, manifests=_full_manifests())
    adapter.put_run(run.model_copy(update={"state": RunState.CANCEL_REQUESTED}))
    orphan = _hold(gate, clock)
    monkeypatch.setattr(executor, "probe", _throttled)

    with pytest.raises(RecoveryFailed, match=r"^failed=0 recovered=1$") as failure:
        coordinator.recover(limit=10)

    assert isinstance(failure.value.__cause__, ProcessorExecutionUnavailable)
    assert [result.state for result in failure.value.results] == [RunState.CANCELED]
    assert cast("Run", adapter.get_run(_TENANT, _RUN_ID)).state is RunState.CANCELED
    assert _permit(gate) == orphan


def test_run_falho_prevalece_sobre_o_erro_do_reconcile(
    adapter: SQLiteControlPlane, executor: _StepFunctions, gate: DynamoDBControlPlane,
    coordinator: PipelineCoordinator, clock: _MutableClock, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed(adapter, manifests=_full_manifests())
    orphan = _hold(gate, clock)
    monkeypatch.setattr(executor, "probe", _throttled)

    with pytest.raises(RecoveryFailed, match=r"^failed=1 recovered=0$") as failure:
        coordinator.recover(limit=10)

    assert isinstance(failure.value.__cause__, ProcessorExecutionUnavailable)
    assert executor.calls == []
    assert _permit(gate) == orphan


def test_perfil_sem_gate_publica_com_uma_unica_leitura_das_unidades(
    adapter: SQLiteControlPlane, executor: _StepFunctions, store: _FakeObjectStore,
    clock: _MutableClock, monkeypatch: pytest.MonkeyPatch,
) -> None:
    ungated = PipelineCoordinator(_dependencies(adapter, executor, store, clock), _execution())
    _seed(adapter, manifests=_full_manifests())
    for _ in range(3):
        ungated.resume(_TENANT, _RUN_ID)
        _complete_dispatch(adapter, executor, store, clock)
    reads = _reads_failing_after_the_first(adapter, monkeypatch)

    result = ungated.resume(_TENANT, _RUN_ID)

    assert (result.state, result.published) == (RunState.PUBLISHED, True)
    assert reads == [_RUN_ID]
