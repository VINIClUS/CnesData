"""Reserve -> start -> bind dispatch protocol; fan-in driven Run state machine."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from cnes_domain.control_plane.commands import (
    BindRunDispatch,
    FinalizeRunCancellation,
    FinishRunDispatch,
    ReserveRunDispatch,
    TransitionRun,
)
from cnes_domain.control_plane.entities import OutboxEvent
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState, RunState
from cnes_domain.orchestration.dispatch_admission import dispatch_holder, settle_run_dispatches
from cnes_domain.orchestration.fan_in import decide_fan_in
from cnes_domain.orchestration.planner import (
    RunPlan,
    execution_request,
    logical_wave_id,
    ready_units,
)
from cnes_domain.ports.environment_gate import UNGATED_ADMISSION, DispatchDeferred
from cnes_domain.ports.processing import CancelRunExecution, ExecutionPermit, ExecutionStatus
from data_processor.orchestration.publisher import PublishRequest

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_domain.control_plane.entities import Run, RunDispatch, RunUnit
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.environment_gate import DeferralReason
    from cnes_domain.ports.processing import (
        ExecutionPolicyConfig,
        ProcessorExecutorPort,
        StartRunExecution,
    )
    from data_processor.orchestration.publisher import DatasetPublisher

_TERMINAL_RUN_STATES = frozenset({
    RunState.PUBLISHED, RunState.PUBLISHED_DEGRADED, RunState.FAILED, RunState.CANCELED,
})
_RECOVERABLE_RUN_STATES = frozenset({
    RunState.PROCESSING, RunState.PUBLISHING, RunState.CANCEL_REQUESTED,
})
_STATUS_OUTCOME = {
    ExecutionStatus.SUCCEEDED: DispatchOutcome.SUCCEEDED,
    ExecutionStatus.FAILED: DispatchOutcome.FAILED,
    ExecutionStatus.CANCELED: DispatchOutcome.CANCELED,
}
logger = logging.getLogger(__name__)


def _processor_recoverable_runs(
    control_plane: ControlPlanePort,
    now: datetime,
    limit: int,
    skipped: set[tuple[str, str]],
) -> tuple[Run, ...]:
    if limit <= 0:
        return ()
    query_limit = limit
    while True:
        candidates = control_plane.list_recoverable_runs(now, query_limit)
        selected = tuple(
            run for run in candidates
            if run.state in _RECOVERABLE_RUN_STATES
            and (run.tenant_id, run.run_id) not in skipped
        )
        if len(selected) >= limit or len(candidates) < query_limit:
            return selected[:limit]
        query_limit *= 2


def allow_execution(run: Run, dispatch: RunDispatch, requested_limit: int) -> ExecutionPermit:
    del dispatch
    return ExecutionPermit(
        tenant_id=run.tenant_id, run_id=run.run_id, max_concurrency=requested_limit,
        policy_version=0, fencing_token=0, binding_context=None,
    )


def noop_execution_started(
    run: Run, request: StartRunExecution, execution_ref: str, permit: ExecutionPermit
) -> None:
    del run, request, execution_ref, permit


@dataclass(frozen=True, slots=True)
class CoordinatorDependencies:
    control_plane: ControlPlanePort
    executor: ProcessorExecutorPort
    publisher: DatasetPublisher
    clock: Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class CoordinatorResult:
    state: RunState
    execution_ref: str | None
    published: bool
    deferred: DeferralReason | None = None


class RecoveryFailed(RuntimeError):
    """Passada de recovery que falhou em runs ou no reconcile (a causa); carrega os resultados."""

    def __init__(
        self, results: tuple[CoordinatorResult, ...], failed: int, *,
        reconcile_failed: bool = False,
    ) -> None:
        super().__init__(f"failed={failed} recovered={len(results)}")
        self.results = results
        self.failed = failed
        self.reconcile_failed = reconcile_failed


def _build_event(run: Run, event_type: str, now: datetime) -> OutboxEvent:
    return OutboxEvent(
        tenant_id=run.tenant_id, event_id=f"{event_type}:{run.tenant_id}:{run.run_id}",
        event_type=event_type, aggregate_id=run.run_id,
        payload={"dataset_name": run.dataset_name}, created_at=now, delivered_at=None,
    )


def _reserve_next_wave(
    control_plane: ControlPlanePort, plan: RunPlan, execution: ExecutionPolicyConfig, now: datetime
) -> RunDispatch | None:
    ready = ready_units(plan, now)
    if not ready:
        return None
    wave_id = logical_wave_id(ready)
    unit_ids = tuple(sorted(unit.unit_id for unit in ready))
    return control_plane.reserve_run_dispatch(ReserveRunDispatch(
        tenant_id=plan.run.tenant_id, run_id=plan.run.run_id, wave_id=wave_id,
        unit_ids=unit_ids, now=now, lease_seconds=execution.dispatch_lease_seconds,
    ))


def _settle_started(
    control_plane: ControlPlanePort, executor: ProcessorExecutorPort,
    plan: RunPlan, dispatch: RunDispatch, now: datetime,
) -> RunDispatch | None:
    try:
        status = executor.status(cast("str", dispatch.execution_ref))
    except ValueError as error:
        if "execution_ref=unknown" not in str(error):
            raise
        logger.warning(
            "execution_lost tenant_id=%s run_id=%s execution_ref=%s",
            plan.run.tenant_id,
            plan.run.run_id,
            dispatch.execution_ref,
        )
        status = ExecutionStatus.FAILED
    if status is ExecutionStatus.RUNNING:
        return dispatch
    control_plane.finish_run_dispatch(FinishRunDispatch(
        tenant_id=plan.run.tenant_id, run_id=plan.run.run_id, dispatch_id=dispatch.dispatch_id,
        outcome=_STATUS_OUTCOME[status], finished_at=now,
    ))
    return None


def _compensate_start(
    control_plane: ControlPlanePort, executor: ProcessorExecutorPort,
    finish: FinishRunDispatch, execution_ref: str,
) -> None:
    # An execution whose bind or started callback failed must not outlive its dispatch:
    # finishing it CANCELED makes the next resume reserve generation+1 instead of a replay.
    logger.warning(
        "dispatch_start_compensated tenant_id=%s run_id=%s dispatch_id=%s",
        finish.tenant_id, finish.run_id, finish.dispatch_id,
    )
    executor.cancel(CancelRunExecution(
        tenant_id=finish.tenant_id, run_id=finish.run_id, execution_ref=execution_ref,
    ))
    control_plane.finish_run_dispatch(finish)


def _start_and_bind(
    control_plane: ControlPlanePort, executor: ProcessorExecutorPort,
    execution: ExecutionPolicyConfig, plan: RunPlan, dispatch: RunDispatch, now: datetime,
) -> str | DispatchDeferred:
    run = plan.run
    requested_limit = min(len(dispatch.unit_ids), execution.deployment_limit)
    permit = execution.callbacks.policy(run, dispatch, requested_limit)
    if permit.tenant_id != run.tenant_id or permit.run_id != run.run_id:
        raise ValueError("execution_permit_identity_mismatch")
    request = execution_request(plan, dispatch, permit.max_concurrency)
    holder = dispatch_holder(dispatch)
    admission = execution.admission.admit(holder, now)
    if isinstance(admission, DispatchDeferred):
        return admission
    try:
        execution_ref = executor.start(request)
    except Exception:
        execution.admission.settle(holder, now)
        raise
    try:
        bound = control_plane.bind_run_dispatch(BindRunDispatch(
            tenant_id=run.tenant_id, run_id=run.run_id, dispatch_id=dispatch.dispatch_id,
            execution_ref=execution_ref, now=now, lease_seconds=execution.dispatch_lease_seconds,
        ))
        execution.admission.confirm(admission, execution_ref, now)
        execution.callbacks.started(run, request, execution_ref, permit)
    except Exception:
        _compensate_start(control_plane, executor, FinishRunDispatch(
            tenant_id=run.tenant_id, run_id=run.run_id, dispatch_id=dispatch.dispatch_id,
            outcome=DispatchOutcome.CANCELED, finished_at=now,
        ), execution_ref)
        execution.admission.settle(holder, now)
        raise
    return cast("str", bound.execution_ref)


def _dispatch_protocol(
    control_plane: ControlPlanePort, executor: ProcessorExecutorPort,
    execution: ExecutionPolicyConfig, plan: RunPlan, now: datetime,
) -> str | DispatchDeferred | None:
    active = control_plane.get_active_run_dispatch(plan.run.tenant_id, plan.run.run_id)
    if active is not None and active.state is DispatchState.STARTED:
        if _settle_started(control_plane, executor, plan, active, now) is not None:
            return active.execution_ref
        execution.admission.settle(dispatch_holder(active), now)
        active = None
    if active is None:
        active = _reserve_next_wave(control_plane, plan, execution, now)
        if active is None:
            return None
    return _start_and_bind(control_plane, executor, execution, plan, active, now)


def _fail_run(
    control_plane: ControlPlanePort, run: Run, missing: tuple[str, ...], now: datetime
) -> Run:
    event = _build_event(run, "run.failed", now)
    return control_plane.transition_run(TransitionRun(
        tenant_id=run.tenant_id, run_id=run.run_id, expected_state=RunState.PROCESSING,
        new_state=RunState.FAILED, missing_sources=missing,
    ), event)


def _transition_to_publishing(
    control_plane: ControlPlanePort, run: Run, missing: tuple[str, ...], now: datetime
) -> Run:
    event = _build_event(run, "run.publishing", now)
    return control_plane.transition_run(TransitionRun(
        tenant_id=run.tenant_id, run_id=run.run_id, expected_state=RunState.PROCESSING,
        new_state=RunState.PUBLISHING, missing_sources=missing,
    ), event)


def _publish_now(
    dependencies: CoordinatorDependencies, run: Run, units: tuple[RunUnit, ...], now: datetime
) -> CoordinatorResult:
    control_plane = dependencies.control_plane
    pointer = control_plane.get_dataset_pointer(run.tenant_id, run.dataset_name)
    expected_version_id = None if pointer is None else pointer.version_id
    dependencies.publisher.publish(PublishRequest(
        run=run, units=units, expected_version_id=expected_version_id, now=now,
    ))
    published_run = cast("Run", control_plane.get_run(run.tenant_id, run.run_id))
    return CoordinatorResult(state=published_run.state, execution_ref=None, published=True)


def _cancel(dependencies: CoordinatorDependencies, run: Run, now: datetime) -> CoordinatorResult:
    # get_active_run_dispatch and claim_run_unit are both scoped to PROCESSING runs;
    # once CANCEL_REQUESTED, no dispatch is visible or claimable. Cancellation is
    # therefore best-effort and has no execution_ref (RunUnitState.CANCELED covers the rest).
    dependencies.executor.cancel(CancelRunExecution(
        tenant_id=run.tenant_id, run_id=run.run_id, execution_ref=None,
    ))
    event = _build_event(run, "run.canceled", now)
    canceled = dependencies.control_plane.finalize_run_cancellation(FinalizeRunCancellation(
        tenant_id=run.tenant_id, run_id=run.run_id,
        expected_state=RunState.CANCEL_REQUESTED, canceled_at=now,
    ), event)
    return CoordinatorResult(state=canceled.state, execution_ref=None, published=False)


def _reconcile_admission(execution: ExecutionPolicyConfig, now: datetime) -> Exception | None:
    # The pass still recovers the runs and then fails chained to this error, keeping its summary.
    try:
        execution.admission.reconcile(now)
    except Exception as error:
        logger.exception("unit_permit_reconcile_failed")
        return error
    return None


def _processing_result(outcome: str | DispatchDeferred | None) -> CoordinatorResult:
    if isinstance(outcome, DispatchDeferred):
        return CoordinatorResult(
            state=RunState.PROCESSING, execution_ref=None, published=False,
            deferred=outcome.reason,
        )
    return CoordinatorResult(state=RunState.PROCESSING, execution_ref=outcome, published=False)


class PipelineCoordinator:
    def __init__(
        self, dependencies: CoordinatorDependencies, execution: ExecutionPolicyConfig
    ) -> None:
        self._dependencies = dependencies
        self._execution = execution

    def resume(self, tenant_id: str, run_id: str, *, settle: bool = True) -> CoordinatorResult:
        """Args: settle: False no after_persist, que roda dentro da própria execução.

        Returns: Estado do Run; com settle, o Run que termina liquida o permit dos dispatches.
        Raises: ValueError: Run inexistente ou em estado não suportado.
        """
        run = self._dependencies.control_plane.get_run(tenant_id, run_id)
        if run is None:
            raise ValueError("run_not_found")
        if run.state in _TERMINAL_RUN_STATES:
            return CoordinatorResult(state=run.state, execution_ref=None, published=False)
        now = self._dependencies.clock()
        result = self._advance(run, now)
        if settle and result.state in _TERMINAL_RUN_STATES:
            self._settle_run(run, now)
        return result

    def _advance(self, run: Run, now: datetime) -> CoordinatorResult:
        if run.state is RunState.CANCEL_REQUESTED:
            return _cancel(self._dependencies, run, now)
        if run.state is RunState.PUBLISHING:
            units = self._dependencies.control_plane.list_run_units(run.tenant_id, run.run_id)
            return _publish_now(self._dependencies, run, units, now)
        if run.state is RunState.PROCESSING:
            return self._resume_processing(run, now)
        raise ValueError(f"unsupported_run_state:{run.state}")

    def _settle_run(self, run: Run, now: datetime) -> None:
        admission = self._execution.admission
        if admission is UNGATED_ADMISSION:
            return
        # The units name the dispatches that ran; settling them releases a finished execution's
        # permit as the Run ends. Any other holder waits for the periodic reconcile.
        units = self._dependencies.control_plane.list_run_units(run.tenant_id, run.run_id)
        settle_run_dispatches(admission, units, now)

    def _resume_processing(self, run: Run, now: datetime) -> CoordinatorResult:
        control_plane = self._dependencies.control_plane
        units = control_plane.list_run_units(run.tenant_id, run.run_id)
        plan = RunPlan(
            run=run, units=units, missing_required=(), missing_optional=run.missing_sources,
            deployment_limit=self._execution.deployment_limit,
        )
        decision = decide_fan_in(plan)
        if decision.state is RunState.FAILED:
            failed = _fail_run(control_plane, run, decision.missing_sources, now)
            return CoordinatorResult(state=failed.state, execution_ref=None, published=False)
        if decision.state is RunState.PUBLISHING:
            publishing = _transition_to_publishing(
                control_plane, run, decision.missing_sources, now
            )
            return _publish_now(self._dependencies, publishing, units, now)
        outcome = _dispatch_protocol(
            control_plane, self._dependencies.executor, self._execution, plan, now
        )
        return _processing_result(outcome)

    def recover(self, limit: int = 100) -> tuple[CoordinatorResult, ...]:
        control_plane = self._dependencies.control_plane
        now = self._dependencies.clock()
        reconcile_error = _reconcile_admission(self._execution, now)
        skipped: set[tuple[str, str]] = set()
        results: list[CoordinatorResult] = []
        failed = 0
        while len(results) < limit:
            candidates = _processor_recoverable_runs(
                control_plane, now, limit - len(results), skipped
            )
            if not candidates:
                break
            for run in candidates:
                skipped.add((run.tenant_id, run.run_id))
                try:
                    results.append(self.resume(run.tenant_id, run.run_id))
                except Exception:
                    failed += 1
                    logger.exception(
                        "recover_run_error tenant_id=%s run_id=%s",
                        run.tenant_id,
                        run.run_id,
                    )
        if reconcile_error is not None:
            raise RecoveryFailed(
                tuple(results), failed, reconcile_failed=True,
            ) from reconcile_error
        if failed:
            raise RecoveryFailed(tuple(results), failed)
        return tuple(results)


__all__ = [
    "CoordinatorDependencies",
    "CoordinatorResult",
    "PipelineCoordinator",
    "RecoveryFailed",
    "allow_execution",
    "noop_execution_started",
]
