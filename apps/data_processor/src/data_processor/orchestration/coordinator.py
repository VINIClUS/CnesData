"""Reserve -> start -> bind dispatch protocol; fan-in driven Run state machine."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from cnes_domain.control_plane.commands import (
    BindRunDispatch,
    FinalizeRunCancellation,
    FinishRunDispatch,
    ReserveRunDispatch,
    TransitionRun,
)
from cnes_domain.control_plane.entities import OutboxEvent
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState, RunState
from cnes_domain.orchestration.fan_in import decide_fan_in
from cnes_domain.orchestration.planner import (
    RunPlan,
    execution_request,
    logical_wave_id,
    ready_units,
)
from cnes_domain.ports.processing import CancelRunExecution, ExecutionPermit, ExecutionStatus
from data_processor.orchestration.publisher import PublishRequest

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_domain.control_plane.entities import Run, RunDispatch, RunUnit
    from cnes_domain.ports.control_plane import ControlPlanePort
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
        status = executor.status(dispatch.execution_ref)
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


def _start_and_bind(
    control_plane: ControlPlanePort, executor: ProcessorExecutorPort,
    execution: ExecutionPolicyConfig, plan: RunPlan, dispatch: RunDispatch, now: datetime,
) -> str:
    run = plan.run
    permit = execution.callbacks.policy(run, dispatch, execution.deployment_limit)
    if permit.tenant_id != run.tenant_id or permit.run_id != run.run_id:
        raise ValueError("execution_permit_identity_mismatch")
    request = execution_request(plan, dispatch, permit.max_concurrency)
    execution_ref = executor.start(request)
    bound = control_plane.bind_run_dispatch(BindRunDispatch(
        tenant_id=run.tenant_id, run_id=run.run_id, dispatch_id=dispatch.dispatch_id,
        execution_ref=execution_ref, now=now, lease_seconds=execution.dispatch_lease_seconds,
    ))
    execution.callbacks.started(run, request, execution_ref, permit)
    return bound.execution_ref


def _dispatch_protocol(
    control_plane: ControlPlanePort, executor: ProcessorExecutorPort,
    execution: ExecutionPolicyConfig, plan: RunPlan, now: datetime,
) -> str | None:
    active = control_plane.get_active_run_dispatch(plan.run.tenant_id, plan.run.run_id)
    if active is not None and active.state is DispatchState.STARTED:
        active = _settle_started(control_plane, executor, plan, active, now)
        if active is not None:
            return active.execution_ref
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
    published_run = control_plane.get_run(run.tenant_id, run.run_id)
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


class PipelineCoordinator:
    def __init__(
        self, dependencies: CoordinatorDependencies, execution: ExecutionPolicyConfig
    ) -> None:
        self._dependencies = dependencies
        self._execution = execution

    def resume(self, tenant_id: str, run_id: str) -> CoordinatorResult:
        control_plane = self._dependencies.control_plane
        run = control_plane.get_run(tenant_id, run_id)
        if run is None:
            raise ValueError("run_not_found")
        if run.state in _TERMINAL_RUN_STATES:
            return CoordinatorResult(state=run.state, execution_ref=None, published=False)
        now = self._dependencies.clock()
        if run.state is RunState.CANCEL_REQUESTED:
            return _cancel(self._dependencies, run, now)
        if run.state is RunState.PUBLISHING:
            units = control_plane.list_run_units(tenant_id, run_id)
            return _publish_now(self._dependencies, run, units, now)
        if run.state is RunState.PROCESSING:
            return self._resume_processing(run, now)
        raise ValueError(f"unsupported_run_state:{run.state}")

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
        execution_ref = _dispatch_protocol(
            control_plane, self._dependencies.executor, self._execution, plan, now
        )
        return CoordinatorResult(
            state=RunState.PROCESSING, execution_ref=execution_ref, published=False
        )

    def recover(self, limit: int = 100) -> tuple[CoordinatorResult, ...]:
        control_plane = self._dependencies.control_plane
        now = self._dependencies.clock()
        skipped: set[tuple[str, str]] = set()
        results: list[CoordinatorResult] = []
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
                    logger.exception(
                        "recover_run_error tenant_id=%s run_id=%s",
                        run.tenant_id,
                        run.run_id,
                    )
        return tuple(results)


__all__ = [
    "CoordinatorDependencies",
    "CoordinatorResult",
    "PipelineCoordinator",
    "allow_execution",
    "noop_execution_started",
]
