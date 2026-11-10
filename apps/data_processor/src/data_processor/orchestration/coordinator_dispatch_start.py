"""Reserve, start and bind the dispatch owned by the pipeline coordinator."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast

from cnes_domain.control_plane.commands import (
    BindRunDispatch,
    FinishRunDispatch,
    ReserveRunDispatch,
)
from cnes_domain.control_plane.dispatch_start import (
    StartAdmission,
    claim_dispatch_start,
    execute_dispatch_start,
)
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState, RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict, ControlPlaneErrorCode
from cnes_domain.orchestration.dispatch_admission import dispatch_holder
from cnes_domain.orchestration.planner import (
    RunPlan,
    execution_request,
    logical_wave_id,
    ready_units,
)
from cnes_domain.ports.environment_gate import UNGATED_ADMISSION, DeferralReason, DispatchDeferred
from cnes_domain.ports.processing import CancelRunExecution, ExecutionStatus

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.control_plane.entities import RunDispatch
    from cnes_domain.control_plane.environment_gate import PermitHolder
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.processing import (
        ExecutionPermit,
        ExecutionPolicyConfig,
        ProcessorExecutorPort,
        StartRunExecution,
    )

_TERMINAL_RUN_STATES = frozenset(
    {
        RunState.PUBLISHED,
        RunState.PUBLISHED_DEGRADED,
        RunState.FAILED,
        RunState.CANCELED,
    }
)
_STATUS_OUTCOME = {
    ExecutionStatus.SUCCEEDED: DispatchOutcome.SUCCEEDED,
    ExecutionStatus.FAILED: DispatchOutcome.FAILED,
    ExecutionStatus.CANCELED: DispatchOutcome.CANCELED,
}
logger = logging.getLogger(__name__)


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
    control_plane: ControlPlanePort,
    executor: ProcessorExecutorPort,
    plan: RunPlan,
    dispatch: RunDispatch,
    now: datetime,
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
    control_plane.finish_run_dispatch(
        FinishRunDispatch(
            tenant_id=plan.run.tenant_id,
            run_id=plan.run.run_id,
            dispatch_id=dispatch.dispatch_id,
            outcome=_STATUS_OUTCOME[status],
            finished_at=now,
            claim=dispatch.start_claim,
        )
    )
    return None


def _compensate_start(
    control_plane: ControlPlanePort, executor: ProcessorExecutorPort,
    finish: FinishRunDispatch, execution_ref: str,
) -> None:
    logger.warning(
        "dispatch_start_compensated tenant_id=%s run_id=%s dispatch_id=%s",
        finish.tenant_id, finish.run_id, finish.dispatch_id,
    )
    executor.cancel(CancelRunExecution(
        tenant_id=finish.tenant_id, run_id=finish.run_id, execution_ref=execution_ref,
    ))
    control_plane.finish_run_dispatch(finish)


def _completed_before_bind(
    control_plane: ControlPlanePort, command: BindRunDispatch, unit_ids: tuple[str, ...],
) -> bool:
    run = control_plane.get_run(command.tenant_id, command.run_id)
    if run is None or run.state not in _TERMINAL_RUN_STATES | {RunState.PUBLISHING}:
        return False
    dispatch = control_plane.get_run_dispatch(command.tenant_id, command.run_id)
    if dispatch is None or dispatch.dispatch_id != command.dispatch_id:
        return False
    if dispatch.state not in {
        DispatchState.RESERVED, DispatchState.STARTING, DispatchState.STARTED,
    }:
        return False
    if dispatch.unit_ids != unit_ids or dispatch.start_claim != command.claim:
        return False
    if dispatch.state is DispatchState.STARTED and dispatch.execution_ref != command.execution_ref:
        return False
    units = control_plane.list_run_units(command.tenant_id, command.run_id)
    dispatched = {unit.unit_id for unit in units if unit.dispatch_id == command.dispatch_id}
    completed = {
        unit.unit_id for unit in units
        if unit.dispatch_id == command.dispatch_id
        and unit.state in {
            RunUnitState.SUCCEEDED, RunUnitState.SUCCEEDED_DEGRADED, RunUnitState.FAILED_FINAL,
        }
    }
    return bool(unit_ids) and dispatched == set(unit_ids) and completed == dispatched


def _prepare_start(
    execution: ExecutionPolicyConfig, plan: RunPlan, dispatch: RunDispatch,
) -> tuple[StartRunExecution, PermitHolder, ExecutionPermit]:
    requested_limit = min(len(dispatch.unit_ids), execution.deployment_limit)
    permit = execution.callbacks.policy(plan.run, dispatch, requested_limit)
    if permit.tenant_id != plan.run.tenant_id or permit.run_id != plan.run.run_id:
        raise ValueError("execution_permit_identity_mismatch")
    request = execution_request(plan, dispatch, permit.max_concurrency)
    return request, dispatch_holder(dispatch, request), permit


def _bind_dispatch(
    control_plane: ControlPlanePort, command: BindRunDispatch, unit_ids: tuple[str, ...],
) -> RunDispatch | None:
    try:
        return control_plane.bind_run_dispatch(command)
    except Conflict as error:
        if error.code != ControlPlaneErrorCode.PARENT_NOT_PROCESSING:
            raise
        if not _completed_before_bind(control_plane, command, unit_ids):
            raise
    return None


def _settle_foreign_bind(
    executor: ProcessorExecutorPort,
    dispatch: RunDispatch,
    bind_command: BindRunDispatch,
    bound_elsewhere: RunDispatch | None,
) -> bool:
    if (
        bound_elsewhere is None
        or bound_elsewhere.dispatch_id != bind_command.dispatch_id
        or bound_elsewhere.state is not DispatchState.STARTED
    ):
        return False
    same_execution = (
        bound_elsewhere.generation == dispatch.generation
        and bound_elsewhere.execution_ref == bind_command.execution_ref
        and bound_elsewhere.start_claim == bind_command.claim
    )
    if not same_execution and bound_elsewhere.execution_ref != bind_command.execution_ref:
        executor.cancel(CancelRunExecution(
            tenant_id=bind_command.tenant_id,
            run_id=bind_command.run_id,
            execution_ref=bind_command.execution_ref,
        ))
    return True


def _start_and_bind(
    control_plane: ControlPlanePort,
    executor: ProcessorExecutorPort,
    execution: ExecutionPolicyConfig,
    plan: RunPlan,
    dispatch: RunDispatch,
    now: datetime,
) -> str | DispatchDeferred:
    request, holder, permit = _prepare_start(execution, plan, dispatch)
    admission = execution.admission.admit(holder, now)
    if isinstance(admission, DispatchDeferred):
        return admission
    attempt = StartAdmission(admission, now, execution.dispatch_lease_seconds)
    claim = claim_dispatch_start(control_plane, dispatch, request, attempt)
    if isinstance(claim, DispatchDeferred):
        execution.admission.settle(holder, now)
        return claim
    try:
        execution_ref = execute_dispatch_start(executor, request, claim)
    except Exception:
        execution.admission.settle(holder, now)
        raise
    bind_command = attempt.bind(dispatch, execution_ref, claim)
    bound: RunDispatch | None = None
    try:
        bound = _bind_dispatch(control_plane, bind_command, dispatch.unit_ids)
        execution.admission.confirm(admission, execution_ref, now)
        execution.callbacks.started(plan.run, request, execution_ref, permit)
    except Exception:
        completed = _completed_before_bind(control_plane, bind_command, dispatch.unit_ids)
        if completed:
            execution.admission.settle(holder, now)
            raise
        bound_elsewhere = control_plane.get_run_dispatch(plan.run.tenant_id, plan.run.run_id)
        if not bound and _settle_foreign_bind(executor, dispatch, bind_command, bound_elsewhere):
            execution.admission.settle(holder, now)
            raise
        _compensate_start(control_plane, executor, attempt.cancel(dispatch, claim), execution_ref)
        execution.admission.settle(holder, now)
        raise
    if _completed_before_bind(control_plane, bind_command, dispatch.unit_ids):
        execution.admission.settle(holder, now)
    return execution_ref if bound is None else cast("str", bound.execution_ref)


def _dispatch_protocol(
    control_plane: ControlPlanePort,
    executor: ProcessorExecutorPort,
    execution: ExecutionPolicyConfig,
    plan: RunPlan,
    now: datetime,
) -> str | DispatchDeferred | None:
    active = control_plane.get_active_run_dispatch(plan.run.tenant_id, plan.run.run_id)
    if active is not None and active.state is DispatchState.STARTING:
        return DispatchDeferred(DeferralReason.PERMIT_HELD)
    if active is not None and active.state is DispatchState.STARTED:
        if _settle_started(control_plane, executor, plan, active, now) is not None:
            return active.execution_ref
        execution.admission.settle(dispatch_holder(active), now)
        active = None
    if active is None:
        active = _reserve_next_wave(control_plane, plan, execution, now)
        if active is None:
            return None
    outcome = _start_and_bind(control_plane, executor, execution, plan, active, now)
    current = control_plane.get_run(plan.run.tenant_id, plan.run.run_id)
    if (
        current is not None
        and current.state in _TERMINAL_RUN_STATES
        and execution.admission is not UNGATED_ADMISSION
    ):
        execution.admission.reconcile(now)
    return outcome


__all__ = ["_dispatch_protocol", "_start_and_bind"]
