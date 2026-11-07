"""Política de concorrência e vinculação de execução faturada."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Protocol, cast, runtime_checkable

from cnes_domain.billing.errors import EntitlementDenied, PermanentBillingError
from cnes_domain.billing.execution import (
    RunBillingState,
    RunExecutionBindingCommand,
    RunExecutionPermit,
)
from cnes_domain.billing.models import BillingAuditEvent
from cnes_domain.billing.validation import require_id
from cnes_domain.control_plane.enums import DispatchState
from cnes_domain.ports.processing import ExecutionPermit
from cnes_domain.profiles import BillingMode

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.billing.ports import BillingAuditPort, ClockPort
    from cnes_domain.control_plane.entities import Run, RunDispatch
    from cnes_domain.ports.processing import StartRunExecution

logger = logging.getLogger(__name__)

LOCAL_ENTITLEMENT_VERSION = 1


def local_billing_account_id(tenant_id: str) -> str:
    """Args: tenant_id: Tenant local.
    Returns: Identificador da conta de faturamento local.
    Raises: ValueError: Tenant em branco.
    """
    require_id(tenant_id, "tenant_id")
    return f"local-{tenant_id}"


@runtime_checkable
class ExecutionBindingPort(Protocol):
    def get_active_run_dispatch(self, tenant_id: str, run_id: str) -> RunDispatch | None:
        raise NotImplementedError

    def get_run_billing_state(self, tenant_id: str, run_id: str) -> RunBillingState | None:
        raise NotImplementedError

    def bind_run_execution(self, command: RunExecutionBindingCommand) -> RunBillingState:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class BillingExecutionDependencies:
    control_plane: ExecutionBindingPort
    clock: ClockPort
    mode: BillingMode
    audit: BillingAuditPort | None = None


def _already_bound(state: RunBillingState, command: RunExecutionBindingCommand) -> bool:
    return state.execution_dispatch_id == command.dispatch_id


def _check_idempotent(state: RunBillingState, command: RunExecutionBindingCommand) -> None:
    if state.execution_ref != command.execution_ref:
        raise PermanentBillingError("run_execution_conflict")
    if state.cancel_requested:
        raise PermanentBillingError("run_execution_canceled")


def _check_expectations(state: RunBillingState, command: RunExecutionBindingCommand) -> None:
    if state.cancel_requested:
        raise PermanentBillingError("run_execution_canceled")
    if state.authorization.entitlement_version != command.expected_entitlement_version:
        raise PermanentBillingError("run_entitlement_changed")
    if state.fencing_token != command.expected_fencing_token:
        raise PermanentBillingError("run_fence_changed")


def _check_previous(state: RunBillingState, command: RunExecutionBindingCommand) -> None:
    current = (state.execution_dispatch_id, state.execution_ref)
    expected = (command.expected_previous_dispatch_id, command.expected_previous_execution_ref)
    if current != expected or command.generation <= state.execution_generation:
        raise PermanentBillingError("run_execution_stale")


def _check_identity(state: RunBillingState | None, command: RunExecutionBindingCommand) -> None:
    if state is None:
        raise PermanentBillingError("run_billing_state_missing")
    if (state.tenant_id, state.run_id) != (command.tenant_id, command.run_id):
        raise PermanentBillingError("run_billing_state_mismatch")


def apply_execution_binding(
    state: RunBillingState | None,
    command: RunExecutionBindingCommand,
) -> RunBillingState:
    """Args: state: Companion atual; command: Vinculação desejada.
    Returns: Novo estado (ou o mesmo objeto em replay idempotente).
    Raises: PermanentBillingError: Estado ausente, divergente ou obsoleto.
    """
    _check_identity(state, command)
    state = cast("RunBillingState", state)
    if _already_bound(state, command):
        _check_idempotent(state, command)
        return state
    _check_expectations(state, command)
    _check_previous(state, command)
    return replace(
        state,
        execution_generation=command.generation,
        execution_wave_id=command.wave_id,
        execution_dispatch_id=command.dispatch_id,
        execution_ref=command.execution_ref,
        execution_unit_ids=command.unit_ids,
        execution_status=DispatchState.STARTED,
        execution_terminal_outcome=None,
        updated_at=command.bound_at,
    )


class BillingConcurrencyPolicy:
    def __init__(self, dependencies: BillingExecutionDependencies) -> None:
        self._dependencies = dependencies

    def __call__(self, run: Run, dispatch: RunDispatch, requested_limit: int) -> ExecutionPermit:
        if (dispatch.tenant_id, dispatch.run_id) != (run.tenant_id, run.run_id):
            raise PermanentBillingError("dispatch_identity_mismatch")
        now = self._dependencies.clock()
        state = self._dependencies.control_plane.get_run_billing_state(
            run.tenant_id, run.run_id
        )
        if state is None:
            return self._without_companion(run, dispatch, requested_limit, now)
        if state.cancel_requested:
            raise EntitlementDenied("reason=run_cancel_requested")
        return _companion_permit(state, dispatch, requested_limit, now)

    def _without_companion(
        self, run: Run, dispatch: RunDispatch, requested_limit: int, now: datetime
    ) -> ExecutionPermit:
        if self._dependencies.mode is BillingMode.STRIPE:
            raise EntitlementDenied("reason=run_billing_state_missing")
        context = RunExecutionPermit(
            billing_account_id=local_billing_account_id(run.tenant_id),
            wave_id=dispatch.wave_id,
            dispatch_id=dispatch.dispatch_id,
            generation=dispatch.generation,
            expected_previous_dispatch_id=None,
            expected_previous_execution_ref=None,
            expected_entitlement_version=LOCAL_ENTITLEMENT_VERSION,
            expected_fencing_token=0,
            authorized_at=now,
        )
        return ExecutionPermit(
            tenant_id=run.tenant_id,
            run_id=run.run_id,
            max_concurrency=requested_limit,
            policy_version=LOCAL_ENTITLEMENT_VERSION,
            fencing_token=0,
            binding_context=context,
        )


def _previous_binding(state: RunBillingState) -> tuple[str | None, str | None]:
    if state.execution_generation == 0:
        return None, None
    return state.execution_dispatch_id, state.execution_ref


def _companion_permit(
    state: RunBillingState, dispatch: RunDispatch, requested_limit: int, now: datetime
) -> ExecutionPermit:
    authorization = state.authorization
    previous = _previous_binding(state)
    context = RunExecutionPermit(
        billing_account_id=state.billing_account_id,
        wave_id=dispatch.wave_id,
        dispatch_id=dispatch.dispatch_id,
        generation=dispatch.generation,
        expected_previous_dispatch_id=previous[0],
        expected_previous_execution_ref=previous[1],
        expected_entitlement_version=authorization.entitlement_version,
        expected_fencing_token=state.fencing_token,
        authorized_at=now,
    )
    return ExecutionPermit(
        tenant_id=state.tenant_id,
        run_id=state.run_id,
        max_concurrency=min(requested_limit, authorization.max_concurrency),
        policy_version=authorization.entitlement_version,
        fencing_token=state.fencing_token,
        binding_context=context,
    )


def _require_context(permit: ExecutionPermit) -> RunExecutionPermit:
    context = permit.binding_context
    if not isinstance(context, RunExecutionPermit):
        raise PermanentBillingError("execution_permit_context_invalid")
    return context


def _require_matching_identity(
    run: Run, request: StartRunExecution, permit: ExecutionPermit, context: RunExecutionPermit
) -> None:
    actual = (
        permit.tenant_id,
        permit.run_id,
        request.tenant_id,
        request.run_id,
        request.dispatch_id,
        request.wave_id,
        permit.policy_version,
        permit.fencing_token,
    )
    expected = (
        run.tenant_id,
        run.run_id,
        run.tenant_id,
        run.run_id,
        context.dispatch_id,
        context.wave_id,
        context.expected_entitlement_version,
        context.expected_fencing_token,
    )
    if actual != expected:
        raise PermanentBillingError("execution_permit_mismatch")


def _require_started(
    active: RunDispatch | None, context: RunExecutionPermit, execution_ref: str
) -> None:
    started = (
        active is not None
        and active.state is DispatchState.STARTED
        and active.dispatch_id == context.dispatch_id
        and active.execution_ref == execution_ref
    )
    if not started:
        raise PermanentBillingError("dispatch_not_started")


class BillingExecutionStarted:
    def __init__(self, dependencies: BillingExecutionDependencies) -> None:
        self._dependencies = dependencies

    def __call__(
        self, run: Run, request: StartRunExecution, execution_ref: str, permit: ExecutionPermit
    ) -> None:
        try:
            self._bind(run, request, execution_ref, permit)
        except Exception as error:
            logger.warning(
                "run_execution_bind_failed tenant_id=%s run_id=%s dispatch_id=%s",
                run.tenant_id,
                run.run_id,
                request.dispatch_id,
            )
            self._audit_failure(run, request, error)
            raise

    def _audit_failure(self, run: Run, request: StartRunExecution, error: Exception) -> None:
        audit = self._dependencies.audit
        if audit is None:
            return
        try:
            audit.append(
                BillingAuditEvent(
                    event_id=(
                        f"run_execution.bind_failed:{run.tenant_id}:{run.run_id}:"
                        f"{request.dispatch_id}"
                    ),
                    event_type="run_execution.bind_failed",
                    aggregate_id=run.run_id,
                    actor_id="system:billing_execution",
                    reason_code="bind_failed",
                    occurred_at=self._dependencies.clock(),
                    attributes={
                        "tenant_id": run.tenant_id,
                        "dispatch_id": request.dispatch_id,
                        "error_code": getattr(error, "code", type(error).__name__),
                    },
                )
            )
        except Exception as audit_error:
            # Auditing must never replace the bind error that is being re-raised.
            logger.warning(
                "billing_audit_append_failed event_type=run_execution.bind_failed code=%s",
                getattr(audit_error, "code", type(audit_error).__name__),
            )

    def _bind(
        self, run: Run, request: StartRunExecution, execution_ref: str, permit: ExecutionPermit
    ) -> None:
        control_plane = self._dependencies.control_plane
        context = _require_context(permit)
        _require_matching_identity(run, request, permit, context)
        active = control_plane.get_active_run_dispatch(run.tenant_id, run.run_id)
        _require_started(active, context, execution_ref)
        state = control_plane.get_run_billing_state(run.tenant_id, run.run_id)
        if state is None:
            if self._dependencies.mode is BillingMode.STRIPE:
                raise PermanentBillingError("run_billing_state_missing")
            return
        control_plane.bind_run_execution(
            RunExecutionBindingCommand(
                tenant_id=run.tenant_id,
                run_id=run.run_id,
                wave_id=context.wave_id,
                dispatch_id=context.dispatch_id,
                generation=context.generation,
                execution_ref=execution_ref,
                unit_ids=request.unit_ids,
                expected_previous_dispatch_id=context.expected_previous_dispatch_id,
                expected_previous_execution_ref=context.expected_previous_execution_ref,
                expected_entitlement_version=context.expected_entitlement_version,
                expected_fencing_token=context.expected_fencing_token,
                bound_at=self._dependencies.clock(),
            )
        )
