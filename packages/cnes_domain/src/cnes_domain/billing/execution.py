"""Billing companions for run execution binding and publication."""

from dataclasses import dataclass
from datetime import datetime

from cnes_domain.billing.models import RunAuthorization
from cnes_domain.billing.validation import (
    optional_id,
    require_bool,
    require_fields,
    require_hex16,
    require_id,
    require_non_negative,
    require_positive,
    require_unique_ids,
    require_utc,
)
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState


def _check_binding(wave_id: str, dispatch_id: str, generation: int) -> None:
    require_hex16(wave_id, "wave_id")
    require_hex16(dispatch_id, "dispatch_id")
    require_positive(generation, "generation")


def _check_previous_binding(dispatch_id: str | None, execution_ref: str | None) -> None:
    if (dispatch_id is None) != (execution_ref is None):
        raise ValueError("reason=previous_binding_incomplete")
    if dispatch_id is not None and execution_ref is not None:
        require_hex16(dispatch_id, "expected_previous_dispatch_id")
        require_id(execution_ref, "expected_previous_execution_ref")


def _check_expectations(entitlement_version: int, fencing_token: int) -> None:
    require_positive(entitlement_version, "expected_entitlement_version")
    require_non_negative(fencing_token, "expected_fencing_token")


def _check_unit_ids(unit_ids: tuple[str, ...]) -> None:
    if not unit_ids:
        raise ValueError("reason=unit_ids_required")
    require_unique_ids(unit_ids, "unit_ids")


@dataclass(frozen=True, slots=True)
class RunExecutionPermit:
    billing_account_id: str
    wave_id: str
    dispatch_id: str
    generation: int
    expected_previous_dispatch_id: str | None
    expected_previous_execution_ref: str | None
    expected_entitlement_version: int
    expected_fencing_token: int
    authorized_at: datetime

    def __post_init__(self) -> None:
        require_id(self.billing_account_id, "billing_account_id")
        _check_binding(self.wave_id, self.dispatch_id, self.generation)
        _check_previous_binding(
            self.expected_previous_dispatch_id, self.expected_previous_execution_ref
        )
        _check_expectations(self.expected_entitlement_version, self.expected_fencing_token)
        require_utc(self.authorized_at, "authorized_at")


@dataclass(frozen=True, slots=True)
class RunBillingState:
    billing_account_id: str
    tenant_id: str
    run_id: str
    authorization: RunAuthorization
    execution_generation: int
    execution_wave_id: str | None
    execution_dispatch_id: str | None
    execution_ref: str | None
    execution_unit_ids: tuple[str, ...]
    execution_status: DispatchState | None
    execution_terminal_outcome: DispatchOutcome | None
    fencing_token: int
    cancel_requested: bool
    updated_at: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("billing_account_id", "tenant_id", "run_id"))
        if self.authorization.billing_account_id != self.billing_account_id:
            raise ValueError("reason=authorization_account_mismatch")
        require_non_negative(self.execution_generation, "execution_generation")
        require_non_negative(self.fencing_token, "fencing_token")
        _check_execution_binding(self)
        _check_terminal_outcome(self.execution_status, self.execution_terminal_outcome)
        require_bool(self.cancel_requested, "cancel_requested_not_bool")
        require_utc(self.updated_at, "updated_at")


def _check_execution_binding(state: RunBillingState) -> None:
    if state.execution_generation == 0:
        _check_unbound(state)
        return
    require_hex16(state.execution_wave_id, "execution_wave_id")
    require_hex16(state.execution_dispatch_id, "execution_dispatch_id")
    _check_unit_ids(state.execution_unit_ids)
    if state.execution_status is None:
        raise ValueError("reason=bound_execution_requires_status")
    optional_id(state.execution_ref, "execution_ref")


def _check_unbound(state: RunBillingState) -> None:
    bound = (
        state.execution_wave_id,
        state.execution_dispatch_id,
        state.execution_ref,
        state.execution_status,
        state.execution_terminal_outcome,
    )
    if any(value is not None for value in bound) or state.execution_unit_ids != ():
        raise ValueError("reason=unbound_execution_has_binding")


def _check_terminal_outcome(
    status: DispatchState | None, outcome: DispatchOutcome | None
) -> None:
    if (outcome is not None) != (status is DispatchState.TERMINAL):
        raise ValueError("reason=terminal_outcome_mismatch")


@dataclass(frozen=True, slots=True)
class RunExecutionBindingCommand:
    tenant_id: str
    run_id: str
    wave_id: str
    dispatch_id: str
    generation: int
    execution_ref: str
    unit_ids: tuple[str, ...]
    expected_previous_dispatch_id: str | None
    expected_previous_execution_ref: str | None
    expected_entitlement_version: int
    expected_fencing_token: int
    bound_at: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("tenant_id", "run_id", "execution_ref"))
        _check_binding(self.wave_id, self.dispatch_id, self.generation)
        _check_unit_ids(self.unit_ids)
        _check_previous_binding(
            self.expected_previous_dispatch_id, self.expected_previous_execution_ref
        )
        _check_expectations(self.expected_entitlement_version, self.expected_fencing_token)
        require_utc(self.bound_at, "bound_at")


@dataclass(frozen=True, slots=True)
class PublicationGuard:
    billing_account_id: str
    expected_entitlement_version: int
    expected_run_fencing_token: int
    checked_at: datetime

    def __post_init__(self) -> None:
        require_id(self.billing_account_id, "billing_account_id")
        require_positive(self.expected_entitlement_version, "expected_entitlement_version")
        require_non_negative(self.expected_run_fencing_token, "expected_run_fencing_token")
        require_utc(self.checked_at, "checked_at")
