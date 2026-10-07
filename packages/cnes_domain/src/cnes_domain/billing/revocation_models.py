"""Immediate revocation commands, progress, settings and store capability."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable

from cnes_domain.billing.execution import RunBillingState
from cnes_domain.billing.ports import BillingAuditPort, ClockPort, EntitlementProjectionPort
from cnes_domain.billing.validation import (
    optional_id,
    require_fields,
    require_id,
    require_non_negative,
    require_positive,
    require_utc,
)
from cnes_domain.control_plane.entities import OutboxEvent, Run, RunDispatch
from cnes_domain.control_plane.enums import RunState
from cnes_domain.ports.processing import ProcessorExecutorPort

MAX_REASON_CODE_LENGTH = 128
REVOKED_REASON_CODE = "revoked"
REVOCABLE_RUN_STATES = frozenset(
    {RunState.PLANNED, RunState.WAITING_INPUTS, RunState.PROCESSING, RunState.CANCEL_REQUESTED}
)
PUBLICATION_DENIABLE_RUN_STATES = frozenset({RunState.PUBLISHING})


def _check_reason(reason_code: str) -> None:
    require_id(reason_code, "reason_code")
    if len(reason_code) > MAX_REASON_CODE_LENGTH:
        raise ValueError("reason=reason_code_too_long")


@dataclass(frozen=True, slots=True)
class ImmediateRevocationCommand:
    billing_account_id: str
    actor_id: str
    reason_code: str
    requested_at: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("billing_account_id", "actor_id"))
        _check_reason(self.reason_code)
        require_utc(self.requested_at, "requested_at")


@dataclass(frozen=True, slots=True)
class RevocationResult:
    entitlement_version: int
    fenced_run_ids: tuple[str, ...]
    cancel_failures: tuple[str, ...]
    failed_run_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        require_positive(self.entitlement_version, "entitlement_version")


@dataclass(frozen=True, slots=True)
class RevokeRunCommand:
    tenant_id: str
    run_id: str
    expected_state: RunState
    expected_fencing_token: int
    reason_code: str
    requested_at: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("tenant_id", "run_id"))
        if self.expected_state not in REVOCABLE_RUN_STATES:
            raise ValueError("reason=run_state_not_revocable")
        require_non_negative(self.expected_fencing_token, "expected_fencing_token")
        _check_reason(self.reason_code)
        require_utc(self.requested_at, "requested_at")


@dataclass(frozen=True, slots=True)
class FailDeniedPublicationCommand:
    tenant_id: str
    run_id: str
    expected_fencing_token: int
    reason_code: str
    failed_at: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("tenant_id", "run_id"))
        require_non_negative(self.expected_fencing_token, "expected_fencing_token")
        _check_reason(self.reason_code)
        require_utc(self.failed_at, "failed_at")


@dataclass(frozen=True, slots=True)
class RevocableRunPage:
    runs: tuple[RunBillingState, ...]
    next_cursor: str | None

    def __post_init__(self) -> None:
        optional_id(self.next_cursor, "next_cursor")


@dataclass(frozen=True, slots=True)
class CancelRunUnitsCommand:
    tenant_id: str
    run_id: str
    expected_run_fencing_token: int
    limit: int
    cursor: str | None
    canceled_at: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("tenant_id", "run_id"))
        require_non_negative(self.expected_run_fencing_token, "expected_run_fencing_token")
        require_positive(self.limit, "limit")
        optional_id(self.cursor, "cursor")
        require_utc(self.canceled_at, "canceled_at")


@dataclass(frozen=True, slots=True)
class CancelRunUnitsResult:
    canceled_unit_ids: tuple[str, ...]
    next_cursor: str | None
    run_canceled: bool

    def __post_init__(self) -> None:
        optional_id(self.next_cursor, "next_cursor")
        if self.run_canceled and self.next_cursor is not None:
            raise ValueError("reason=canceled_run_has_cursor")


class RevocationPhase(StrEnum):
    FENCING = "fencing"
    CANCELING = "canceling"
    FINALIZING = "finalizing"
    COMPLETE = "complete"


@dataclass(frozen=True, slots=True)
class RevocationProgress:
    billing_account_id: str
    entitlement_version: int
    phase: RevocationPhase
    run_cursor: str | None
    updated_at: datetime

    def __post_init__(self) -> None:
        require_id(self.billing_account_id, "billing_account_id")
        require_positive(self.entitlement_version, "entitlement_version")
        RevocationPhase(self.phase)
        optional_id(self.run_cursor, "run_cursor")
        if self.phase is RevocationPhase.COMPLETE and self.run_cursor is not None:
            raise ValueError("reason=complete_progress_has_cursor")
        require_utc(self.updated_at, "updated_at")


@runtime_checkable
class RevocationStorePort(Protocol):
    def get_run(self, tenant_id: str, run_id: str) -> Run | None: ...
    def get_run_billing_state(self, tenant_id: str, run_id: str) -> RunBillingState | None: ...
    def get_active_run_dispatch(self, tenant_id: str, run_id: str) -> RunDispatch | None: ...
    def get_run_dispatch(self, tenant_id: str, run_id: str) -> RunDispatch | None: ...
    def list_revocable_runs(
        self, billing_account_id: str, limit: int, cursor: str | None,
    ) -> RevocableRunPage: ...
    def request_run_revocation(
        self, command: RevokeRunCommand, event: OutboxEvent,
    ) -> RunBillingState: ...
    def fail_denied_publication(
        self, command: FailDeniedPublicationCommand, event: OutboxEvent,
    ) -> bool: ...
    def cancel_run_units(self, command: CancelRunUnitsCommand) -> CancelRunUnitsResult: ...
    def get_revocation_progress(self, billing_account_id: str) -> RevocationProgress | None: ...
    def save_revocation_progress(
        self, expected: RevocationProgress | None, replacement: RevocationProgress,
    ) -> bool: ...


@dataclass(frozen=True, slots=True)
class RevocationDependencies:
    projection: EntitlementProjectionPort
    store: RevocationStorePort
    executor: ProcessorExecutorPort
    audit: BillingAuditPort
    clock: ClockPort


@dataclass(frozen=True, slots=True)
class RevocationSettings:
    run_page_size: int = 25
    unit_batch_size: int = 98

    def __post_init__(self) -> None:
        require_positive(self.run_page_size, "run_page_size")
        require_positive(self.unit_batch_size, "unit_batch_size")


DEFAULT_SETTINGS = RevocationSettings()
