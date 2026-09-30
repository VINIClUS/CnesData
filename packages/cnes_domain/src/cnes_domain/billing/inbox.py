"""Modelos imutáveis do inbox de webhooks e da recuperação Stripe."""

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum

from cnes_domain.billing.validation import (
    optional_id,
    require_id,
    require_non_negative,
    require_positive,
    require_sha256,
    require_utc,
)

STRIPE_EVENT_PAGE_LIMIT = 100


class InboxDisposition(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    IGNORED = "ignored"


class InboxProcessingState(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED_RETRYABLE = "failed_retryable"
    FAILED_FINAL = "failed_final"
    IGNORED = "ignored"


_ACTIVE_STATES = frozenset({
    InboxProcessingState.PENDING,
    InboxProcessingState.PROCESSING,
    InboxProcessingState.FAILED_RETRYABLE,
})


def _require_bool(value: object, reason: str) -> None:
    if not isinstance(value, bool):
        raise ValueError(f"reason={reason}")


def _is_positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _require_page_limit(value: object, reason: str) -> None:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= STRIPE_EVENT_PAGE_LIMIT
    ):
        raise ValueError(f"reason={reason}")


def _require_counters(owner: object, names: tuple[str, ...]) -> None:
    for name in names:
        require_non_negative(getattr(owner, name), name)


@dataclass(frozen=True, slots=True)
class InboxAcceptResult:
    event_id: str
    disposition: InboxDisposition

    def __post_init__(self) -> None:
        require_id(self.event_id, "event_id")


@dataclass(frozen=True, slots=True)
class InboxClaim:
    event_id: str
    event_type: str
    customer_id: str
    subscription_id: str | None
    attempt: int | None
    acquired: bool

    def __post_init__(self) -> None:
        require_id(self.event_id, "event_id")
        require_id(self.event_type, "event_type")
        require_id(self.customer_id, "customer_id")
        optional_id(self.subscription_id, "subscription_id")
        _require_bool(self.acquired, "claim_acquired_not_bool")
        consistent = _is_positive_int(self.attempt) if self.acquired else self.attempt is None
        if not consistent:
            raise ValueError("reason=claim_attempt_mismatch")


@dataclass(frozen=True, slots=True)
class InboxRecoveryRecord:
    state: InboxProcessingState
    attempt: int
    due_at: datetime | None
    due_index_key: str | None

    def __post_init__(self) -> None:
        require_non_negative(self.attempt, "attempt")
        if self.state in _ACTIVE_STATES:
            if self.due_at is None or self.due_index_key is None:
                raise ValueError("reason=active_state_requires_due")
            require_utc(self.due_at, "due_at")
            require_id(self.due_index_key, "due_index_key")
        elif self.due_at is not None or self.due_index_key is not None:
            raise ValueError("reason=terminal_state_forbids_due")


@dataclass(frozen=True, slots=True)
class ProjectionResult:
    event_id: str
    applied: bool
    entitlement_version: int | None

    def __post_init__(self) -> None:
        require_id(self.event_id, "event_id")
        _require_bool(self.applied, "applied_not_bool")
        if self.entitlement_version is not None:
            require_positive(self.entitlement_version, "entitlement_version")
        elif self.applied:
            raise ValueError("reason=applied_requires_version")


@dataclass(frozen=True, slots=True)
class RecoveryRequest:
    lookback_hours: int
    batch_size: int

    def __post_init__(self) -> None:
        require_positive(self.lookback_hours, "lookback_hours")
        _require_page_limit(self.batch_size, "batch_size_out_of_range")


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    scanned: int
    imported: int
    reprocessed: int
    failed: int
    next_cursor: str | None

    def __post_init__(self) -> None:
        _require_counters(self, ("scanned", "imported", "reprocessed", "failed"))
        optional_id(self.next_cursor, "next_cursor")


@dataclass(frozen=True, slots=True)
class ReservationRecoveryRequest:
    now: datetime
    limit: int
    cursor: str | None

    def __post_init__(self) -> None:
        require_utc(self.now, "now")
        require_positive(self.limit, "limit")
        optional_id(self.cursor, "cursor")


@dataclass(frozen=True, slots=True)
class ReservationRecoveryResult:
    examined: int
    released: int
    next_cursor: str | None

    def __post_init__(self) -> None:
        _require_counters(self, ("examined", "released"))
        optional_id(self.next_cursor, "next_cursor")
        if self.released > self.examined:
            raise ValueError("reason=released_exceeds_examined")


@dataclass(frozen=True, slots=True)
class ReconciliationRequest:
    limit: int
    cursor: str | None

    def __post_init__(self) -> None:
        require_positive(self.limit, "limit")
        optional_id(self.cursor, "cursor")


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    examined: int
    drift_found: int
    corrected: int
    failed: int
    next_cursor: str | None

    def __post_init__(self) -> None:
        _require_counters(self, ("examined", "drift_found", "corrected", "failed"))
        optional_id(self.next_cursor, "next_cursor")
        if self.corrected > self.drift_found:
            raise ValueError("reason=corrected_exceeds_drift")


@dataclass(frozen=True, slots=True)
class StripeEvent:
    event_id: str
    event_type: str
    created_at: datetime
    stripe_customer_id: str | None
    stripe_subscription_id: str | None
    payload_sha256: str

    def __post_init__(self) -> None:
        require_id(self.event_id, "event_id")
        require_id(self.event_type, "event_type")
        require_utc(self.created_at, "created_at")
        optional_id(self.stripe_customer_id, "stripe_customer_id")
        optional_id(self.stripe_subscription_id, "stripe_subscription_id")
        require_sha256(self.payload_sha256, "payload_sha256")


@dataclass(frozen=True, slots=True)
class StripeEventListRequest:
    created_gte: datetime
    starting_after: str | None
    limit: int

    def __post_init__(self) -> None:
        require_utc(self.created_gte, "created_gte")
        optional_id(self.starting_after, "starting_after")
        _require_page_limit(self.limit, "limit_out_of_range")


@dataclass(frozen=True, slots=True)
class StripeEventPage:
    events: tuple[StripeEvent, ...]
    has_more: bool

    def __post_init__(self) -> None:
        _require_bool(self.has_more, "has_more_not_bool")


@dataclass(frozen=True, slots=True)
class StripeRecoveryCursor:
    cycle_id: str
    created_gte: datetime
    starting_after: str | None
    version: int

    def __post_init__(self) -> None:
        require_id(self.cycle_id, "cycle_id")
        require_utc(self.created_gte, "created_gte")
        optional_id(self.starting_after, "starting_after")
        require_positive(self.version, "version")

    def advance(self, starting_after: str | None) -> "StripeRecoveryCursor":
        return replace(self, starting_after=starting_after, version=self.version + 1)


def require_cursor_successor(
    expected: StripeRecoveryCursor, replacement: StripeRecoveryCursor,
) -> None:
    """Args: expected: Cursor atual; replacement: Cursor proposto.
    Raises: ValueError: Substituto não é o sucessor imediato do cursor atual.
    """
    if (
        replacement.cycle_id != expected.cycle_id
        or replacement.created_gte != expected.created_gte
        or replacement.version != expected.version + 1
    ):
        raise ValueError("reason=cursor_not_successor")
