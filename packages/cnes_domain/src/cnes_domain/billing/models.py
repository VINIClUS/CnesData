"""Immutable billing domain models."""

from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum

from cnes_domain.billing.validation import (
    freeze_attributes,
    freeze_dimensions,
    optional_id,
    optional_non_negative,
    optional_utc,
    require_fields,
    require_finite,
    require_id,
    require_non_negative,
    require_not_before,
    require_positive,
    require_unique_ids,
    require_utc,
)

LOCAL_UNMETERED_PLAN_KEY = "local-unmetered"


class SubscriptionStatus(StrEnum):
    TRIALING = "trialing"
    ACTIVE = "active"
    PAST_DUE = "past_due"
    INCOMPLETE = "incomplete"
    INCOMPLETE_EXPIRED = "incomplete_expired"
    UNPAID = "unpaid"
    PAUSED = "paused"
    CANCELED = "canceled"
    ADMIN_REVOKED = "admin_revoked"


class EntitlementAction(StrEnum):
    CREATE_RUN = "create_run"
    REGISTER_AGENT = "register_agent"
    ANALYTICS_QUERY = "analytics_query"
    SERVING_ACCESS = "serving_access"
    TENANT_CREATION = "tenant_creation"
    PUBLISH_RUN = "publish_run"


class BillingAccountStatus(StrEnum):
    ACTIVE = "active"
    TRANSFER_PENDING = "transfer_pending"
    CLOSED = "closed"


class BillingEnforcementMode(StrEnum):
    OFF = "off"
    SHADOW = "shadow"
    ENFORCE = "enforce"


class AccessLevel(StrEnum):
    FULL = "full"
    READ_ONLY = "read_only"
    BLOCKED = "blocked"


class ReservationStatus(StrEnum):
    RESERVED = "reserved"
    CONSUMED = "consumed"
    RELEASED = "released"


class ReservationKind(StrEnum):
    RUN = "run"
    ANALYTICS = "analytics"


class CapacityKind(StrEnum):
    TENANT = "tenant"
    AGENT = "agent"


class ReadConsistency(StrEnum):
    EVENTUAL = "eventual"
    STRONG = "strong"


def _check_features(features: frozenset[str]) -> None:
    require_unique_ids(sorted(features), "features")


def _check_quotas_present(plan_key: str, quotas: "QuotaLimits") -> None:
    if plan_key == LOCAL_UNMETERED_PLAN_KEY:
        return
    if any(getattr(quotas, f.name) is None for f in fields(QuotaLimits)):
        raise ValueError("reason=stripe_plan_requires_quotas")


@dataclass(frozen=True, slots=True)
class BillingAccount:
    billing_account_id: str
    stripe_customer_id: str | None
    owner_user_id: str
    status: BillingAccountStatus
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("billing_account_id", "owner_user_id"))
        optional_id(self.stripe_customer_id, "stripe_customer_id")
        require_fields(self, require_utc, ("created_at", "updated_at"))
        require_not_before(self.updated_at, self.created_at, "updated_before_created")


@dataclass(frozen=True, slots=True)
class BillingAccountTenantLink:
    billing_account_id: str
    tenant_id: str
    linked_by_user_id: str
    reason_code: str
    linked_at: datetime

    def __post_init__(self) -> None:
        names = ("billing_account_id", "tenant_id", "linked_by_user_id", "reason_code")
        require_fields(self, require_id, names)
        require_utc(self.linked_at, "linked_at")


@dataclass(frozen=True, slots=True)
class BillingAccountPage:
    accounts: tuple[BillingAccount, ...]
    next_cursor: str | None

    def __post_init__(self) -> None:
        optional_id(self.next_cursor, "next_cursor")


@dataclass(frozen=True, slots=True)
class QuotaLimits:
    max_tenants: int | None
    max_agents: int | None
    max_runs_per_period: int | None
    max_concurrency: int | None
    retention_days: int | None
    athena_scan_budget_bytes: int | None

    def __post_init__(self) -> None:
        require_fields(self, optional_non_negative, (f.name for f in fields(self)))


@dataclass(frozen=True, slots=True)
class PlanVersion:
    plan_version_id: str
    plan_key: str
    stripe_product_id: str | None
    stripe_price_ids: tuple[str, ...]
    features: frozenset[str]
    quotas: QuotaLimits
    grace_period_days: int
    effective_from: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("plan_version_id", "plan_key"))
        optional_id(self.stripe_product_id, "stripe_product_id")
        require_unique_ids(self.stripe_price_ids, "stripe_price_ids")
        _check_features(self.features)
        _check_quotas_present(self.plan_key, self.quotas)
        require_non_negative(self.grace_period_days, "grace_period_days")
        require_utc(self.effective_from, "effective_from")


@dataclass(frozen=True, slots=True)
class EntitlementSnapshot:
    billing_account_id: str
    stripe_subscription_id: str | None
    subscription_status: SubscriptionStatus
    cancel_at_period_end: bool
    plan_version_id: str
    features: frozenset[str]
    quotas: QuotaLimits
    period_start: datetime
    period_end: datetime
    grace_until: datetime | None
    valid_until: datetime
    entitlement_version: int
    updated_at: datetime
    source_event_id: str

    def __post_init__(self) -> None:
        names = ("billing_account_id", "plan_version_id", "source_event_id")
        require_fields(self, require_id, names)
        optional_id(self.stripe_subscription_id, "stripe_subscription_id")
        _check_features(self.features)
        require_fields(
            self,
            require_utc,
            ("period_start", "period_end", "valid_until", "updated_at"),
        )
        optional_utc(self.grace_until, "grace_until")
        require_positive(self.entitlement_version, "entitlement_version")
        _check_snapshot_window(self)


def _check_snapshot_window(snapshot: EntitlementSnapshot) -> None:
    require_not_before(snapshot.period_end, snapshot.period_start, "period_end_before_start")
    require_not_before(snapshot.valid_until, snapshot.updated_at, "valid_until_before_updated_at")
    if snapshot.grace_until is not None:
        require_not_before(
            snapshot.grace_until, snapshot.period_start, "grace_until_before_period_start"
        )


@dataclass(frozen=True, slots=True)
class EntitlementDecision:
    action: EntitlementAction
    allowed: bool
    access_level: AccessLevel
    reason: str
    entitlement_version: int
    quota_limit: int | None

    def __post_init__(self) -> None:
        require_id(self.reason, "reason")
        require_positive(self.entitlement_version, "entitlement_version")
        optional_non_negative(self.quota_limit, "quota_limit")


@dataclass(frozen=True, slots=True)
class RunAuthorization:
    billing_account_id: str
    plan_version_id: str
    entitlement_version: int
    max_concurrency: int
    budget_reservation_id: str | None
    authorized_at: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("billing_account_id", "plan_version_id"))
        require_fields(self, require_positive, ("entitlement_version", "max_concurrency"))
        optional_id(self.budget_reservation_id, "budget_reservation_id")
        require_utc(self.authorized_at, "authorized_at")


@dataclass(frozen=True, slots=True)
class AnalyticsAuthorization:
    billing_account_id: str
    entitlement_version: int
    budget_reservation_id: str | None
    max_scan_bytes: int
    authorized_at: datetime

    def __post_init__(self) -> None:
        require_id(self.billing_account_id, "billing_account_id")
        require_positive(self.entitlement_version, "entitlement_version")
        optional_id(self.budget_reservation_id, "budget_reservation_id")
        require_non_negative(self.max_scan_bytes, "max_scan_bytes")
        require_utc(self.authorized_at, "authorized_at")


@dataclass(frozen=True, slots=True)
class CapacityReservation:
    reservation_id: str
    billing_account_id: str
    resource_id: str
    kind: CapacityKind
    status: ReservationStatus
    created_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        names = ("reservation_id", "billing_account_id", "resource_id")
        require_fields(self, require_id, names)
        require_fields(self, require_utc, ("created_at", "expires_at"))
        require_not_before(self.expires_at, self.created_at, "expires_before_created")


@dataclass(frozen=True, slots=True)
class QuotaReservation:
    reservation_id: str
    billing_account_id: str
    resource_id: str
    kind: ReservationKind
    period_start: datetime
    reserved_runs: int
    reserved_scan_bytes: int
    consumed_runs: int
    consumed_scan_bytes: int
    status: ReservationStatus
    created_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        names = ("reservation_id", "billing_account_id", "resource_id")
        require_fields(self, require_id, names)
        require_fields(self, require_utc, ("period_start", "created_at", "expires_at"))
        counters = (
            "reserved_runs",
            "reserved_scan_bytes",
            "consumed_runs",
            "consumed_scan_bytes",
        )
        require_fields(self, require_non_negative, counters)
        require_not_before(self.expires_at, self.created_at, "expires_before_created")


@dataclass(frozen=True, slots=True)
class BillingAuditEvent:
    event_id: str
    event_type: str
    aggregate_id: str
    actor_id: str
    reason_code: str
    occurred_at: datetime
    attributes: Mapping[str, str | int | bool | None]

    def __post_init__(self) -> None:
        names = ("event_id", "event_type", "aggregate_id", "actor_id", "reason_code")
        require_fields(self, require_id, names)
        require_utc(self.occurred_at, "occurred_at")
        object.__setattr__(self, "attributes", freeze_attributes(self.attributes, "attributes"))


@dataclass(frozen=True, slots=True)
class BillingMetric:
    name: str
    value: float
    unit: str
    dimensions: Mapping[str, str]
    occurred_at: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("name", "unit"))
        require_finite(self.value, "value")
        require_utc(self.occurred_at, "occurred_at")
        object.__setattr__(self, "dimensions", freeze_dimensions(self.dimensions, "dimensions"))
