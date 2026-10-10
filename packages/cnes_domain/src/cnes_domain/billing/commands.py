"""Immutable billing command and request models."""

from dataclasses import dataclass
from datetime import datetime
from typing import cast

from cnes_domain.billing.models import (
    BillingAccount,
    BillingAccountTenantLink,
    BillingAuditEvent,
    CapacityKind,
    EntitlementSnapshot,
    PlanVersion,
    RunAuthorization,
    SubscriptionStatus,
)
from cnes_domain.billing.validation import (
    optional_id,
    optional_non_negative,
    require_bool,
    require_competencia,
    require_fields,
    require_id,
    require_non_negative,
    require_not_before,
    require_positive,
    require_sha256,
    require_unique_ids,
    require_utc,
)
from cnes_domain.control_plane.entities import RunDependency, Tenant


def _check_dependencies(dependencies: tuple[RunDependency, ...]) -> None:
    if not dependencies:
        raise ValueError("reason=dependencies_required")
    keys = {(dep.source_type, dep.file_subtype) for dep in dependencies}
    if len(keys) != len(dependencies):
        raise ValueError("reason=duplicate_dependency")


@dataclass(frozen=True, slots=True)
class CreateBillingAccountCommand:
    account: BillingAccount
    initial_tenant_link: BillingAccountTenantLink
    idempotency_key: str

    def __post_init__(self) -> None:
        require_id(self.idempotency_key, "idempotency_key")
        if self.account.billing_account_id != self.initial_tenant_link.billing_account_id:
            raise ValueError("reason=account_link_mismatch")


@dataclass(frozen=True, slots=True)
class LinkBillingTenantCommand:
    link: BillingAccountTenantLink
    expected_account_updated_at: datetime
    idempotency_key: str

    def __post_init__(self) -> None:
        require_utc(self.expected_account_updated_at, "expected_account_updated_at")
        require_id(self.idempotency_key, "idempotency_key")


@dataclass(frozen=True, slots=True)
class TransferOwnerCommand:
    billing_account_id: str
    expected_owner_user_id: str
    new_owner_user_id: str
    actor_id: str
    reason_code: str
    transferred_at: datetime

    def __post_init__(self) -> None:
        names = (
            "billing_account_id",
            "expected_owner_user_id",
            "new_owner_user_id",
            "actor_id",
            "reason_code",
        )
        require_fields(self, require_id, names)
        require_utc(self.transferred_at, "transferred_at")
        if self.new_owner_user_id == self.expected_owner_user_id:
            raise ValueError("reason=owner_unchanged")


@dataclass(frozen=True, slots=True)
class AttachStripeCustomerCommand:
    billing_account_id: str
    stripe_customer_id: str
    expected_updated_at: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("billing_account_id", "stripe_customer_id"))
        require_utc(self.expected_updated_at, "expected_updated_at")


@dataclass(frozen=True, slots=True)
class CreateBilledTenantCommand:
    tenant: Tenant
    link: BillingAccountTenantLink
    reservation_id: str
    idempotency_key: str
    creator_issuer: str

    def __post_init__(self) -> None:
        names = ("reservation_id", "idempotency_key", "creator_issuer")
        require_fields(self, require_id, names)
        if self.tenant.tenant_id != self.link.tenant_id:
            raise ValueError("reason=tenant_link_mismatch")


@dataclass(frozen=True, slots=True)
class GateRequest:
    billing_account_id: str
    tenant_id: str

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("billing_account_id", "tenant_id"))


@dataclass(frozen=True, slots=True)
class CreateRunRequest:
    billing_account_id: str
    tenant_id: str
    run_id: str
    competencia: str
    dataset_name: str
    dependencies: tuple[RunDependency, ...]
    idempotency_key: str
    request_hash: str
    requested_concurrency: int
    estimated_scan_bytes: int

    def __post_init__(self) -> None:
        names = (
            "billing_account_id",
            "tenant_id",
            "run_id",
            "dataset_name",
            "idempotency_key",
        )
        require_fields(self, require_id, names)
        require_competencia(self.competencia, "competencia")
        require_sha256(self.request_hash, "request_hash")
        require_positive(self.requested_concurrency, "requested_concurrency")
        require_non_negative(self.estimated_scan_bytes, "estimated_scan_bytes")
        _check_dependencies(self.dependencies)


@dataclass(frozen=True, slots=True)
class AnalyticsRequest:
    billing_account_id: str
    tenant_id: str
    query_id: str
    idempotency_key: str
    request_hash: str
    estimated_scan_bytes: int

    def __post_init__(self) -> None:
        names = ("billing_account_id", "tenant_id", "query_id", "idempotency_key")
        require_fields(self, require_id, names)
        require_sha256(self.request_hash, "request_hash")
        require_non_negative(self.estimated_scan_bytes, "estimated_scan_bytes")


@dataclass(frozen=True, slots=True)
class PublishGateRequest:
    billing_account_id: str
    tenant_id: str
    run_id: str
    expected_entitlement_version: int
    expected_fencing_token: int

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("billing_account_id", "tenant_id", "run_id"))
        require_positive(self.expected_entitlement_version, "expected_entitlement_version")
        require_non_negative(self.expected_fencing_token, "expected_fencing_token")


@dataclass(frozen=True, slots=True)
class SnapshotWrite:
    expected_version: int
    snapshot: EntitlementSnapshot
    audit_events: tuple[BillingAuditEvent, ...]

    def __post_init__(self) -> None:
        require_non_negative(self.expected_version, "expected_version")
        if self.snapshot.entitlement_version != self.expected_version + 1:
            raise ValueError("reason=snapshot_version_not_successor")
        if not isinstance(cast("object", self.audit_events), tuple):
            raise ValueError("reason=audit_events_not_tuple")


@dataclass(frozen=True, slots=True)
class ReserveRunCommand:
    request: CreateRunRequest
    snapshot: EntitlementSnapshot
    deployment_max_concurrency: int
    reservation_id: str
    expires_at: datetime

    def __post_init__(self) -> None:
        require_positive(self.deployment_max_concurrency, "deployment_max_concurrency")
        require_id(self.reservation_id, "reservation_id")
        require_utc(self.expires_at, "expires_at")
        if self.request.billing_account_id != self.snapshot.billing_account_id:
            raise ValueError("reason=snapshot_account_mismatch")


@dataclass(frozen=True, slots=True)
class ReserveAnalyticsCommand:
    request: AnalyticsRequest
    snapshot: EntitlementSnapshot
    reservation_id: str
    expires_at: datetime

    def __post_init__(self) -> None:
        require_id(self.reservation_id, "reservation_id")
        require_utc(self.expires_at, "expires_at")
        if self.request.billing_account_id != self.snapshot.billing_account_id:
            raise ValueError("reason=snapshot_account_mismatch")


@dataclass(frozen=True, slots=True)
class AuthorizedRunCommand:
    request: CreateRunRequest
    authorization: RunAuthorization

    def __post_init__(self) -> None:
        if self.request.billing_account_id != self.authorization.billing_account_id:
            raise ValueError("reason=authorization_account_mismatch")


@dataclass(frozen=True, slots=True)
class CapacityReservationCommand:
    billing_account_id: str
    tenant_id: str
    resource_id: str
    kind: CapacityKind
    idempotency_key: str
    request_hash: str
    entitlement_version: int
    limit: int | None

    def __post_init__(self) -> None:
        names = ("billing_account_id", "tenant_id", "resource_id", "idempotency_key")
        require_fields(self, require_id, names)
        require_sha256(self.request_hash, "request_hash")
        require_positive(self.entitlement_version, "entitlement_version")
        optional_non_negative(self.limit, "limit")


@dataclass(frozen=True, slots=True)
class ReleaseCapacityCommand:
    billing_account_id: str
    reservation_id: str
    released_at: datetime
    reason_code: str

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("billing_account_id", "reservation_id", "reason_code"))
        require_utc(self.released_at, "released_at")


@dataclass(frozen=True, slots=True)
class ConsumeCapacityCommand:
    billing_account_id: str
    reservation_id: str
    consumed_at: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("billing_account_id", "reservation_id"))
        require_utc(self.consumed_at, "consumed_at")


@dataclass(frozen=True, slots=True)
class ConsumeReservationCommand:
    billing_account_id: str
    reservation_id: str
    actual_scan_bytes: int
    consumed_at: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("billing_account_id", "reservation_id"))
        require_non_negative(self.actual_scan_bytes, "actual_scan_bytes")
        require_utc(self.consumed_at, "consumed_at")


@dataclass(frozen=True, slots=True)
class ReleaseReservationCommand:
    billing_account_id: str
    reservation_id: str
    released_at: datetime
    reason_code: str

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("billing_account_id", "reservation_id", "reason_code"))
        require_utc(self.released_at, "released_at")


@dataclass(frozen=True, slots=True)
class CheckoutCommand:
    billing_account_id: str
    stripe_customer_id: str
    plan_version: PlanVersion
    idempotency_key: str

    def __post_init__(self) -> None:
        names = ("billing_account_id", "stripe_customer_id", "idempotency_key")
        require_fields(self, require_id, names)


@dataclass(frozen=True, slots=True)
class CreateStripeCustomerCommand:
    billing_account_id: str
    idempotency_key: str

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("billing_account_id", "idempotency_key"))


@dataclass(frozen=True, slots=True)
class StripeCustomer:
    stripe_customer_id: str

    def __post_init__(self) -> None:
        require_id(self.stripe_customer_id, "stripe_customer_id")


@dataclass(frozen=True, slots=True)
class PortalCommand:
    billing_account_id: str
    stripe_customer_id: str
    idempotency_key: str

    def __post_init__(self) -> None:
        names = ("billing_account_id", "stripe_customer_id", "idempotency_key")
        require_fields(self, require_id, names)


@dataclass(frozen=True, slots=True)
class HostedSession:
    session_id: str
    url: str

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("session_id", "url"))
        if not self.url.startswith("https://"):
            raise ValueError("reason=hosted_session_url_not_https")


@dataclass(frozen=True, slots=True)
class ReservePendingCheckoutCommand:
    billing_account_id: str
    request_key: str
    expires_at: datetime

    def __post_init__(self) -> None:
        require_id(self.billing_account_id, "billing_account_id")
        require_sha256(self.request_key, "request_key")
        require_utc(self.expires_at, "expires_at")


@dataclass(frozen=True, slots=True)
class ReleasePendingCheckoutCommand:
    billing_account_id: str
    request_key: str
    reserved_at: datetime

    def __post_init__(self) -> None:
        require_id(self.billing_account_id, "billing_account_id")
        require_sha256(self.request_key, "request_key")
        require_utc(self.reserved_at, "reserved_at")


@dataclass(frozen=True, slots=True)
class PendingCheckout:
    billing_account_id: str
    request_key: str
    reserved_at: datetime
    expires_at: datetime
    replayed: bool = False

    def __post_init__(self) -> None:
        require_id(self.billing_account_id, "billing_account_id")
        require_sha256(self.request_key, "request_key")
        require_utc(self.reserved_at, "reserved_at")
        require_utc(self.expires_at, "expires_at")
        require_not_before(self.expires_at, self.reserved_at, "expiry_before_reservation")


@dataclass(frozen=True, slots=True)
class StripeStateRequest:
    stripe_customer_id: str
    stripe_subscription_id: str | None

    def __post_init__(self) -> None:
        require_id(self.stripe_customer_id, "stripe_customer_id")
        optional_id(self.stripe_subscription_id, "stripe_subscription_id")


@dataclass(frozen=True, slots=True)
class StripeBillingState:
    stripe_customer_id: str
    stripe_subscription_id: str
    subscription_status: SubscriptionStatus
    cancel_at_period_end: bool
    stripe_price_id: str
    active_features: frozenset[str]
    period_start: datetime
    period_end: datetime
    latest_invoice_id: str | None

    def __post_init__(self) -> None:
        names = ("stripe_customer_id", "stripe_subscription_id", "stripe_price_id")
        require_fields(self, require_id, names)
        optional_id(self.latest_invoice_id, "latest_invoice_id")
        require_bool(self.cancel_at_period_end, "cancel_at_period_end_not_bool")
        require_unique_ids(sorted(self.active_features), "active_features")
        require_fields(self, require_utc, ("period_start", "period_end"))
        require_not_before(self.period_end, self.period_start, "period_end_before_start")
