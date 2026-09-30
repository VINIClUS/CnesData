"""Billing ports: projection, catalog, quota, Stripe, inbox, audit, metrics and clock."""

from collections.abc import Callable
from datetime import datetime
from typing import Protocol, runtime_checkable

from cnes_domain.billing.commands import (
    AttachStripeCustomerCommand,
    CapacityReservationCommand,
    CheckoutCommand,
    ConsumeCapacityCommand,
    ConsumeReservationCommand,
    CreateBillingAccountCommand,
    CreateStripeCustomerCommand,
    HostedSession,
    LinkBillingTenantCommand,
    PortalCommand,
    ReleaseCapacityCommand,
    ReleaseReservationCommand,
    ReserveAnalyticsCommand,
    ReserveRunCommand,
    SnapshotWrite,
    StripeBillingState,
    StripeCustomer,
    StripeStateRequest,
    TransferOwnerCommand,
)
from cnes_domain.billing.inbox import (
    InboxAcceptResult,
    InboxClaim,
    InboxProcessingState,
    InboxRecoveryRecord,
    StripeEvent,
    StripeEventListRequest,
    StripeEventPage,
    StripeRecoveryCursor,
)
from cnes_domain.billing.models import (
    AnalyticsAuthorization,
    BillingAccount,
    BillingAccountPage,
    BillingAccountTenantLink,
    BillingAuditEvent,
    BillingMetric,
    CapacityReservation,
    EntitlementSnapshot,
    PlanVersion,
    QuotaReservation,
    ReadConsistency,
    RunAuthorization,
)

ClockPort = Callable[[], datetime]


@runtime_checkable
class EntitlementProjectionPort(Protocol):
    def get_snapshot(
        self, billing_account_id: str, consistency: ReadConsistency,
    ) -> EntitlementSnapshot | None: ...
    def compare_and_set_snapshot(self, command: SnapshotWrite) -> bool: ...
    def commit_claimed_snapshot(self, claim: InboxClaim, command: SnapshotWrite) -> bool: ...


@runtime_checkable
class BillingCatalogPort(Protocol):
    def create_account(self, command: CreateBillingAccountCommand) -> BillingAccount: ...
    def get_account(self, billing_account_id: str) -> BillingAccount | None: ...
    def get_account_by_customer(self, stripe_customer_id: str) -> BillingAccount | None: ...
    def list_stripe_accounts(self, limit: int, cursor: str | None) -> BillingAccountPage: ...
    def get_tenant_link(
        self, billing_account_id: str, tenant_id: str, consistency: ReadConsistency,
    ) -> BillingAccountTenantLink | None: ...
    def link_tenant(self, command: LinkBillingTenantCommand) -> BillingAccountTenantLink: ...
    def attach_customer(self, command: AttachStripeCustomerCommand) -> BillingAccount: ...
    def transfer_owner(self, command: TransferOwnerCommand) -> BillingAccount: ...
    def publish_plan(self, plan: PlanVersion) -> PlanVersion: ...
    def get_plan(self, plan_version_id: str) -> PlanVersion | None: ...
    def get_plan_by_price(self, stripe_price_id: str) -> PlanVersion | None: ...


@runtime_checkable
class QuotaReservationPort(Protocol):
    def reserve_and_create_run(self, command: ReserveRunCommand) -> RunAuthorization: ...
    def reserve_analytics(self, command: ReserveAnalyticsCommand) -> AnalyticsAuthorization: ...
    def reserve_capacity(self, command: CapacityReservationCommand) -> CapacityReservation: ...
    def consume_capacity(self, command: ConsumeCapacityCommand) -> CapacityReservation: ...
    def release_capacity(self, command: ReleaseCapacityCommand) -> CapacityReservation: ...
    def consume(self, command: ConsumeReservationCommand) -> QuotaReservation: ...
    def release(self, command: ReleaseReservationCommand) -> QuotaReservation: ...


@runtime_checkable
class StripeGatewayPort(Protocol):
    def create_customer(self, command: CreateStripeCustomerCommand) -> StripeCustomer: ...
    def create_checkout(self, command: CheckoutCommand) -> HostedSession: ...
    def create_portal(self, command: PortalCommand) -> HostedSession: ...
    def get_current_state(self, request: StripeStateRequest) -> StripeBillingState: ...
    def list_events(self, request: StripeEventListRequest) -> StripeEventPage: ...


@runtime_checkable
class RecoveryCursorPort(Protocol):
    def load(self, consistency: ReadConsistency) -> StripeRecoveryCursor | None: ...
    def start(self, cursor: StripeRecoveryCursor) -> bool: ...
    def advance(
        self, expected: StripeRecoveryCursor, replacement: StripeRecoveryCursor,
    ) -> bool: ...
    def complete(self, expected: StripeRecoveryCursor, completed_at: datetime) -> bool: ...


@runtime_checkable
class WebhookInboxPort(Protocol):
    def accept(self, event: StripeEvent) -> InboxAcceptResult: ...
    def claim(self, event_id: str, now: datetime) -> InboxClaim: ...
    def mark_processed(self, claim: InboxClaim, entitlement_version: int) -> None: ...
    def mark_failed(self, claim: InboxClaim, error_code: str, retryable: bool) -> None: ...
    def get_state(
        self, event_id: str, consistency: ReadConsistency,
    ) -> InboxProcessingState | None: ...
    def get_recovery_record(
        self, event_id: str, consistency: ReadConsistency,
    ) -> InboxRecoveryRecord | None: ...
    def list_recoverable(self, now: datetime, limit: int) -> tuple[StripeEvent, ...]: ...


@runtime_checkable
class SecretProviderPort(Protocol):
    def get_secret(self, secret_arn: str) -> str: ...


@runtime_checkable
class BillingAuditPort(Protocol):
    def append(self, event: BillingAuditEvent) -> None: ...


@runtime_checkable
class BillingMetricsPort(Protocol):
    def emit(self, metric: BillingMetric) -> None: ...
