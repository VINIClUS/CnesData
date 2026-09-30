"""Local unmetered billing adapters for BILLING_MODE=disabled, with no network or secrets."""

from datetime import UTC, datetime

from cnes_domain.billing.commands import (
    CapacityReservationCommand,
    ConsumeCapacityCommand,
    ConsumeReservationCommand,
    ReleaseCapacityCommand,
    ReleaseReservationCommand,
    ReserveAnalyticsCommand,
    ReserveRunCommand,
    SnapshotWrite,
)
from cnes_domain.billing.errors import BillingDisabledError
from cnes_domain.billing.inbox import InboxClaim
from cnes_domain.billing.models import (
    AnalyticsAuthorization,
    CapacityKind,
    CapacityReservation,
    EntitlementSnapshot,
    QuotaLimits,
    QuotaReservation,
    ReadConsistency,
    ReservationKind,
    ReservationStatus,
    RunAuthorization,
    SubscriptionStatus,
)
from cnes_domain.billing.ports import ClockPort

LOCAL_UNMETERED_PLAN_VERSION_ID = "local-unmetered-v1"
_FOREVER = datetime.max.replace(tzinfo=UTC)
_LOCAL_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_UNMETERED = QuotaLimits(None, None, None, None, None, None)
_CAPACITY_PREFIX = "local-capacity"
_WRITE_DISABLED = "billing_mode=disabled operation=write_snapshot"


def disabled_snapshot(billing_account_id: str, now: datetime) -> EntitlementSnapshot:
    """Args: billing_account_id: Conta local; now: Instante da leitura.
    Returns: Snapshot ativo sem medição e sem expiração.
    """
    return EntitlementSnapshot(
        billing_account_id=billing_account_id,
        stripe_subscription_id=None,
        subscription_status=SubscriptionStatus.ACTIVE,
        cancel_at_period_end=False,
        plan_version_id=LOCAL_UNMETERED_PLAN_VERSION_ID,
        features=frozenset({"*"}),
        quotas=_UNMETERED,
        period_start=now,
        period_end=_FOREVER,
        grace_until=None,
        valid_until=_FOREVER,
        entitlement_version=1,
        updated_at=now,
        source_event_id="local-disabled",
    )


class DisabledEntitlementProjection:
    def __init__(self, clock: ClockPort) -> None:
        self._clock = clock

    def get_snapshot(
        self,
        billing_account_id: str,
        consistency: ReadConsistency,
    ) -> EntitlementSnapshot | None:
        return disabled_snapshot(billing_account_id, self._clock())

    def compare_and_set_snapshot(self, command: SnapshotWrite) -> bool:
        raise BillingDisabledError(_WRITE_DISABLED)

    def commit_claimed_snapshot(self, claim: InboxClaim, command: SnapshotWrite) -> bool:
        raise BillingDisabledError(_WRITE_DISABLED)


class DisabledQuotaReservations:
    def __init__(self, clock: ClockPort) -> None:
        self._clock = clock

    def reserve_and_create_run(self, command: ReserveRunCommand) -> RunAuthorization:
        return RunAuthorization(
            billing_account_id=command.request.billing_account_id,
            plan_version_id=command.snapshot.plan_version_id,
            entitlement_version=command.snapshot.entitlement_version,
            max_concurrency=command.deployment_max_concurrency,
            budget_reservation_id=None,
            authorized_at=self._clock(),
        )

    def reserve_analytics(self, command: ReserveAnalyticsCommand) -> AnalyticsAuthorization:
        return AnalyticsAuthorization(
            billing_account_id=command.request.billing_account_id,
            entitlement_version=command.snapshot.entitlement_version,
            budget_reservation_id=None,
            max_scan_bytes=command.request.estimated_scan_bytes,
            authorized_at=self._clock(),
        )

    def reserve_capacity(self, command: CapacityReservationCommand) -> CapacityReservation:
        reservation_id = f"{_CAPACITY_PREFIX}#{command.kind}#{command.resource_id}"
        return self._capacity(
            command.billing_account_id,
            reservation_id,
            ReservationStatus.RESERVED,
        )

    def consume_capacity(self, command: ConsumeCapacityCommand) -> CapacityReservation:
        return self._capacity(
            command.billing_account_id,
            command.reservation_id,
            ReservationStatus.CONSUMED,
        )

    def release_capacity(self, command: ReleaseCapacityCommand) -> CapacityReservation:
        return self._capacity(
            command.billing_account_id,
            command.reservation_id,
            ReservationStatus.RELEASED,
        )

    def consume(self, command: ConsumeReservationCommand) -> QuotaReservation:
        return self._run_reservation(
            command.billing_account_id,
            command.reservation_id,
            ReservationStatus.CONSUMED,
        )

    def release(self, command: ReleaseReservationCommand) -> QuotaReservation:
        return self._run_reservation(
            command.billing_account_id,
            command.reservation_id,
            ReservationStatus.RELEASED,
        )

    def _capacity(
        self,
        billing_account_id: str,
        reservation_id: str,
        status: ReservationStatus,
    ) -> CapacityReservation:
        kind, resource_id = _decode_capacity_id(reservation_id)
        return CapacityReservation(
            reservation_id=reservation_id,
            billing_account_id=billing_account_id,
            resource_id=resource_id,
            kind=kind,
            status=status,
            created_at=_LOCAL_EPOCH,
            expires_at=_FOREVER,
        )

    def _run_reservation(
        self,
        billing_account_id: str,
        reservation_id: str,
        status: ReservationStatus,
    ) -> QuotaReservation:
        return QuotaReservation(
            reservation_id=reservation_id,
            billing_account_id=billing_account_id,
            resource_id=reservation_id,
            kind=ReservationKind.RUN,
            period_start=_LOCAL_EPOCH,
            reserved_runs=0,
            reserved_scan_bytes=0,
            consumed_runs=0,
            consumed_scan_bytes=0,
            status=status,
            created_at=_LOCAL_EPOCH,
            expires_at=_FOREVER,
        )


def _decode_capacity_id(reservation_id: str) -> tuple[CapacityKind, str]:
    prefix, _, remainder = reservation_id.partition("#")
    kind, _, resource_id = remainder.partition("#")
    if prefix != _CAPACITY_PREFIX or kind not in set(CapacityKind) or not resource_id:
        raise BillingDisabledError("billing_mode=disabled operation=foreign_capacity_reservation")
    return CapacityKind(kind), resource_id
