"""Projetor de eventos Stripe: estado atual da Stripe para snapshot de entitlement."""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from cnes_domain.billing.commands import SnapshotWrite, StripeBillingState, StripeStateRequest
from cnes_domain.billing.errors import (
    PermanentBillingError,
    RetryableBillingError,
    StaleInboxClaim,
)
from cnes_domain.billing.inbox import InboxClaim, ProjectionResult
from cnes_domain.billing.models import (
    BillingAuditEvent,
    EntitlementSnapshot,
    PlanVersion,
    ReadConsistency,
    SubscriptionStatus,
)
from cnes_domain.billing.ports import (
    BillingCatalogPort,
    ClockPort,
    EntitlementProjectionPort,
    StripeGatewayPort,
    WebhookInboxPort,
)
from cnes_infra.billing.dynamodb_items import deterministic_id
from cnes_infra.billing.webhook_inbox_items import STRIPE_INBOX_MAX_ATTEMPTS

STRIPE_PROJECTION_CAS_RETRIES = 3
STRIPE_SNAPSHOT_VALIDITY_MARGIN_HOURS = 72
PROJECTION_ACTOR_ID = "stripe_webhook"
_PROJECTION_REASON = "stripe_webhook_projection"
_STATE_INVALID = "stripe_state_invalid"
_ATTEMPTS_EXHAUSTED = "inbox_attempts_exhausted"
_TRANSIENT_PERMANENT_CODES = frozenset({"stripe_request_rejected"})
_DATA_ERRORS = (ValueError, TypeError, AttributeError)
_ENDED_STATUSES = frozenset({SubscriptionStatus.CANCELED, SubscriptionStatus.INCOMPLETE_EXPIRED})

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ProjectorDependencies:
    """Portas usadas pelo projetor de eventos Stripe."""

    inbox: WebhookInboxPort
    catalog: BillingCatalogPort
    stripe: StripeGatewayPort
    projection: EntitlementProjectionPort
    clock: ClockPort


@dataclass(frozen=True, slots=True)
class _Inputs:
    claim: InboxClaim
    billing_account_id: str
    state: StripeBillingState
    plan: PlanVersion
    current: EntitlementSnapshot | None
    now: datetime


def _status(inputs: _Inputs) -> SubscriptionStatus:
    current = inputs.current
    if current is not None and current.subscription_status is SubscriptionStatus.ADMIN_REVOKED:
        return SubscriptionStatus.ADMIN_REVOKED
    return inputs.state.subscription_status


def _grace_until(inputs: _Inputs, status: SubscriptionStatus) -> datetime | None:
    if status is not SubscriptionStatus.PAST_DUE:
        return None
    current = inputs.current
    if (
        current is not None
        and current.subscription_status is SubscriptionStatus.PAST_DUE
        and current.stripe_subscription_id == inputs.state.stripe_subscription_id
        and current.grace_until is not None
    ):
        return max(current.grace_until, inputs.state.period_start)
    return inputs.state.period_start + timedelta(days=inputs.plan.grace_period_days)


def _valid_until(inputs: _Inputs, grace_until: datetime | None) -> datetime:
    period_end = inputs.state.period_end
    latest = max(inputs.now, period_end, grace_until or period_end)
    return latest + timedelta(hours=STRIPE_SNAPSHOT_VALIDITY_MARGIN_HOURS)


def _snapshot(inputs: _Inputs, status: SubscriptionStatus, version: int) -> EntitlementSnapshot:
    state = inputs.state
    grace_until = _grace_until(inputs, status)
    return EntitlementSnapshot(
        billing_account_id=inputs.billing_account_id,
        stripe_subscription_id=state.stripe_subscription_id,
        subscription_status=status,
        cancel_at_period_end=state.cancel_at_period_end,
        plan_version_id=inputs.plan.plan_version_id,
        features=state.active_features,
        quotas=inputs.plan.quotas,
        period_start=state.period_start,
        period_end=state.period_end,
        grace_until=grace_until,
        valid_until=_valid_until(inputs, grace_until),
        entitlement_version=version,
        updated_at=inputs.now,
        source_event_id=inputs.claim.event_id,
    )


def _audit(inputs: _Inputs, snapshot: EntitlementSnapshot, event_type: str) -> BillingAuditEvent:
    version = snapshot.entitlement_version
    return BillingAuditEvent(
        event_id=deterministic_id(event_type, inputs.claim.event_id, str(version)),
        event_type=event_type,
        aggregate_id=inputs.billing_account_id,
        actor_id=PROJECTION_ACTOR_ID,
        reason_code=_PROJECTION_REASON,
        occurred_at=inputs.now,
        attributes={
            "source_event_id": inputs.claim.event_id,
            "stripe_event_type": inputs.claim.event_type,
            "entitlement_version": version,
            "previous_version": version - 1,
            "plan_version_id": snapshot.plan_version_id,
            "subscription_status": snapshot.subscription_status.value,
            **_previous_status(inputs, event_type),
        },
    )


def _previous_status(inputs: _Inputs, event_type: str) -> dict[str, str | None]:
    if event_type != "subscription.status_changed":
        return {}
    current = inputs.current
    return {"previous_status": None if current is None else current.subscription_status.value}


def _audits(inputs: _Inputs, snapshot: EntitlementSnapshot) -> tuple[BillingAuditEvent, ...]:
    events = [_audit(inputs, snapshot, "entitlement.changed")]
    current = inputs.current
    if current is None or current.subscription_status is not snapshot.subscription_status:
        events.append(_audit(inputs, snapshot, "subscription.status_changed"))
    return tuple(events)


def _build_write(inputs: _Inputs) -> SnapshotWrite:
    expected = 0 if inputs.current is None else inputs.current.entitlement_version
    snapshot = _snapshot(inputs, _status(inputs), expected + 1)
    return SnapshotWrite(expected, snapshot, _audits(inputs, snapshot))


def _not_applied(event_id: str) -> ProjectionResult:
    return ProjectionResult(event_id, False, None)


class StripeEventProjector:
    """Projeta o estado atual da Stripe no snapshot sob o fence do inbox."""

    def __init__(self, dependencies: ProjectorDependencies) -> None:
        self._deps = dependencies

    def process(self, event_id: str) -> ProjectionResult:
        """Reivindica o evento e projeta o estado atual da Stripe.

        Args: Identificador do evento Stripe no inbox.
        Returns: Resultado com aplicação e versão do entitlement.
        Raises: RetryableBillingError para falha recuperável (já registrada no inbox).
        """
        claim = self._deps.inbox.claim(event_id, self._deps.clock())
        if not claim.acquired:
            return _not_applied(event_id)
        try:
            if claim.attempt > STRIPE_INBOX_MAX_ATTEMPTS:
                self._fail(claim, _ATTEMPTS_EXHAUSTED, retryable=False)
                return _not_applied(event_id)
            return self._project_or_fail(claim)
        except StaleInboxClaim:
            logger.info("stripe_projection_stale event_id=%s", event_id)
            return _not_applied(event_id)

    def _project_or_fail(self, claim: InboxClaim) -> ProjectionResult:
        try:
            return self._project(claim)
        except RetryableBillingError as error:
            self._fail(claim, error.code, retryable=True)
            raise
        except PermanentBillingError as error:
            return self._fail_permanent(claim, error)
        except _DATA_ERRORS:
            self._fail(claim, _STATE_INVALID, retryable=False)
            return _not_applied(claim.event_id)

    def _fail_permanent(self, claim: InboxClaim, error: PermanentBillingError) -> ProjectionResult:
        if error.code in _TRANSIENT_PERMANENT_CODES:
            self._fail(claim, error.code, retryable=True)
            raise RetryableBillingError(error.code) from error
        self._fail(claim, error.code, retryable=False)
        return _not_applied(claim.event_id)

    def _fail(self, claim: InboxClaim, code: str, retryable: bool) -> None:
        self._deps.inbox.mark_failed(claim, code, retryable)
        logger.info(
            "stripe_projection_failed event_id=%s code=%s retryable=%s",
            claim.event_id, code, retryable,
        )

    def _project(self, claim: InboxClaim) -> ProjectionResult:
        account = self._deps.catalog.get_account_by_customer(claim.customer_id)
        if account is None:
            raise RetryableBillingError("stripe_customer_mapping_missing")
        for _ in range(STRIPE_PROJECTION_CAS_RETRIES):
            current = self._deps.projection.get_snapshot(
                account.billing_account_id, ReadConsistency.STRONG
            )
            state = self._current_state(claim, current)
            write = self._write(claim, account.billing_account_id, state, current)
            if self._deps.projection.commit_claimed_snapshot(claim, write):
                version = write.snapshot.entitlement_version
                logger.info(
                    "stripe_projection_applied event_id=%s version=%d", claim.event_id, version
                )
                return ProjectionResult(claim.event_id, True, version)
        raise RetryableBillingError("snapshot_cas_exhausted")

    def _current_state(
        self, claim: InboxClaim, current: EntitlementSnapshot | None
    ) -> StripeBillingState:
        current_id = None if current is None else current.stripe_subscription_id
        subscription_id = claim.subscription_id or current_id
        state = self._deps.stripe.get_current_state(
            StripeStateRequest(claim.customer_id, subscription_id)
        )
        superseded = (
            current_id is not None
            and current_id != state.stripe_subscription_id
            and state.subscription_status in _ENDED_STATUSES
        )
        if not superseded:
            return state
        return self._deps.stripe.get_current_state(
            StripeStateRequest(claim.customer_id, current_id)
        )

    def _write(
        self,
        claim: InboxClaim,
        account_id: str,
        state: StripeBillingState,
        current: EntitlementSnapshot | None,
    ) -> SnapshotWrite:
        plan = self._deps.catalog.get_plan_by_price(state.stripe_price_id)
        if plan is None:
            raise RetryableBillingError("stripe_price_unmapped")
        return _build_write(_Inputs(claim, account_id, state, plan, current, self._deps.clock()))
