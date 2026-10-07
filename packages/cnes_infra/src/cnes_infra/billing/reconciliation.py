"""Reconciliação Stripe: corrige drift do snapshot por CAS e retoma por cursor."""

import hashlib
import json
import logging
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from cnes_domain.billing.commands import SnapshotWrite, StripeBillingState, StripeStateRequest
from cnes_domain.billing.errors import (
    BillingDependencyError,
    BillingError,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.inbox import ReconciliationRequest, ReconciliationResult
from cnes_domain.billing.models import (
    AccessLevel,
    BillingAccount,
    BillingAuditEvent,
    EntitlementAction,
    EntitlementSnapshot,
    ReadConsistency,
    SubscriptionStatus,
)
from cnes_domain.billing.policy import EntitlementPolicy
from cnes_domain.billing.ports import (
    BillingAuditPort,
    BillingCatalogPort,
    BillingMetricsPort,
    ClockPort,
    EntitlementProjectionPort,
    StripeGatewayPort,
)
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.dynamodb_items import deterministic_id
from cnes_infra.billing.enforcement import AccessLossEnforcerPort
from cnes_infra.billing.metrics import BillingMetricName, billing_metric
from cnes_infra.billing.reconciliation_cursor import ReconciliationCursor
from cnes_infra.billing.snapshot_mapping import (
    COMPARED_FIELDS,
    SnapshotMappingInput,
    canonical_fields,
    map_snapshot,
)

__all__ = [
    "COMPARED_FIELDS",
    "CORRECTED_EVENT",
    "DRIFT_EVENT",
    "RECONCILER_ACTOR_ID",
    "RECONCILIATION_CAS_RETRIES",
    "RECONCILIATION_REASON",
    "AccessLossEnforcerPort",
    "BillingReconciler",
    "ReconciliationCursorPort",
    "ReconciliationDependencies",
]

RECONCILER_ACTOR_ID = "system:reconciler"
RECONCILIATION_REASON = "stripe_projection_drift"
DRIFT_EVENT = "billing.reconciliation_drift"
CORRECTED_EVENT = "billing.reconciliation_corrected"
RECONCILIATION_CAS_RETRIES = 3
_ACCESS_LOSS_REASON = "stripe_access_loss"
_STATE_INVALID = "stripe_state_invalid"
_NO_LIVE_SUBSCRIPTION = "stripe_subscription_ambiguous"
_ENDED_STATUSES = frozenset({SubscriptionStatus.CANCELED, SubscriptionStatus.INCOMPLETE_EXPIRED})
_DATA_ERRORS = (ValueError, TypeError, AttributeError)
_STRIPE_UNAVAILABLE = "stripe_unavailable"

logger = logging.getLogger(__name__)


@runtime_checkable
class ReconciliationCursorPort(Protocol):
    def load(self) -> ReconciliationCursor:
        raise NotImplementedError

    def save(
        self, expected: ReconciliationCursor, position: str | None
    ) -> ReconciliationCursor | None:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class ReconciliationDependencies:
    """Portas usadas pela reconciliação Stripe."""

    catalog: BillingCatalogPort
    stripe: StripeGatewayPort
    projection: EntitlementProjectionPort
    cursor: ReconciliationCursorPort
    enforcer: AccessLossEnforcerPort | None
    audit: BillingAuditPort
    metrics: BillingMetricsPort
    clock: ClockPort


@dataclass(slots=True)
class _Tally:
    examined: int = 0
    drift: int = 0
    corrected: int = 0
    failed: int = 0


@dataclass(slots=True)
class _Run:
    stored: ReconciliationCursor
    position: str | None
    tally: _Tally = field(default_factory=_Tally)


@dataclass(frozen=True, slots=True)
class _Observation:
    current: EntitlementSnapshot
    desired: EntitlementSnapshot
    state: StripeBillingState
    fields: tuple[str, ...]


def _fingerprint(snapshot: EntitlementSnapshot) -> str:
    payload = json.dumps(canonical_fields(snapshot), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _drift_fields(
    current: EntitlementSnapshot, desired: EntitlementSnapshot, state: StripeBillingState
) -> tuple[str, ...]:
    before, after = canonical_fields(current), canonical_fields(desired)
    if _billable_after_revocation(current, state):
        before["subscription_status"] = state.subscription_status.value
    return tuple(name for name in COMPARED_FIELDS if before[name] != after[name])


def _billable_after_revocation(current: EntitlementSnapshot, state: StripeBillingState) -> bool:
    # ADMIN_REVOKED is sticky in the mapping, so a still-live Stripe subscription is compared raw.
    revoked = current.subscription_status is SubscriptionStatus.ADMIN_REVOKED
    return revoked and state.subscription_status not in _ENDED_STATUSES


def _audit_event(
    event_type: str, observation: _Observation, corrected: bool
) -> BillingAuditEvent:
    current, desired, state = observation.current, observation.desired, observation.state
    version = current.entitlement_version
    new_hash = _fingerprint(desired)
    return BillingAuditEvent(
        event_id=deterministic_id(event_type, current.billing_account_id, str(version), new_hash),
        event_type=event_type,
        aggregate_id=current.billing_account_id,
        actor_id=RECONCILER_ACTOR_ID,
        reason_code=RECONCILIATION_REASON,
        occurred_at=desired.updated_at,
        attributes={
            "entitlement_version": version + 1 if event_type == CORRECTED_EVENT else version,
            "previous_version": version,
            "prior_snapshot_sha256": _fingerprint(current),
            "new_snapshot_sha256": new_hash,
            "drift_fields": ",".join(observation.fields),
            "stripe_customer_id": state.stripe_customer_id,
            "stripe_subscription_id": state.stripe_subscription_id,
            "latest_invoice_id": state.latest_invoice_id,
            "previous_status": current.subscription_status.value,
            "subscription_status": desired.subscription_status.value,
            "corrected": corrected,
        },
    )


class BillingReconciler:
    """Reconcilia snapshots com o estado atual da Stripe e retoma por cursor."""

    def __init__(self, dependencies: ReconciliationDependencies) -> None:
        self._deps = dependencies

    def run(self, request: ReconciliationRequest) -> ReconciliationResult:
        """Reconcilia um lote de contas Stripe a partir do cursor persistido.

        Args: Limite do lote e cursor opcional que prevalece sobre o persistido.
        Returns: Contadores do lote e próximo cursor (None conclui o ciclo).
        Raises: RetryableBillingError para cursor disputado; falhas inesperadas propagam.
        """
        stored = self._deps.cursor.load()
        position = request.cursor if request.cursor is not None else stored.position
        page = self._deps.catalog.list_stripe_accounts(request.limit, position)
        run = _Run(stored, position)
        for account in page.accounts:
            try:
                self._reconcile_or_fail(account, run.tally)
            except BillingDependencyError:
                return self._finish(run, run.position)
            self._advance(run, account.billing_account_id)
        self._advance(run, page.next_cursor)
        return self._finish(run, page.next_cursor)

    def _advance(self, run: _Run, position: str | None) -> None:
        saved = self._deps.cursor.save(run.stored, position)
        if saved is None:
            raise RetryableBillingError("reconciliation_cursor_contended")
        run.stored = saved
        run.position = position

    def _finish(self, run: _Run, next_cursor: str | None) -> ReconciliationResult:
        tally = run.tally
        now = self._deps.clock()
        self._deps.metrics.emit(
            billing_metric(BillingMetricName.RECONCILIATION_DRIFT, tally.drift, now)
        )
        logger.info(
            "billing_reconcile_completed examined=%d drift=%d corrected=%d failed=%d",
            tally.examined,
            tally.drift,
            tally.corrected,
            tally.failed,
        )
        return ReconciliationResult(
            tally.examined, tally.drift, tally.corrected, tally.failed, next_cursor
        )

    def _reconcile_or_fail(self, account: BillingAccount, tally: _Tally) -> None:
        try:
            self._reconcile(account, tally)
        except BillingError as error:
            tally.failed += 1
            logger.warning(
                "billing_reconcile_failed billing_account_id=%s code=%s",
                account.billing_account_id,
                error.code,
            )
            if isinstance(error, BillingDependencyError) or error.code == _STRIPE_UNAVAILABLE:
                raise BillingDependencyError(error.code) from error

    def _reconcile(self, account: BillingAccount, tally: _Tally) -> None:
        tally.examined += 1
        account_id = account.billing_account_id
        current = self._deps.projection.get_snapshot(account_id, ReadConsistency.STRONG)
        if current is None:
            logger.info(
                "billing_reconcile_skipped billing_account_id=%s reason=snapshot_missing",
                account_id,
            )
            return
        self._enforce(self._settle(account, current, tally))

    def _settle(
        self, account: BillingAccount, current: EntitlementSnapshot, tally: _Tally
    ) -> EntitlementSnapshot:
        seen: _Observation | None = None
        for attempt in range(RECONCILIATION_CAS_RETRIES):
            if attempt:
                current = self._reload(account.billing_account_id)
            observation = self._observe(account, current)
            if not observation.fields:
                return self._without_drift(seen, current, tally)
            if current.subscription_status is SubscriptionStatus.ADMIN_REVOKED:
                self._audit_drift_only(observation, tally)
                return current
            if self._correct(observation):
                tally.drift += 1
                tally.corrected += 1
                return observation.desired
            seen = observation
        raise RetryableBillingError("reconciliation_cas_exhausted")

    def _reload(self, account_id: str) -> EntitlementSnapshot:
        snapshot = self._deps.projection.get_snapshot(account_id, ReadConsistency.STRONG)
        if snapshot is None:
            raise PermanentBillingError("entitlement_snapshot_missing")
        return snapshot

    def _observe(self, account: BillingAccount, current: EntitlementSnapshot) -> _Observation:
        try:
            return self._map_current(account, current)
        except _DATA_ERRORS as error:
            raise PermanentBillingError(_STATE_INVALID) from error

    def _map_current(self, account: BillingAccount, current: EntitlementSnapshot) -> _Observation:
        state = self._stripe_state(account, current)
        plan = self._deps.catalog.get_plan_by_price(state.stripe_price_id)
        if plan is None:
            raise RetryableBillingError("stripe_price_unmapped")
        version = current.entitlement_version + 1
        mapping = SnapshotMappingInput(
            current.billing_account_id, state, plan, current, self._deps.clock()
        )
        desired = map_snapshot(mapping, version, f"reconciliation:{version}")
        return _Observation(current, desired, state, _drift_fields(current, desired, state))

    def _stripe_state(
        self, account: BillingAccount, current: EntitlementSnapshot
    ) -> StripeBillingState:
        # Stripe is read after the snapshot so a stale state never overwrites a projector fix.
        customer = account.stripe_customer_id
        state = self._deps.stripe.get_current_state(
            StripeStateRequest(customer, current.stripe_subscription_id)
        )
        if state.subscription_status not in _ENDED_STATUSES:
            return state
        try:
            return self._deps.stripe.get_current_state(StripeStateRequest(customer, None))
        except BillingError as error:
            if error.code != _NO_LIVE_SUBSCRIPTION:
                raise
        return state

    def _without_drift(
        self, seen: _Observation | None, current: EntitlementSnapshot, tally: _Tally
    ) -> EntitlementSnapshot:
        if seen is not None:
            self._audit_drift_only(seen, tally)
        return current

    def _audit_drift_only(self, observation: _Observation, tally: _Tally) -> None:
        self._deps.audit.append(_audit_event(DRIFT_EVENT, observation, corrected=False))
        tally.drift += 1

    def _correct(self, observation: _Observation) -> bool:
        events = (
            _audit_event(DRIFT_EVENT, observation, corrected=True),
            _audit_event(CORRECTED_EVENT, observation, corrected=True),
        )
        write = SnapshotWrite(observation.current.entitlement_version, observation.desired, events)
        return self._deps.projection.compare_and_set_snapshot(write)

    def _enforce(self, snapshot: EntitlementSnapshot) -> None:
        enforcer = self._deps.enforcer
        if enforcer is None:
            return
        if snapshot.subscription_status is SubscriptionStatus.ADMIN_REVOKED:
            return
        now = self._deps.clock()
        # SERVING_ACCESS is non-critical: its level ignores the valid_until cut-off.
        decision = EntitlementPolicy(BillingMode.STRIPE).evaluate(
            snapshot, EntitlementAction.SERVING_ACCESS, now
        )
        if decision.access_level is AccessLevel.FULL:
            account_id = snapshot.billing_account_id
            result = enforcer.resume_pending(account_id, RECONCILER_ACTOR_ID)
        else:
            result = enforcer.enforce_access_loss(snapshot, RECONCILER_ACTOR_ID)
        if result is not None and result.fenced_run_ids:
            self._deps.metrics.emit(
                billing_metric(
                    BillingMetricName.RUNS_CANCELED_BY_REVOCATION,
                    len(result.fenced_run_ids),
                    now,
                    {"Reason": _ACCESS_LOSS_REASON},
                )
            )
