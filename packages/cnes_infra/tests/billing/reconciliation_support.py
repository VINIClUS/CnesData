"""Fakes em memória e montagem do ambiente de testes da reconciliação."""

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from cnes_domain.billing.commands import SnapshotWrite, StripeBillingState, StripeStateRequest
from cnes_domain.billing.models import (
    BillingAccount,
    BillingAccountPage,
    BillingAuditEvent,
    BillingMetric,
    EntitlementSnapshot,
    PlanVersion,
    ReadConsistency,
    SubscriptionStatus,
)
from cnes_domain.billing.revocation import RevocationResult
from cnes_infra.billing.reconciliation import BillingReconciler, ReconciliationDependencies
from cnes_infra.billing.reconciliation_cursor import ReconciliationCursor
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    make_account,
    make_plan,
    make_snapshot,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock

PRICE = "price_monthly"
FEATURES = frozenset({"create_run", "serving_access"})


def make_state(**changes: Any) -> StripeBillingState:
    values: dict[str, Any] = {
        "stripe_customer_id": "cus_01",
        "stripe_subscription_id": "sub_01",
        "subscription_status": SubscriptionStatus.ACTIVE,
        "cancel_at_period_end": False,
        "stripe_price_id": PRICE,
        "active_features": FEATURES,
        "period_start": NOW,
        "period_end": NOW + timedelta(days=30),
        "latest_invoice_id": "in_01",
    }
    values.update(changes)
    return StripeBillingState(**values)


def stripe_account(account_id: str) -> BillingAccount:
    return make_account(account_id, stripe_customer_id="cus_01")


class FakeCatalog:
    def __init__(self, accounts: list[BillingAccount], plan: PlanVersion | None) -> None:
        self.accounts = sorted(accounts, key=lambda item: item.billing_account_id)
        self.plan = plan
        self.next_cursor: str | None = None
        self.calls: list[tuple[int, str | None]] = []

    def list_stripe_accounts(self, limit: int, cursor: str | None) -> BillingAccountPage:
        self.calls.append((limit, cursor))
        rest = [a for a in self.accounts if cursor is None or a.billing_account_id > cursor]
        return BillingAccountPage(tuple(rest[:limit]), self.next_cursor)

    def get_plan_by_price(self, stripe_price_id: str) -> PlanVersion | None:
        return self.plan if stripe_price_id == PRICE else None


class FakeStripe:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.states: list[StripeBillingState] = [make_state()]
        self.error: Exception | None = None
        self.requests: list[StripeStateRequest] = []

    def get_current_state(self, request: StripeStateRequest) -> StripeBillingState:
        self.events.append("stripe")
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return self.states.pop(0) if len(self.states) > 1 else self.states[0]


class FakeProjection:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.snapshots: dict[str, EntitlementSnapshot] = {}
        self.racers: list[EntitlementSnapshot | None] = []
        self.writes: list[SnapshotWrite] = []
        self.committed_audits: list[BillingAuditEvent] = []
        self.cas_calls = 0

    def get_snapshot(
        self, billing_account_id: str, consistency: ReadConsistency
    ) -> EntitlementSnapshot | None:
        assert consistency is ReadConsistency.STRONG
        self.events.append("snapshot")
        return self.snapshots.get(billing_account_id)

    def compare_and_set_snapshot(self, command: SnapshotWrite) -> bool:
        self.events.append("cas")
        self.cas_calls += 1
        self.writes.append(command)
        account_id = command.snapshot.billing_account_id
        if self.racers:
            self._race(account_id)
            return False
        stored = self.snapshots[account_id]
        if stored.entitlement_version != command.expected_version:
            return False
        self.snapshots[account_id] = command.snapshot
        self.committed_audits.extend(command.audit_events)
        return True

    def _race(self, account_id: str) -> None:
        racer = self.racers.pop(0)
        if racer is None:
            del self.snapshots[account_id]
        else:
            self.snapshots[account_id] = racer


class FakeCursor:
    def __init__(self) -> None:
        self.stored = ReconciliationCursor(None, 0, None, None)
        self.saves: list[str | None] = []
        self.contended_after: int | None = None

    def load(self) -> ReconciliationCursor:
        return self.stored

    def save(self, expected: ReconciliationCursor, position: str | None):
        if self.contended_after is not None and len(self.saves) >= self.contended_after:
            return None
        self.saves.append(position)
        self.stored = ReconciliationCursor(position, expected.version + 1, NOW, None)
        return self.stored


class FakeEnforcer:
    def __init__(self) -> None:
        self.calls: list[tuple[EntitlementSnapshot, str]] = []
        self.fenced: tuple[str, ...] = ()
        self.error: Exception | None = None

    def enforce_access_loss(self, snapshot: EntitlementSnapshot, actor_id: str) -> RevocationResult:
        self.calls.append((snapshot, actor_id))
        if self.error is not None:
            raise self.error
        return RevocationResult(snapshot.entitlement_version, self.fenced, ())


class FakeAudit:
    def __init__(self) -> None:
        self.events: list[BillingAuditEvent] = []

    def append(self, event: BillingAuditEvent) -> None:
        self.events.append(event)


class FakeMetrics:
    def __init__(self) -> None:
        self.emitted: list[BillingMetric] = []

    def emit(self, metric: BillingMetric) -> None:
        self.emitted.append(metric)

    def named(self, name: str) -> list[BillingMetric]:
        return [metric for metric in self.emitted if metric.name == name]


@dataclass
class Env:
    catalog: FakeCatalog
    stripe: FakeStripe
    projection: FakeProjection
    cursor: FakeCursor
    enforcer: FakeEnforcer
    audit: FakeAudit
    metrics: FakeMetrics
    events: list[str] = field(default_factory=list)
    clock: MutableClock = field(default_factory=lambda: MutableClock(NOW))

    def reconciler(self) -> BillingReconciler:
        return BillingReconciler(
            ReconciliationDependencies(
                self.catalog, self.stripe, self.projection, self.cursor,
                self.enforcer, self.audit, self.metrics, self.clock.now,
            )
        )


def make_env(*account_ids: str, plan: PlanVersion | None = None) -> Env:
    events: list[str] = []
    ids = account_ids or ("ba_01",)
    chosen = make_plan(features=FEATURES) if plan is None else plan
    env = Env(
        FakeCatalog([stripe_account(item) for item in ids], chosen),
        FakeStripe(events), FakeProjection(events), FakeCursor(),
        FakeEnforcer(), FakeAudit(), FakeMetrics(), events,
    )
    for item in ids:
        env.projection.snapshots[item] = make_snapshot(item)
    return env


def drifted(account_id: str = "ba_01", version: int = 1, **changes: Any) -> EntitlementSnapshot:
    values: dict[str, Any] = {"features": frozenset({"create_run"})}
    values.update(changes)
    return make_snapshot(account_id, version, **values)
