"""Fakes de fronteira do observador de shadow."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from cnes_domain.billing.models import (
    BillingAccountTenantLink,
    BillingAuditEvent,
    BillingMetric,
    CapacityKind,
    EntitlementAction,
    EntitlementSnapshot,
    QuotaLimits,
    ReadConsistency,
    SubscriptionStatus,
)
from cnes_domain.billing.shadow import (
    ShadowEntitlementObserver,
    ShadowObservation,
    ShadowObserverDependencies,
)

NOW = datetime(2026, 9, 15, 12, 30, tzinfo=UTC)
ACCOUNT = "ba-1"
TENANT = "354130"
LOGGER = "cnes_domain.billing.shadow"
_S = SubscriptionStatus
QUOTAS = QuotaLimits(
    max_tenants=3,
    max_agents=2,
    max_runs_per_period=100,
    max_concurrency=4,
    retention_days=30,
    athena_scan_budget_bytes=1_000_000,
)
SNAPSHOT = EntitlementSnapshot(
    billing_account_id=ACCOUNT,
    stripe_subscription_id="sub_1",
    subscription_status=_S.ACTIVE,
    cancel_at_period_end=False,
    plan_version_id="plan-v1",
    features=frozenset({"analytics_query", "serving_history"}),
    quotas=QUOTAS,
    period_start=datetime(2026, 9, 1, tzinfo=UTC),
    period_end=datetime(2026, 10, 1, tzinfo=UTC),
    grace_until=None,
    valid_until=NOW + timedelta(hours=1),
    entitlement_version=7,
    updated_at=NOW - timedelta(hours=1),
    source_event_id="evt_1",
)
UNSET: Any = object()


def make_link(tenant_id: str = TENANT, account: str = ACCOUNT) -> BillingAccountTenantLink:
    return BillingAccountTenantLink(account, tenant_id, "user-1", "tenant_created", NOW)


class FakeCatalog:
    def __init__(
        self,
        link: BillingAccountTenantLink | None = None,
        tenant_link: BillingAccountTenantLink | None = None,
        error: Exception | None = None,
    ) -> None:
        self.link = link
        self.tenant_link = tenant_link
        self.error = error
        self.calls: list[tuple[str, ...]] = []

    def get_tenant_account(
        self, tenant_id: str, consistency: ReadConsistency,
    ) -> BillingAccountTenantLink | None:
        self.calls.append(("account", tenant_id, consistency.value))
        if self.error is not None:
            raise self.error
        return self.link

    def get_tenant_link(
        self, billing_account_id: str, tenant_id: str, consistency: ReadConsistency,
    ) -> BillingAccountTenantLink | None:
        self.calls.append(("link", billing_account_id, tenant_id, consistency.value))
        return self.tenant_link


class FakeProjection:
    def __init__(
        self, snapshot: EntitlementSnapshot | None = SNAPSHOT, error: Exception | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.error = error
        self.calls: list[tuple[str, ReadConsistency]] = []

    def get_snapshot(
        self, billing_account_id: str, consistency: ReadConsistency,
    ) -> EntitlementSnapshot | None:
        self.calls.append((billing_account_id, consistency))
        if self.error is not None:
            raise self.error
        return self.snapshot


class FakeCapacity:
    def __init__(self, counts: dict[CapacityKind, int] | None = None) -> None:
        self.counts = counts or {}
        self.calls: list[tuple[str, CapacityKind]] = []

    def get_capacity_count(self, billing_account_id: str, kind: CapacityKind) -> int | None:
        self.calls.append((billing_account_id, kind))
        return self.counts.get(kind)


class SpyAudit:
    def __init__(self, error: Exception | None = None) -> None:
        self.events: list[BillingAuditEvent] = []
        self.error = error

    def append(self, event: BillingAuditEvent) -> None:
        if self.error is not None:
            raise self.error
        self.events.append(event)


class SpyMetrics:
    def __init__(self, error: Exception | None = None) -> None:
        self.metrics: list[BillingMetric] = []
        self.error = error

    def emit(self, metric: BillingMetric) -> None:
        if self.error is not None:
            raise self.error
        self.metrics.append(metric)


class Harness:
    def __init__(self, **overrides: Any) -> None:
        self.catalog = overrides.get("catalog") or FakeCatalog(make_link())
        self.projection = overrides.get("projection") or FakeProjection()
        self.capacity = overrides.get("capacity") or FakeCapacity()
        self.audit = overrides.get("audit") or SpyAudit()
        self.metrics = overrides.get("metrics", UNSET)
        if self.metrics is UNSET:
            self.metrics = SpyMetrics()
        self.clock = overrides.get("clock") or (lambda: NOW)
        self.observer = ShadowEntitlementObserver(ShadowObserverDependencies(
            self.catalog, cast("Any", self.projection), self.capacity, self.audit, self.clock,
            self.metrics,
        ))

    def reasons(self) -> list[str]:
        return [event.reason_code for event in self.audit.events]

    def metric_names(self) -> list[str]:
        return [metric.name for metric in self.metrics.metrics]


def observe(harness: Harness, observation: ShadowObservation) -> None:
    harness.observer.observe(observation)


def bucket(*parts: str) -> str:
    encoded = json.dumps(list(parts), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()[:32]


def agent_observation() -> ShadowObservation:
    return ShadowObservation(EntitlementAction.REGISTER_AGENT, TENANT)
