"""Retomada da reconciliação Stripe após queda converge sem correção duplicada."""

import pytest

pytest.importorskip("moto")

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any

from cnes_domain.billing.commands import StripeBillingState, StripeStateRequest
from cnes_domain.billing.errors import RetryableBillingError
from cnes_domain.billing.inbox import ReconciliationRequest, ReconciliationResult
from cnes_domain.billing.models import (
    EntitlementSnapshot,
    ReadConsistency,
    SubscriptionStatus,
)
from cnes_domain.billing.revocation import RevocationPhase, RevocationSettings
from cnes_domain.control_plane.enums import RunState
from cnes_infra.billing.audit_outbox import DynamoBillingAudit
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_items import encode_snapshot
from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection
from cnes_infra.billing.reconciliation import (
    BillingReconciler,
    ReconciliationDependencies,
)
from cnes_infra.billing.reconciliation_cursor import (
    DynamoReconciliationCursor,
    ReconciliationCursor,
)
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    make_create_command,
    make_plan,
    make_snapshot,
    put_tenant,
)
from packages.cnes_infra.tests.billing.dynamodb_catalog_support import attach
from packages.cnes_infra.tests.billing.revocation_support import (
    RevEnv,
    open_env,
    stored_run,
)
from packages.cnes_infra.tests.billing.test_dynamodb_revocation_service import (
    RecordingExecutor,
    ServiceOptions,
    build_service,
    companion,
    outbox_events,
    seed_simple_run,
    snapshot_of,
)

pytestmark = [pytest.mark.chaos]

ACCOUNT_IDS = ("ba_01", "ba_02", "ba_03")
CORRECTED = "billing.reconciliation_corrected"
PAGE = ReconciliationRequest(limit=10, cursor=None)


class FakeStripe:
    def __init__(self) -> None:
        self.states: dict[str, StripeBillingState] = {}
        self.failing: set[str] = set()
        self.calls: list[str] = []

    def get_current_state(self, request: StripeStateRequest) -> StripeBillingState:
        customer = request.stripe_customer_id
        self.calls.append(customer)
        if customer in self.failing:
            raise RetryableBillingError("stripe_unavailable")
        return self.states[customer]

    def create_customer(self, command: Any) -> Any:
        raise NotImplementedError

    def create_checkout(self, command: Any) -> Any:
        raise NotImplementedError

    def create_portal(self, command: Any) -> Any:
        raise NotImplementedError

    def list_events(self, request: Any) -> Any:
        raise NotImplementedError


class CrashingCursor:
    def __init__(self, inner: DynamoReconciliationCursor, crash_at: str | None) -> None:
        self._inner = inner
        self.crash_at = crash_at
        self.saves = 0

    def load(self) -> ReconciliationCursor:
        return self._inner.load()

    def save(self, expected: ReconciliationCursor, position: str | None) -> Any:
        if position is not None and position == self.crash_at:
            self.crash_at = None
            raise RuntimeError("process_crash")
        self.saves += 1
        return self._inner.save(expected, position)


class CrashingEnforcer:
    def __init__(self, inner: Any, crashes: int) -> None:
        self._inner = inner
        self.crashes = crashes

    def enforce_access_loss(self, snapshot: EntitlementSnapshot, actor_id: str) -> Any:
        if self.crashes > 0:
            self.crashes -= 1
            raise RuntimeError("process_crash")
        return self._inner.enforce_access_loss(snapshot, actor_id)

    def resume_pending(self, billing_account_id: str, actor_id: str) -> Any:
        return self._inner.resume_pending(billing_account_id, actor_id)


class CrashingProgressStore:
    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.crashes = 1

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def save_revocation_progress(self, expected: Any, replacement: Any) -> bool:
        paging = replacement.phase is RevocationPhase.FENCING and replacement.run_cursor
        if paging and self.crashes > 0:
            self.crashes -= 1
            raise RuntimeError("process_crash")
        return self._inner.save_revocation_progress(expected, replacement)


class MetricSink:
    def __init__(self) -> None:
        self.metrics: list[Any] = []

    def emit(self, metric: Any) -> None:
        self.metrics.append(metric)


@dataclass(frozen=True, slots=True)
class Harness:
    env: RevEnv
    stripe: FakeStripe
    cursor: CrashingCursor
    reconciler: BillingReconciler


def stripe_state(customer: str, **changes: Any) -> StripeBillingState:
    state = StripeBillingState(
        stripe_customer_id=customer,
        stripe_subscription_id="sub_01",
        subscription_status=SubscriptionStatus.ACTIVE,
        cancel_at_period_end=False,
        stripe_price_id="price_monthly",
        active_features=frozenset({"create_run", "serving_access"}),
        period_start=NOW,
        period_end=NOW + timedelta(days=30),
        latest_invoice_id="in_01",
    )
    return replace(state, **changes)


def drifted_state(customer: str) -> StripeBillingState:
    return stripe_state(
        customer, period_start=NOW + timedelta(days=1), period_end=NOW + timedelta(days=31)
    )


def seed_accounts(env: RevEnv, plan: Any) -> DynamoBillingCatalog:
    catalog = DynamoBillingCatalog(env.client, env.table, env.clock.now)
    catalog.publish_plan(plan)
    for index, account_id in enumerate(ACCOUNT_IDS):
        tenant = f"tenant-{index}"
        put_tenant(env.client, tenant)
        catalog.create_account(make_create_command(account_id, tenant, f"create-{index}"))
        catalog.attach_customer(attach(account_id, f"cus_{index + 1}"))
    return catalog


def seed_snapshots(env: RevEnv) -> None:
    for account_id in ACCOUNT_IDS:
        item = encode_snapshot(make_snapshot(account_id))
        env.client.put_item(TableName=env.table, Item=item)


def build_harness(
    env: RevEnv, catalog: DynamoBillingCatalog, crash_at: str | None = None,
    enforcer: Any = None,
) -> Harness:
    stripe = FakeStripe()
    cursor = CrashingCursor(
        DynamoReconciliationCursor(env.client, env.table, env.clock.now), crash_at
    )
    dependencies = ReconciliationDependencies(
        catalog=catalog,
        stripe=stripe,
        projection=DynamoEntitlementProjection(env.client, env.table, env.clock.now),
        cursor=cursor,
        enforcer=enforcer or build_service(env, RecordingExecutor()),
        audit=DynamoBillingAudit(env.client, env.table),
        metrics=MetricSink(),
        clock=env.clock.now,
    )
    return Harness(env, stripe, cursor, BillingReconciler(dependencies))


@contextmanager
def drifted_env(crash_at: str | None = None) -> Iterator[Harness]:
    with open_env() as env:
        catalog = seed_accounts(env, make_plan())
        seed_snapshots(env)
        harness = build_harness(env, catalog, crash_at)
        for index in range(len(ACCOUNT_IDS)):
            harness.stripe.states[f"cus_{index + 1}"] = drifted_state(f"cus_{index + 1}")
        yield harness


def version_of(env: RevEnv, account_id: str) -> int:
    projection = DynamoEntitlementProjection(env.client, env.table, env.clock.now)
    snapshot = projection.get_snapshot(account_id, ReadConsistency.STRONG)
    assert snapshot is not None
    return snapshot.entitlement_version


def stored_cursor(env: RevEnv) -> ReconciliationCursor:
    return DynamoReconciliationCursor(env.client, env.table, env.clock.now).load()


def run_event_ids(env: RevEnv) -> list[str]:
    kinds = ("run.cancel_requested", "run.canceled")
    return sorted(e.event_id for kind in kinds for e in outbox_events(env, kind))


def counts(result: ReconciliationResult) -> tuple[int, int, int, int]:
    return result.examined, result.drift_found, result.corrected, result.failed


def test_crash_no_meio_da_pagina_retoma_e_corrige_uma_unica_vez() -> None:
    with drifted_env(crash_at="ba_02") as harness:
        env = harness.env

        with pytest.raises(RuntimeError, match="process_crash"):
            harness.reconciler.run(PAGE)

        assert [version_of(env, account) for account in ACCOUNT_IDS] == [2, 2, 1]
        assert stored_cursor(env).position == "ba_01"
        saves_before = harness.cursor.saves

        result = harness.reconciler.run(PAGE)

        assert counts(result) == (2, 1, 1, 0)
        assert result.next_cursor is None
        assert [version_of(env, account) for account in ACCOUNT_IDS] == [2, 2, 2]
        corrected = outbox_events(env, CORRECTED)
        assert sorted(event.aggregate_id for event in corrected) == list(ACCOUNT_IDS)
        assert len({event.event_id for event in corrected}) == len(ACCOUNT_IDS) == 3
        final = stored_cursor(env)
        assert final.position is None
        assert final.last_completed_at == NOW
        assert final.updated_at is not None
        assert final.version == saves_before + 3 == 4


def test_queda_entre_cas_e_fence_retoma_enforcement() -> None:
    with open_env() as env:
        catalog = seed_accounts(env, make_plan(quotas=snapshot_quotas(env)))
        dispatch = seed_simple_run(env, "run-01")
        executor = RecordingExecutor()
        enforcer = CrashingEnforcer(build_service(env, executor), crashes=1)
        harness = build_harness(env, catalog, enforcer=enforcer)
        before = snapshot_of(env)
        harness.stripe.states["cus_1"] = stripe_state(
            "cus_1", subscription_status=SubscriptionStatus.CANCELED,
            active_features=before.features,
        )

        with pytest.raises(RuntimeError, match="process_crash"):
            harness.reconciler.run(PAGE)

        crashed = snapshot_of(env)
        assert crashed.subscription_status is SubscriptionStatus.CANCELED
        assert crashed.entitlement_version == before.entitlement_version + 1
        assert not companion(env, "run-01").cancel_requested
        assert stored_run(env, "run-01").state is not RunState.CANCELED
        assert stored_cursor(env).position is None
        assert stored_cursor(env).version == 0

        result = harness.reconciler.run(PAGE)

        assert result.drift_found == 0
        resumed = snapshot_of(env)
        assert resumed.entitlement_version == crashed.entitlement_version
        assert resumed.subscription_status is SubscriptionStatus.CANCELED
        assert companion(env, "run-01").cancel_requested
        assert stored_run(env, "run-01").state is RunState.CANCELED
        progress = env.store.get_revocation_progress("ba_01")
        assert progress is not None
        assert (progress.entitlement_version, progress.phase) == (
            resumed.entitlement_version, RevocationPhase.COMPLETE
        )
        assert executor.refs() == {("run-01", dispatch.execution_ref)}
        assert stored_cursor(env).position is None
        assert stored_cursor(env).last_completed_at == NOW

        audited = run_event_ids(env)
        assert audited

        third = harness.reconciler.run(PAGE)

        assert third.drift_found == 0
        assert len(executor.requests) == 1
        assert run_event_ids(env) == audited
        assert companion(env, "run-01").fencing_token == 1


def test_queda_no_meio_do_fencing_e_acesso_restaurado_liquida_runs_fenceadas() -> None:
    with open_env() as env:
        catalog = seed_accounts(env, make_plan(quotas=snapshot_quotas(env)))
        first = seed_simple_run(env, "run-01")
        seed_simple_run(env, "run-02")
        executor = RecordingExecutor()
        options = ServiceOptions(
            settings=RevocationSettings(run_page_size=1),
            store=CrashingProgressStore(env.store),
        )
        harness = build_harness(env, catalog, enforcer=build_service(env, executor, options))
        before = snapshot_of(env)
        harness.stripe.states["cus_1"] = stripe_state(
            "cus_1", subscription_status=SubscriptionStatus.CANCELED,
            active_features=before.features,
        )

        with pytest.raises(RuntimeError, match="process_crash"):
            harness.reconciler.run(PAGE)

        fenced = [run for run in ("run-01", "run-02") if companion(env, run).cancel_requested]
        assert len(fenced) == 1
        untouched = "run-02" if fenced == ["run-01"] else "run-01"
        harness.stripe.states["cus_1"] = stripe_state("cus_1", active_features=before.features)

        result = harness.reconciler.run(PAGE)

        assert counts(result)[1:] == (1, 1, 0)
        assert snapshot_of(env).subscription_status is SubscriptionStatus.ACTIVE
        assert stored_run(env, fenced[0]).state is RunState.CANCELED
        assert not companion(env, untouched).cancel_requested
        assert stored_run(env, untouched).state is not RunState.CANCELED
        assert {run for run, _ in executor.refs()} == set(fenced)
        progress = env.store.get_revocation_progress("ba_01")
        assert progress is not None
        assert progress.phase is RevocationPhase.COMPLETE
        assert first.execution_ref is not None


def test_falha_de_dependencia_nao_avanca_cursor() -> None:
    with drifted_env() as harness:
        env = harness.env
        harness.stripe.failing.add("cus_2")

        result = harness.reconciler.run(PAGE)

        assert counts(result) == (2, 1, 1, 1)
        assert result.next_cursor == "ba_01"
        assert stored_cursor(env).position == "ba_01"
        assert [version_of(env, account) for account in ACCOUNT_IDS] == [2, 1, 1]
        harness.stripe.failing.clear()
        harness.stripe.calls.clear()

        resumed = harness.reconciler.run(PAGE)

        assert counts(resumed) == (2, 2, 2, 0)
        assert resumed.next_cursor is None
        assert harness.stripe.calls == ["cus_2", "cus_3"]
        assert [version_of(env, account) for account in ACCOUNT_IDS] == [2, 2, 2]
        assert stored_cursor(env).position is None


def snapshot_quotas(env: RevEnv) -> Any:
    return snapshot_of(env).quotas
