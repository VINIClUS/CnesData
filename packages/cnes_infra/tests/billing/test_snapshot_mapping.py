"""Testes do mapeamento puro Stripe para snapshot de entitlement."""

from datetime import timedelta
from typing import Any

from cnes_domain.billing.commands import StripeBillingState
from cnes_domain.billing.models import SubscriptionStatus
from cnes_infra.billing.snapshot_mapping import (
    STRIPE_SNAPSHOT_VALIDITY_MARGIN_HOURS,
    SnapshotMappingInput,
    map_snapshot,
    mapped_status,
)
from packages.cnes_infra.tests.billing.billing_factories import NOW, make_plan, make_snapshot

MARGIN = timedelta(hours=STRIPE_SNAPSHOT_VALIDITY_MARGIN_HOURS)
PERIOD_END = NOW + timedelta(days=30)


def _state(**changes: Any) -> StripeBillingState:
    values: dict[str, Any] = {
        "stripe_customer_id": "cus_01",
        "stripe_subscription_id": "sub_01",
        "subscription_status": SubscriptionStatus.ACTIVE,
        "cancel_at_period_end": False,
        "stripe_price_id": "price_monthly",
        "active_features": frozenset({"create_run"}),
        "period_start": NOW,
        "period_end": PERIOD_END,
        "latest_invoice_id": None,
    }
    values.update(changes)
    return StripeBillingState(**values)


def _inputs(state: StripeBillingState, current: Any = None, now: Any = NOW):
    return SnapshotMappingInput("ba_01", state, make_plan(), current, now)


def test_status_admin_revoked_do_snapshot_atual_e_sticky():
    current = make_snapshot(subscription_status=SubscriptionStatus.ADMIN_REVOKED)
    assert mapped_status(_inputs(_state(), current)) is SubscriptionStatus.ADMIN_REVOKED


def test_status_da_stripe_sem_snapshot_atual():
    state = _state(subscription_status=SubscriptionStatus.TRIALING)
    assert mapped_status(_inputs(state)) is SubscriptionStatus.TRIALING


def test_status_da_stripe_quando_snapshot_atual_nao_revogado():
    state = _state(subscription_status=SubscriptionStatus.CANCELED)
    assert mapped_status(_inputs(state, make_snapshot())) is SubscriptionStatus.CANCELED


def test_grace_ancorado_na_mesma_assinatura_past_due():
    anchor = NOW + timedelta(days=2)
    current = make_snapshot(subscription_status=SubscriptionStatus.PAST_DUE, grace_until=anchor)
    state = _state(subscription_status=SubscriptionStatus.PAST_DUE)
    snapshot = map_snapshot(_inputs(state, current), 2, "evt_x")
    assert snapshot.grace_until == anchor


def test_grace_ancorado_usa_period_start_quando_maior():
    current = make_snapshot(subscription_status=SubscriptionStatus.PAST_DUE, grace_until=NOW)
    later = NOW + timedelta(days=10)
    state = _state(subscription_status=SubscriptionStatus.PAST_DUE, period_start=later)
    assert map_snapshot(_inputs(state, current), 2, "evt_x").grace_until == later


def test_grace_recalculado_para_assinatura_diferente():
    current = make_snapshot(
        subscription_status=SubscriptionStatus.PAST_DUE,
        grace_until=NOW + timedelta(days=30),
        stripe_subscription_id="sub_old",
    )
    state = _state(subscription_status=SubscriptionStatus.PAST_DUE)
    snapshot = map_snapshot(_inputs(state, current), 2, "evt_x")
    assert snapshot.grace_until == NOW + timedelta(days=7)


def test_grace_recalculado_quando_snapshot_atual_nao_past_due():
    state = _state(subscription_status=SubscriptionStatus.PAST_DUE)
    snapshot = map_snapshot(_inputs(state, make_snapshot()), 2, "evt_x")
    assert snapshot.grace_until == NOW + timedelta(days=7)


def test_grace_recalculado_quando_grace_atual_none():
    current = make_snapshot(subscription_status=SubscriptionStatus.PAST_DUE, grace_until=None)
    state = _state(subscription_status=SubscriptionStatus.PAST_DUE)
    assert map_snapshot(_inputs(state, current), 2, "evt_x").grace_until == NOW + timedelta(days=7)


def test_sem_grace_para_status_diferente_de_past_due():
    snapshot = map_snapshot(_inputs(_state()), 1, "evt_x")
    assert snapshot.grace_until is None
    assert snapshot.valid_until == PERIOD_END + MARGIN


def test_valid_until_usa_grace_quando_posterior_ao_period_end():
    state = _state(subscription_status=SubscriptionStatus.PAST_DUE, period_end=NOW)
    snapshot = map_snapshot(_inputs(state), 1, "evt_x")
    assert snapshot.valid_until == NOW + timedelta(days=7) + MARGIN


def test_valid_until_usa_now_posterior_ao_period_end():
    later = PERIOD_END + timedelta(days=5)
    snapshot = map_snapshot(_inputs(_state(), now=later), 1, "evt_x")
    assert snapshot.valid_until == later + MARGIN
    assert snapshot.updated_at == later


def test_propaga_versao_e_source_event_id():
    snapshot = map_snapshot(_inputs(_state()), 7, "evt_abc")
    assert snapshot.entitlement_version == 7
    assert snapshot.source_event_id == "evt_abc"


def test_features_quotas_e_plano_vem_do_state_e_do_plan():
    plan = make_plan(plan_version_id="plan_v9", max_agents=9)
    state = _state(cancel_at_period_end=True, active_features=frozenset({"a", "b"}))
    snapshot = map_snapshot(SnapshotMappingInput("ba_77", state, plan, None, NOW), 1, "e")
    assert snapshot.billing_account_id == "ba_77"
    assert snapshot.stripe_subscription_id == "sub_01"
    assert snapshot.plan_version_id == "plan_v9"
    assert snapshot.quotas == plan.quotas
    assert snapshot.features == frozenset({"a", "b"})
    assert snapshot.cancel_at_period_end is True
    assert (snapshot.period_start, snapshot.period_end) == (NOW, PERIOD_END)
