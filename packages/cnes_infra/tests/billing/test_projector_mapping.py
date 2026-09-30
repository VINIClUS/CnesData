"""Testes do StripeEventProjector (BIL-021): mapeamento de ciclo de vida e auditoria."""

from datetime import timedelta

import pytest

from cnes_domain.billing.commands import StripeStateRequest
from cnes_domain.billing.models import SubscriptionStatus
from cnes_infra.billing.projector import (
    PROJECTION_ACTOR_ID,
    STRIPE_SNAPSHOT_VALIDITY_MARGIN_HOURS,
)
from cnes_infra.billing.webhook_inbox_items import STRIPE_WEBHOOK_EVENT_TYPES
from packages.cnes_infra.tests.billing.billing_factories import NOW
from packages.cnes_infra.tests.billing.test_projector import (
    ACCOUNT_ID,
    CUSTOMER,
    SUBSCRIPTION,
    make_state,
    projector_env,
)

MARGIN = timedelta(hours=STRIPE_SNAPSHOT_VALIDITY_MARGIN_HOURS)
ACTIVE = SubscriptionStatus.ACTIVE


@pytest.mark.parametrize("event_type", sorted(STRIPE_WEBHOOK_EVENT_TYPES))
def test_todo_tipo_de_evento_projeta_o_estado_atual(event_type):
    subscription = None if event_type.startswith("entitlements.") else SUBSCRIPTION
    with projector_env() as env:
        env.accept("evt_01", event_type, subscription)
        result = env.projector().process("evt_01")
        snapshot = env.snapshot()
    env.stripe.get_current_state.assert_called_once_with(StripeStateRequest(CUSTOMER, subscription))
    assert result.applied is True
    assert snapshot.subscription_status is ACTIVE
    assert snapshot.billing_account_id == ACCOUNT_ID
    assert snapshot.stripe_subscription_id == SUBSCRIPTION
    assert snapshot.source_event_id == "evt_01"


@pytest.mark.parametrize(
    ("event_type", "status"),
    [
        ("customer.subscription.paused", SubscriptionStatus.PAUSED),
        ("customer.subscription.resumed", SubscriptionStatus.ACTIVE),
        ("customer.subscription.deleted", SubscriptionStatus.CANCELED),
        ("invoice.payment_failed", SubscriptionStatus.PAST_DUE),
        ("invoice.payment_action_required", SubscriptionStatus.INCOMPLETE),
    ],
)
def test_status_do_stripe_e_copiado_para_o_snapshot(event_type, status):
    with projector_env() as env:
        env.stripe.get_current_state.return_value = make_state(subscription_status=status)
        env.accept("evt_01", event_type)
        env.projector().process("evt_01")
        snapshot = env.snapshot()
    assert snapshot.subscription_status is status


def test_cancelamento_no_fim_do_periodo_e_projetado():
    with projector_env() as env:
        env.stripe.get_current_state.return_value = make_state(cancel_at_period_end=True)
        env.accept()
        env.projector().process("evt_01")
        snapshot = env.snapshot()
    assert snapshot.cancel_at_period_end is True
    assert snapshot.subscription_status is ACTIVE


def test_renovacao_paga_desloca_o_periodo():
    renewed_start = NOW + timedelta(days=30)
    with projector_env() as env:
        renewed_end = renewed_start + timedelta(days=30)
        renewed = make_state(period_start=renewed_start, period_end=renewed_end)
        env.stripe.get_current_state.side_effect = [make_state(), renewed]
        env.accept("evt_01")
        env.accept("evt_02", "invoice.paid")
        env.projector().process("evt_01")
        env.projector().process("evt_02")
        snapshot = env.snapshot()
    assert snapshot.period_start == renewed_start
    assert snapshot.period_end == renewed_start + timedelta(days=30)
    assert snapshot.entitlement_version == 2


def test_past_due_define_grace_a_partir_do_inicio_do_periodo():
    with projector_env() as env:
        env.clock.advance(timedelta(days=3))
        env.stripe.get_current_state.return_value = make_state(
            subscription_status=SubscriptionStatus.PAST_DUE
        )
        env.accept("evt_01", "invoice.payment_failed")
        env.projector().process("evt_01")
        snapshot = env.snapshot()
    assert snapshot.grace_until == NOW + timedelta(days=7)


def test_status_ativo_nao_define_grace():
    with projector_env() as env:
        env.accept()
        env.projector().process("evt_01")
        snapshot = env.snapshot()
    assert snapshot.grace_until is None


def test_valid_until_usa_fim_do_periodo_mais_margem():
    with projector_env() as env:
        env.accept()
        env.projector().process("evt_01")
        snapshot = env.snapshot()
    assert snapshot.valid_until == NOW + timedelta(days=30) + MARGIN


def test_valid_until_usa_grace_quando_posterior_ao_fim_do_periodo():
    state = make_state(
        subscription_status=SubscriptionStatus.PAST_DUE, period_end=NOW + timedelta(days=1)
    )
    with projector_env() as env:
        env.stripe.get_current_state.return_value = state
        env.accept("evt_01", "invoice.payment_failed")
        env.projector().process("evt_01")
        snapshot = env.snapshot()
    assert snapshot.valid_until == NOW + timedelta(days=7) + MARGIN


def test_valid_until_usa_agora_quando_periodo_ja_terminou():
    state = make_state(period_start=NOW - timedelta(days=60), period_end=NOW - timedelta(days=30))
    with projector_env() as env:
        env.stripe.get_current_state.return_value = state
        env.accept()
        env.projector().process("evt_01")
        snapshot = env.snapshot()
    assert snapshot.valid_until == NOW + MARGIN


def test_versao_esperada_encadeia_entre_eventos():
    with projector_env() as env:
        for index in (1, 2, 3):
            env.accept(f"evt_{index:02d}")
        projector = env.projector()
        results = [projector.process(f"evt_0{index}") for index in (1, 2, 3)]
        versions = [result.entitlement_version for result in results]
        snapshot = env.snapshot()
    assert versions == [1, 2, 3]
    assert snapshot.source_event_id == "evt_03"


def test_primeiro_snapshot_audita_mudanca_de_entitlement_e_de_status():
    with projector_env() as env:
        env.accept()
        env.projector().process("evt_01")
        changed = env.audit_rows("entitlement.changed", "evt_01", 1)
        status = env.audit_rows("subscription.status_changed", "evt_01", 1)
    assert len(changed) == 1
    assert len(status) == 1
    assert changed[0]["aggregate_id"] == ACCOUNT_ID
    assert changed[0]["payload"]["actor_id"] == PROJECTION_ACTOR_ID
    assert changed[0]["payload"]["reason_code"] == "stripe_webhook_projection"
    attributes = changed[0]["payload"]["attributes"]
    assert attributes["source_event_id"] == "evt_01"
    assert attributes["stripe_event_type"] == "customer.subscription.updated"
    assert attributes["entitlement_version"] == 1
    assert attributes["previous_version"] == 0
    assert attributes["plan_version_id"] == "plan_v1"
    assert attributes["subscription_status"] == "active"
    assert status[0]["payload"]["attributes"]["previous_status"] is None
    assert status[0]["payload"]["attributes"]["source_event_id"] == "evt_01"


def test_status_inalterado_audita_somente_mudanca_de_entitlement():
    with projector_env() as env:
        env.accept("evt_01")
        env.accept("evt_02")
        env.projector().process("evt_01")
        env.projector().process("evt_02")
        changed = env.audit_rows("entitlement.changed", "evt_02", 2)
        status = env.audit_rows("subscription.status_changed", "evt_02", 2)
    assert len(changed) == 1
    assert changed[0]["payload"]["attributes"]["previous_version"] == 1
    assert status == []


def test_status_alterado_audita_status_anterior():
    with projector_env() as env:
        env.accept("evt_01")
        env.accept("evt_02", "customer.subscription.deleted")
        env.projector().process("evt_01")
        env.stripe.get_current_state.return_value = make_state(
            subscription_status=SubscriptionStatus.CANCELED
        )
        env.projector().process("evt_02")
        status = env.audit_rows("subscription.status_changed", "evt_02", 2)
    attributes = status[0]["payload"]["attributes"]
    assert attributes["previous_status"] == "active"
    assert attributes["subscription_status"] == "canceled"
