"""Testes do StripeEventProjector (BIL-021): assinaturas concorrentes e ancoragem de grace."""

from datetime import timedelta

import pytest

from cnes_domain.billing.commands import StripeStateRequest
from cnes_domain.billing.inbox import InboxProcessingState
from cnes_domain.billing.models import SubscriptionStatus
from packages.cnes_infra.tests.billing.billing_factories import NOW
from packages.cnes_infra.tests.billing.test_projector import CUSTOMER, make_state, projector_env

ACTIVE = SubscriptionStatus.ACTIVE
CANCELED = SubscriptionStatus.CANCELED
PAST_DUE = SubscriptionStatus.PAST_DUE
EXPIRED = SubscriptionStatus.INCOMPLETE_EXPIRED
MONTH = timedelta(days=30)


def answer_by_subscription(env, states):
    def answer(request):
        return states[request.stripe_subscription_id]

    env.stripe.get_current_state.side_effect = answer


def process(env, event_id, event_type, subscription):
    env.accept(event_id, event_type, subscription)
    return env.projector().process(event_id)


@pytest.mark.parametrize(
    ("event_type", "old_status"),
    [
        ("customer.subscription.deleted", CANCELED),
        ("invoice.payment_failed", EXPIRED),
    ],
)
def test_evento_tardio_de_assinatura_encerrada_nao_substitui_a_atual(event_type, old_status):
    with projector_env() as env:
        answer_by_subscription(
            env,
            {
                "sub_new": make_state(stripe_subscription_id="sub_new"),
                "sub_old": make_state(
                    stripe_subscription_id="sub_old", subscription_status=old_status
                ),
            },
        )
        process(env, "evt_new", "customer.subscription.created", "sub_new")
        result = process(env, "evt_old", event_type, "sub_old")
        snapshot = env.snapshot()
        state = env.inbox_state("evt_old")
    assert result.applied is True
    assert state is InboxProcessingState.PROCESSED
    assert snapshot.stripe_subscription_id == "sub_new"
    assert snapshot.subscription_status is ACTIVE
    assert snapshot.source_event_id == "evt_old"
    requested = [call.args[0] for call in env.stripe.get_current_state.call_args_list]
    assert requested[-1] == StripeStateRequest(CUSTOMER, "sub_new")


def test_nova_assinatura_ativa_substitui_a_assinatura_encerrada():
    with projector_env() as env:
        answer_by_subscription(
            env,
            {
                "sub_old": make_state(
                    stripe_subscription_id="sub_old", subscription_status=CANCELED
                ),
                "sub_new": make_state(stripe_subscription_id="sub_new"),
            },
        )
        process(env, "evt_old", "customer.subscription.deleted", "sub_old")
        process(env, "evt_new", "customer.subscription.created", "sub_new")
        snapshot = env.snapshot()
    assert snapshot.stripe_subscription_id == "sub_new"
    assert snapshot.subscription_status is ACTIVE
    assert env.stripe.get_current_state.call_count == 2


def test_evento_so_de_customer_usa_a_assinatura_do_snapshot_atual():
    with projector_env() as env:
        answer_by_subscription(
            env,
            {
                "sub_old": make_state(
                    stripe_subscription_id="sub_old", subscription_status=CANCELED
                ),
                None: make_state(stripe_subscription_id="sub_other"),
            },
        )
        process(env, "evt_del", "customer.subscription.deleted", "sub_old")
        result = process(env, "evt_ent", "entitlements.active_entitlement_summary.updated", None)
        snapshot = env.snapshot()
        state = env.inbox_state("evt_ent")
    last = env.stripe.get_current_state.call_args.args[0]
    assert last == StripeStateRequest(CUSTOMER, "sub_old")
    assert result.applied is True
    assert state is InboxProcessingState.PROCESSED
    assert snapshot.stripe_subscription_id == "sub_old"
    assert snapshot.subscription_status is CANCELED


def past_due_renewals(env, shift):
    states = [
        make_state(
            subscription_status=PAST_DUE,
            period_start=NOW + shift * index,
            period_end=NOW + shift * index + MONTH,
        )
        for index in range(3)
    ]
    env.stripe.get_current_state.side_effect = states
    process(env, "evt_01", "invoice.payment_failed", "sub_01")
    anchored = env.snapshot().grace_until
    process(env, "evt_02", "invoice.payment_failed", "sub_01")
    process(env, "evt_03", "invoice.payment_failed", "sub_01")
    return anchored, env.snapshot()


def test_grace_nao_desloca_em_renovacoes_consecutivas_inadimplentes():
    with projector_env() as env:
        anchored, snapshot = past_due_renewals(env, timedelta(days=3))
    assert anchored is not None
    assert anchored > NOW
    assert snapshot.grace_until == anchored
    assert snapshot.period_start == NOW + timedelta(days=6)
    assert snapshot.entitlement_version == 3


def test_grace_ancorado_nao_fica_anterior_ao_inicio_do_periodo_renovado():
    with projector_env() as env:
        anchored, snapshot = past_due_renewals(env, MONTH)
    assert snapshot.period_start == NOW + 2 * MONTH
    assert snapshot.grace_until == snapshot.period_start
    assert snapshot.grace_until > anchored
    assert snapshot.entitlement_version == 3


def test_inadimplencia_apos_periodo_ativo_inicia_novo_grace():
    with projector_env() as env:
        late = make_state(
            subscription_status=PAST_DUE, period_start=NOW + MONTH, period_end=NOW + 2 * MONTH
        )
        env.stripe.get_current_state.side_effect = [make_state(), late]
        process(env, "evt_01", "customer.subscription.created", "sub_01")
        process(env, "evt_02", "invoice.payment_failed", "sub_01")
        snapshot = env.snapshot()
    assert snapshot.grace_until is not None
    assert snapshot.grace_until > NOW + MONTH


def test_grace_reinicia_quando_a_assinatura_inadimplente_muda():
    with projector_env() as env:
        first = make_state(subscription_status=PAST_DUE)
        other = make_state(
            stripe_subscription_id="sub_new",
            subscription_status=PAST_DUE,
            period_start=NOW + MONTH,
            period_end=NOW + 2 * MONTH,
        )
        env.stripe.get_current_state.side_effect = [first, other]
        process(env, "evt_01", "invoice.payment_failed", "sub_01")
        anchored = env.snapshot().grace_until
        process(env, "evt_02", "invoice.payment_failed", "sub_new")
        snapshot = env.snapshot()
    assert snapshot.stripe_subscription_id == "sub_new"
    assert snapshot.grace_until == anchored + MONTH
