"""Testes do BillingReconciler: assinatura substituída, falhas por conta e retomada."""

import logging

import pytest

from cnes_domain.billing.errors import BillingDependencyError, RetryableBillingError
from cnes_domain.billing.inbox import ReconciliationRequest, ReconciliationResult
from cnes_domain.billing.models import SubscriptionStatus
from cnes_infra.billing.reconciliation import RECONCILER_ACTOR_ID
from packages.cnes_infra.tests.billing.billing_factories import make_snapshot
from packages.cnes_infra.tests.billing.reconciliation_support import (
    FEATURES,
    Env,
    make_env,
    make_state,
)

REQUEST = ReconciliationRequest(limit=10, cursor=None)
RUNS_METRIC = "RunsCanceledByRevocation"


def _run(env: Env) -> ReconciliationResult:
    return env.reconciler().run(REQUEST)


def test_assinatura_encerrada_e_substituida_usa_a_assinatura_viva():
    env = make_env()
    env.stripe.states = [
        make_state(subscription_status=SubscriptionStatus.CANCELED),
        make_state(stripe_subscription_id="sub_02"),
    ]
    result = _run(env)
    assert [r.stripe_subscription_id for r in env.stripe.requests] == ["sub_01", None]
    written = env.projection.snapshots["ba_01"]
    assert (written.stripe_subscription_id, written.subscription_status) == (
        "sub_02", SubscriptionStatus.ACTIVE,
    )
    assert (result.drift_found, result.corrected) == (1, 1)
    assert env.enforcer.calls == []


def test_assinatura_encerrada_sem_substituta_mantem_estado_encerrado():
    env = make_env()
    env.stripe.states = [
        make_state(subscription_status=SubscriptionStatus.CANCELED),
        RetryableBillingError("stripe_subscription_ambiguous"),
    ]
    result = _run(env)
    written = env.projection.snapshots["ba_01"]
    assert (written.stripe_subscription_id, written.subscription_status) == (
        "sub_01", SubscriptionStatus.CANCELED,
    )
    assert (result.corrected, result.failed) == (1, 0)
    assert len(env.enforcer.calls) == 1


def test_falha_da_busca_da_substituta_falha_a_conta():
    env = make_env()
    env.stripe.states = [
        make_state(subscription_status=SubscriptionStatus.CANCELED),
        BillingDependencyError("stripe_unavailable"),
    ]
    result = _run(env)
    assert (result.failed, result.corrected) == (1, 0)
    assert env.projection.cas_calls == 0


@pytest.mark.parametrize(
    "error", [ValueError("reason=x"), TypeError("x"), AttributeError("items")],
    ids=["value", "type", "attribute"],
)
def test_objeto_stripe_malformado_falha_a_conta_sem_derrubar_o_lote(error, caplog):
    env = make_env("ba_01", "ba_02", "ba_03")
    env.stripe.states = [make_state(), error, make_state()]
    with caplog.at_level(logging.WARNING):
        result = _run(env)
    assert (result.examined, result.failed) == (3, 1)
    assert env.cursor.saves == ["ba_01", "ba_02", "ba_03", None]
    assert "billing_account_id=ba_02 code=stripe_state_invalid" in caplog.text


def test_defeito_do_enforcer_nao_e_engolido():
    env = make_env()
    env.enforcer.error = AttributeError("bug")
    with pytest.raises(AttributeError):
        _run(env)
    assert env.cursor.saves == []


@pytest.mark.parametrize(
    "error",
    [RetryableBillingError("stripe_unavailable"), BillingDependencyError("dynamodb_unavailable")],
    ids=["stripe", "dynamodb"],
)
def test_indisponibilidade_de_dependencia_para_o_lote(error):
    env = make_env("ba_01", "ba_02")
    env.stripe.error = error
    result = _run(env)
    assert (result.examined, result.failed, result.next_cursor) == (1, 1, None)
    assert env.cursor.saves == []


def test_admin_revoked_nao_delega_enforcement_concorrente_com_revoke():
    env = make_env()
    env.projection.snapshots["ba_01"] = make_snapshot(
        subscription_status=SubscriptionStatus.ADMIN_REVOKED
    )
    _run(env)
    assert env.enforcer.calls == []


def test_acesso_pleno_retoma_revogacao_pendente():
    env = make_env()
    _run(env)
    assert env.enforcer.resumed == [("ba_01", RECONCILER_ACTOR_ID)]
    assert env.enforcer.calls == []


def test_perda_de_acesso_nao_usa_retomada_de_pendente():
    env = make_env()
    env.stripe.states = [make_state(subscription_status=SubscriptionStatus.UNPAID)]
    _run(env)
    assert env.enforcer.resumed == []
    assert len(env.enforcer.calls) == 1


def test_falha_na_retomada_pendente_conta_em_failed_e_avanca():
    env = make_env()
    env.enforcer.error = RetryableBillingError("revocation_progress_contended")
    result = _run(env)
    assert result.failed == 1
    assert env.cursor.saves == ["ba_01", None]

def test_retomada_que_fenceia_emite_metrica_de_cancelamento():
    env = make_env()
    env.enforcer.fenced = ("run-1",)
    _run(env)
    [metric] = env.metrics.named(RUNS_METRIC)
    assert metric.value == 1


def test_admin_revoked_com_assinatura_viva_na_stripe_e_drift_sem_correcao():
    env = make_env()
    env.projection.snapshots["ba_01"] = make_snapshot(
        subscription_status=SubscriptionStatus.ADMIN_REVOKED, features=FEATURES
    )
    result = _run(env)
    assert (result.drift_found, result.corrected) == (1, 0)
    assert env.projection.cas_calls == 0
    [event] = env.audit.events
    assert event.attributes["drift_fields"] == "subscription_status"
    assert event.attributes["subscription_status"] == "admin_revoked"


def test_admin_revoked_com_assinatura_encerrada_na_stripe_nao_e_drift():
    env = make_env()
    env.projection.snapshots["ba_01"] = make_snapshot(
        subscription_status=SubscriptionStatus.ADMIN_REVOKED, features=FEATURES
    )
    env.stripe.states = [make_state(subscription_status=SubscriptionStatus.CANCELED)]
    result = _run(env)
    assert result.drift_found == 0
    assert env.audit.events == []
