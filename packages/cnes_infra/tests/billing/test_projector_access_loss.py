"""Testes da delegação do projetor à revogação imediata em perda efetiva de acesso."""

import logging
from datetime import timedelta
from typing import Any

from cnes_domain.billing.errors import RetryableBillingError
from cnes_domain.billing.models import SubscriptionStatus
from cnes_domain.billing.revocation import RevocationResult
from cnes_infra.billing.dynamodb_items import encode_snapshot
from cnes_infra.billing.projector import PROJECTION_ACTOR_ID
from packages.cnes_infra.tests.billing.billing_factories import TABLE_NAME, make_snapshot
from packages.cnes_infra.tests.billing.test_projector import (
    ACCOUNT_ID,
    make_state,
    projector_env,
)

CANCELED = SubscriptionStatus.CANCELED


class SpyEnforcer:
    def __init__(self, error: Exception | None = None, fenced: tuple[str, ...] = ()) -> None:
        self.calls: list[tuple[Any, str]] = []
        self.error = error
        self.fenced = fenced

    def enforce_access_loss(self, snapshot: Any, actor_id: str) -> RevocationResult:
        self.calls.append((snapshot, actor_id))
        if self.error is not None:
            raise self.error
        return RevocationResult(snapshot.entitlement_version, self.fenced, ())


class SpyMetrics:
    def __init__(self) -> None:
        self.emitted: list[Any] = []

    def emit(self, metric: Any) -> None:
        self.emitted.append(metric)

    def resume_pending(self, billing_account_id: str, actor_id: str) -> RevocationResult | None:
        raise AssertionError("resume_not_expected")


def _run_transition(env, enforcer, state, advance=timedelta(0)):
    env.accept("evt_01")
    env.accept("evt_02")
    env.projector().process("evt_01")
    env.clock.advance(advance)
    env.stripe.get_current_state.return_value = state
    return env.projector(enforcer=enforcer).process("evt_02")


def test_ativo_para_cancelado_delega_uma_vez_com_o_snapshot_gravado():
    spy = SpyEnforcer()
    with projector_env() as env:
        result = _run_transition(env, spy, make_state(subscription_status=CANCELED))
        stored = env.snapshot()
    assert result.applied is True
    assert spy.calls == [(stored, PROJECTION_ACTOR_ID)]
    assert stored.entitlement_version == 2


def test_cancelamento_agendado_antes_do_fim_do_periodo_nao_delega():
    spy = SpyEnforcer()
    with projector_env() as env:
        _run_transition(env, spy, make_state(cancel_at_period_end=True))
        stored = env.snapshot()
    assert stored.entitlement_version == 2
    assert spy.calls == []


def test_cancelamento_agendado_apos_o_fim_do_periodo_delega():
    spy = SpyEnforcer()
    with projector_env() as env:
        _run_transition(env, spy, make_state(cancel_at_period_end=True), timedelta(days=31))
    assert len(spy.calls) == 1


def test_full_para_full_nao_delega():
    spy = SpyEnforcer()
    with projector_env() as env:
        _run_transition(env, spy, make_state(active_features=frozenset({"other"})))
    assert spy.calls == []


def test_primeiro_snapshot_nao_delega():
    spy = SpyEnforcer()
    with projector_env() as env:
        env.stripe.get_current_state.return_value = make_state(subscription_status=CANCELED)
        env.accept()
        result = env.projector(enforcer=spy).process("evt_01")
    assert result.applied is True
    assert spy.calls == []


def test_admin_revoked_anterior_nao_delega():
    spy = SpyEnforcer()
    with projector_env() as env:
        revoked = make_snapshot(ACCOUNT_ID, 1, subscription_status=SubscriptionStatus.ADMIN_REVOKED)
        env.client.put_item(TableName=TABLE_NAME, Item=encode_snapshot(revoked))
        env.stripe.get_current_state.return_value = make_state(subscription_status=CANCELED)
        env.accept()
        env.projector(enforcer=spy).process("evt_01")
    assert spy.calls == []


def test_sem_enforcer_nao_delega_e_aplica():
    with projector_env() as env:
        result = _run_transition(env, None, make_state(subscription_status=CANCELED))
    assert result.applied is True


def test_falha_retryable_do_enforcer_nao_desfaz_o_commit(caplog):
    spy = SpyEnforcer(RetryableBillingError("revocation_pending"))
    with projector_env() as env, caplog.at_level(logging.WARNING):
        result = _run_transition(env, spy, make_state(subscription_status=CANCELED))
        stored = env.snapshot()
    assert result.applied is True
    assert stored.subscription_status is CANCELED
    assert "stripe_projection_enforcement_failed event_id=evt_02" in caplog.text
    assert "code=revocation_pending" in caplog.text


def test_excecao_inesperada_do_enforcer_e_registrada_sem_desfazer_o_commit(caplog):
    spy = SpyEnforcer(ValueError("corrupt_run"))
    with projector_env() as env, caplog.at_level(logging.WARNING):
        result = _run_transition(env, spy, make_state(subscription_status=CANCELED))
        stored = env.snapshot()
    assert (result.applied, result.entitlement_version) == (True, 2)
    assert stored.subscription_status is CANCELED
    assert "stripe_projection_enforcement_failed event_id=evt_02" in caplog.text
    assert "code=ValueError" in caplog.text


def test_cancelamento_agendado_seguido_de_cancelado_apos_o_fim_do_periodo_delega():
    spy = SpyEnforcer()
    with projector_env() as env:
        env.accept("evt_01")
        env.accept("evt_02")
        env.stripe.get_current_state.return_value = make_state(cancel_at_period_end=True)
        env.projector().process("evt_01")
        env.clock.advance(timedelta(days=31))
        env.stripe.get_current_state.return_value = make_state(
            subscription_status=CANCELED, cancel_at_period_end=True,
        )
        env.projector(enforcer=spy).process("evt_02")
    assert len(spy.calls) == 1


def test_runs_fenceadas_pelo_projetor_emitem_metrica_de_cancelamento():
    spy, metrics = SpyEnforcer(fenced=("run-1", "run-2")), SpyMetrics()
    with projector_env() as env:
        env.accept("evt_01")
        env.accept("evt_02")
        env.projector().process("evt_01")
        env.stripe.get_current_state.return_value = make_state(subscription_status=CANCELED)
        env.projector(enforcer=spy, metrics=metrics).process("evt_02")
    [metric] = metrics.emitted
    assert (metric.name, metric.value) == ("RunsCanceledByRevocation", 2)
    assert dict(metric.dimensions) == {"Reason": "stripe_access_loss"}


def test_delegacao_sem_runs_fenceadas_nao_emite_metrica():
    spy, metrics = SpyEnforcer(), SpyMetrics()
    with projector_env() as env:
        env.accept("evt_01")
        env.accept("evt_02")
        env.projector().process("evt_01")
        env.stripe.get_current_state.return_value = make_state(subscription_status=CANCELED)
        env.projector(enforcer=spy, metrics=metrics).process("evt_02")
    assert len(spy.calls) == 1
    assert metrics.emitted == []


def test_past_due_alem_da_carencia_delega():
    spy = SpyEnforcer()
    with projector_env() as env:
        _run_transition(
            env, spy, make_state(subscription_status=SubscriptionStatus.PAST_DUE),
            timedelta(days=60),
        )
    assert len(spy.calls) == 1
