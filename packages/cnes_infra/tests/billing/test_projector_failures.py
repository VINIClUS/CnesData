"""Testes de classificação de falhas do StripeEventProjector (BIL-021)."""

from datetime import timedelta
from unittest.mock import Mock

import pytest

from cnes_domain.billing.errors import PermanentBillingError, RetryableBillingError
from cnes_domain.billing.inbox import InboxProcessingState
from cnes_domain.billing.models import ReadConsistency
from cnes_infra.billing.webhook_inbox_items import STRIPE_INBOX_MAX_ATTEMPTS
from packages.cnes_infra.tests.billing.test_projector import projector_env

_STRONG = ReadConsistency.STRONG
UNAVAILABLE = RetryableBillingError("stripe_unavailable")


@pytest.mark.parametrize(
    "error",
    [ValueError("x"), TypeError("x"), AttributeError("x")],
    ids=["value_error", "type_error", "attribute_error"],
)
def test_erro_de_dados_na_projecao_vira_falha_final_stripe_state_invalid(error):
    with projector_env() as env:
        env.stripe.get_current_state.side_effect = error
        env.accept()
        result = env.projector().process("evt_01")
        state = env.inbox_state()
        rows = env.failed_final_rows("evt_01", 1)
    assert result.applied is False
    assert state is InboxProcessingState.FAILED_FINAL
    assert len(rows) == 1
    assert rows[0]["payload"]["reason_code"] == "stripe_state_invalid"


def test_erro_de_dados_ao_ler_snapshot_vira_falha_final():
    with projector_env() as env:
        projection = Mock()
        projection.get_snapshot.side_effect = ValueError("x")
        env.accept()
        result = env.projector(projection=projection).process("evt_01")
        state = env.inbox_state()
    assert result.applied is False
    assert state is InboxProcessingState.FAILED_FINAL


def test_constante_de_tentativas_maximas_do_inbox():
    assert STRIPE_INBOX_MAX_ATTEMPTS == 75


def _fail_until(env, attempts: int) -> None:
    env.stripe.get_current_state.side_effect = UNAVAILABLE
    env.accept()
    for _ in range(attempts):
        with pytest.raises(RetryableBillingError):
            env.projector().process("evt_01")
        env.clock.advance(timedelta(hours=2))
    env.stripe.get_current_state.side_effect = None


def test_tentativas_esgotadas_viram_falha_final_sem_chamar_stripe():
    with projector_env() as env:
        _fail_until(env, STRIPE_INBOX_MAX_ATTEMPTS)
        calls = env.stripe.get_current_state.call_count
        result = env.projector().process("evt_01")
        state = env.inbox_state()
        rows = env.failed_final_rows("evt_01", STRIPE_INBOX_MAX_ATTEMPTS + 1)
        snapshot = env.snapshot()
    assert result.applied is False
    assert state is InboxProcessingState.FAILED_FINAL
    assert env.stripe.get_current_state.call_count == calls
    assert [row["payload"]["reason_code"] for row in rows] == ["inbox_attempts_exhausted"]
    assert snapshot is None


def test_tentativa_no_limite_ainda_projeta_normalmente():
    with projector_env() as env:
        _fail_until(env, STRIPE_INBOX_MAX_ATTEMPTS - 1)
        result = env.projector().process("evt_01")
        state = env.inbox_state()
    assert result.applied is True
    assert state is InboxProcessingState.PROCESSED


def test_stripe_request_rejected_e_tratado_como_retryable():
    with projector_env() as env:
        rejected = PermanentBillingError("stripe_request_rejected")
        env.stripe.get_current_state.side_effect = rejected
        env.accept()
        with pytest.raises(RetryableBillingError) as raised:
            env.projector().process("evt_01")
        record = env.inbox.get_recovery_record("evt_01", _STRONG)
        rows = env.failed_final_rows("evt_01", 1)
    assert raised.value.code == "stripe_request_rejected"
    assert raised.value.__cause__ is rejected
    assert record.state is InboxProcessingState.FAILED_RETRYABLE
    assert rows == []
