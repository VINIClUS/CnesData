"""Testes unitários do WebhookRecovery (BIL-021) com portas dublês nas bordas."""

from datetime import timedelta
from typing import Any
from unittest.mock import Mock

import pytest

from cnes_domain.billing.errors import RetryableBillingError
from cnes_domain.billing.inbox import (
    InboxAcceptResult,
    InboxDisposition,
    InboxProcessingState,
    InboxRecoveryRecord,
    ProjectionResult,
    RecoveryRequest,
    StripeRecoveryCursor,
)
from cnes_infra.billing.keys import stripe_recovery_due_sort_key
from cnes_infra.billing.recovery import RecoveryDependencies, WebhookRecovery
from packages.cnes_infra.tests.billing.billing_factories import NOW
from packages.cnes_infra.tests.billing.test_recovery import page, stripe_event
from packages.cnes_infra.tests.contracts.clock import MutableClock

STATE = InboxProcessingState
CURSOR = StripeRecoveryCursor("cycle-01", NOW - timedelta(hours=72), None, 1)
OTHER = StripeRecoveryCursor("cycle-other", NOW - timedelta(hours=10), None, 4)


def active(state: InboxProcessingState, event_id: str, due_at: Any, key: str | None = None):
    due_key = key or stripe_recovery_due_sort_key(due_at, event_id)
    return InboxRecoveryRecord(state, 1, due_at, due_key)


def terminal(state: InboxProcessingState) -> InboxRecoveryRecord:
    return InboxRecoveryRecord(state, 1, None, None)


def applied(event_id: str) -> ProjectionResult:
    return ProjectionResult(event_id, True, 1)


def build(records: Any = None) -> tuple[WebhookRecovery, Mock, Mock, Mock, Mock]:
    inbox, projector, stripe, cursor = Mock(), Mock(), Mock(), Mock()
    inbox.list_recoverable.return_value = ()
    inbox.get_recovery_record.side_effect = records or (lambda *_: terminal(STATE.PROCESSED))
    inbox.accept.return_value = InboxAcceptResult("evt_a", InboxDisposition.ACCEPTED)
    cursor.load.return_value = CURSOR
    cursor.advance.return_value = True
    stripe.list_events.return_value = page()
    deps = RecoveryDependencies(inbox, projector, stripe, cursor, MutableClock(NOW).now)
    return WebhookRecovery(deps), inbox, projector, stripe, cursor


def test_lote_cheio_do_inbox_nao_consulta_stripe_nem_cursor():
    recovery, inbox, projector, stripe, cursor = build()
    inbox.list_recoverable.return_value = (stripe_event("evt_a"), stripe_event("evt_b"))
    projector.process.side_effect = [applied("evt_a"), applied("evt_b")]
    result = recovery.run(RecoveryRequest(72, 2))
    assert (result.scanned, result.reprocessed, result.next_cursor) == (2, 2, None)
    stripe.list_events.assert_not_called()
    assert cursor.method_calls == []


def test_pre_passe_nao_liquidado_retorna_sem_consultar_stripe():
    pending = active(STATE.PENDING, "evt_a", NOW)
    recovery, inbox, projector, stripe, cursor = build(lambda *_: pending)
    inbox.list_recoverable.return_value = (stripe_event("evt_a"),)
    projector.process.return_value = ProjectionResult("evt_a", False, None)
    result = recovery.run(RecoveryRequest(72, 100))
    assert result.scanned == 1
    stripe.list_events.assert_not_called()
    assert cursor.method_calls == []


def test_drain_inbox_retorna_contadores_e_cursor_nulo():
    handoff = active(STATE.FAILED_RETRYABLE, "evt_b", NOW + timedelta(seconds=30))
    records = {"evt_a": terminal(STATE.PROCESSED), "evt_b": handoff}
    recovery, inbox, projector, _, _ = build(lambda event_id, _: records[event_id])
    inbox.list_recoverable.return_value = (stripe_event("evt_a"), stripe_event("evt_b"))
    projector.process.side_effect = [applied("evt_a"), RetryableBillingError("stripe_down")]
    result = recovery.drain_inbox(100)
    assert (result.scanned, result.imported, result.reprocessed) == (2, 0, 1)
    assert (result.failed, result.next_cursor) == (1, None)


def test_falha_final_do_inbox_conta_como_failed():
    recovery, inbox, projector, _, _ = build(lambda *_: terminal(STATE.FAILED_FINAL))
    inbox.list_recoverable.return_value = (stripe_event("evt_a"),)
    projector.process.return_value = ProjectionResult("evt_a", False, None)
    assert recovery.drain_inbox(100).failed == 1


def test_registro_ausente_apos_tentativa_e_nao_liquidado():
    recovery, inbox, projector, stripe, _ = build(lambda *_: None)
    inbox.list_recoverable.return_value = (stripe_event("evt_a"),)
    projector.process.return_value = ProjectionResult("evt_a", False, None)
    recovery.run(RecoveryRequest(72, 100))
    stripe.list_events.assert_not_called()


@pytest.mark.parametrize(
    "record",
    [
        active(STATE.FAILED_RETRYABLE, "evt_a", NOW + timedelta(seconds=30), "x#evt_a"),
        active(STATE.FAILED_RETRYABLE, "evt_a", NOW),
    ],
    ids=["chave_divergente", "vencido_sem_handoff"],
)
def test_failed_retryable_sem_handoff_valido_nao_e_liquidado(record):
    recovery, inbox, projector, stripe, _ = build(lambda *_: record)
    inbox.list_recoverable.return_value = (stripe_event("evt_a"),)
    projector.process.return_value = ProjectionResult("evt_a", False, None)
    result = recovery.run(RecoveryRequest(72, 100))
    assert result.failed == 0
    stripe.list_events.assert_not_called()


def test_excecao_desconhecida_do_projetor_propaga_em_drain():
    recovery, inbox, projector, _, _ = build()
    inbox.list_recoverable.return_value = (stripe_event("evt_a"),)
    projector.process.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom"):
        recovery.drain_inbox(100)


def test_start_perdido_recarrega_ciclo_do_outro_worker():
    recovery, _, _, stripe, cursor = build()
    cursor.load.side_effect = [None, OTHER]
    cursor.start.return_value = False
    recovery.run(RecoveryRequest(72, 100))
    request = stripe.list_events.call_args.args[0]
    assert request.created_gte == OTHER.created_gte
    cursor.complete.assert_called_once_with(OTHER, NOW)


def test_start_perdido_sem_ciclo_recarregado_e_conflito():
    recovery, _, _, _, cursor = build()
    cursor.load.side_effect = [None, None]
    cursor.start.return_value = False
    with pytest.raises(RetryableBillingError, match="stripe_cursor_conflict"):
        recovery.run(RecoveryRequest(72, 100))
    cursor.complete.assert_not_called()


def test_advance_perdido_nao_sobrescreve_cursor():
    recovery, _, _, stripe, cursor = build()
    stripe.list_events.return_value = page("evt_a", has_more=True)
    cursor.advance.return_value = False
    result = recovery.run(RecoveryRequest(72, 100))
    assert result.next_cursor is None
    assert result.imported == 1
    cursor.advance.assert_called_once_with(CURSOR, CURSOR.advance("evt_a"))
    cursor.complete.assert_not_called()
