"""Testes dos ciclos limitados do BillingWorker."""

from apps.billing_worker.tests.support import NOW, RECONCILED, RELEASED, RESULT, SWEPT, make_worker
from cnes_domain.billing.inbox import (
    ReconciliationRequest,
    RecoveryRequest,
    RecoveryResult,
    ReservationRecoveryRequest,
)


def test_inbox_delega_ao_dreno_e_emite_backlog_nao_aplicado() -> None:
    worker, jobs = make_worker()
    assert worker.run_inbox(37) == RESULT
    jobs.recovery.drain_inbox.assert_called_once_with(37)
    assert jobs.metrics.values("RecoveryBacklog") == [1]


def test_backlog_nunca_e_negativo() -> None:
    worker, jobs = make_worker()
    jobs.recovery.drain_inbox.return_value = RecoveryResult(1, 0, 3, 0, None)
    worker.run_inbox(5)
    assert jobs.metrics.values("RecoveryBacklog") == [0]


def test_recover_delega_uma_vez_com_a_requisicao() -> None:
    worker, jobs = make_worker()
    assert worker.run_recover() == RESULT
    jobs.recovery.run.assert_called_once_with(RecoveryRequest(72, 100))


def test_reconcile_roda_um_lote_pelo_cursor_persistido() -> None:
    worker, jobs = make_worker()
    assert worker.run_reconcile(25) == RECONCILED
    jobs.reconciler.run.assert_called_once_with(ReconciliationRequest(25, None))


def test_revoke_pending_roda_uma_pagina_da_varredura() -> None:
    worker, jobs = make_worker()
    assert worker.run_revoke_pending(10) == SWEPT
    jobs.revocations.run.assert_called_once_with(ReconciliationRequest(10, None))


def test_revoke_pending_sem_enforcement_e_noop() -> None:
    worker, _jobs = make_worker(revocations=None)
    assert worker.run_revoke_pending(10) is None


def test_release_expired_usa_o_relogio_e_emite_reservas_expiradas() -> None:
    worker, jobs = make_worker()
    assert worker.run_release_expired(50) == RELEASED
    jobs.reservations.reconcile_expired_reservations.assert_called_once_with(
        ReservationRecoveryRequest(NOW, 50, None)
    )
    assert jobs.metrics.values("QuotaReservationsExpired") == [2]
