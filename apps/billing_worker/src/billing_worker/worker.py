"""Execução dos ciclos limitados do worker de billing."""

from dataclasses import dataclass
from typing import Protocol

from cnes_domain.billing.inbox import (
    ReconciliationRequest,
    ReconciliationResult,
    RecoveryRequest,
    RecoveryResult,
    ReservationRecoveryRequest,
    ReservationRecoveryResult,
)
from cnes_domain.billing.ports import BillingMetricsPort, ClockPort
from cnes_infra.billing.metrics import BillingMetricName, billing_metric
from cnes_infra.billing.revocation_sweep import RevocationSweepResult


class RecoveryRunner(Protocol):
    def drain_inbox(self, limit: int) -> RecoveryResult: ...  # pragma: no cover

    def run(self, request: RecoveryRequest) -> RecoveryResult: ...  # pragma: no cover


class ReconcileRunner(Protocol):
    def run(self, request: ReconciliationRequest) -> ReconciliationResult: ...  # pragma: no cover


class SweepRunner(Protocol):
    def run(self, request: ReconciliationRequest) -> RevocationSweepResult: ...  # pragma: no cover


class ReservationRecoveryRunner(Protocol):
    def reconcile_expired_reservations(
        self, request: ReservationRecoveryRequest
    ) -> ReservationRecoveryResult: ...  # pragma: no cover


@dataclass(frozen=True, slots=True)
class WorkerJobs:
    recovery: RecoveryRunner
    request: RecoveryRequest
    reconciler: ReconcileRunner
    revocations: SweepRunner | None
    reservations: ReservationRecoveryRunner
    metrics: BillingMetricsPort
    clock: ClockPort


class BillingWorker:
    def __init__(self, jobs: WorkerJobs) -> None:
        self._jobs = jobs

    def run_inbox(self, limit: int) -> RecoveryResult:
        """Args: limit: Máximo de eventos vencidos do inbox neste ciclo.
        Returns: Resultado do dreno limitado; emite RecoveryBacklog.
        Raises: BillingError: Falha de dependência ou permanente.
        """
        result = self._jobs.recovery.drain_inbox(limit)
        backlog = max(result.scanned - result.reprocessed, 0)
        self._emit(BillingMetricName.RECOVERY_BACKLOG, backlog)
        return result

    def run_recover(self) -> RecoveryResult:
        """Returns: Resultado de uma página de recovery pelo cursor de eventos Stripe.
        Raises: RetryableBillingError: Página não resolvida ou sem progresso.
        """
        return self._jobs.recovery.run(self._jobs.request)

    def run_reconcile(self, limit: int) -> ReconciliationResult:
        """Args: limit: Máximo de contas reconciliadas neste ciclo.
        Returns: Contadores e próximo cursor persistido.
        Raises: RetryableBillingError: Cursor disputado.
        """
        return self._jobs.reconciler.run(ReconciliationRequest(limit, None))

    def run_revoke_pending(self, limit: int) -> RevocationSweepResult | None:
        """Args: limit: Máximo de contas varridas neste ciclo.
        Returns: Contadores da retomada, ou None sem enforcement (modo off).
        Raises: RetryableBillingError: Cursor disputado.
        """
        if self._jobs.revocations is None:
            return None
        return self._jobs.revocations.run(ReconciliationRequest(limit, None))

    def run_release_expired(self, limit: int) -> ReservationRecoveryResult:
        """Args: limit: Máximo de reservas vencidas examinadas neste ciclo.
        Returns: Reservas examinadas e liberadas; emite QuotaReservationsExpired.
        Raises: BillingDependencyError, PermanentBillingError.
        """
        request = ReservationRecoveryRequest(self._jobs.clock(), limit, None)
        result = self._jobs.reservations.reconcile_expired_reservations(request)
        self._emit(BillingMetricName.QUOTA_RESERVATIONS_EXPIRED, result.released)
        return result

    def _emit(self, name: BillingMetricName, value: int) -> None:
        self._jobs.metrics.emit(billing_metric(name, value, self._jobs.clock()))
