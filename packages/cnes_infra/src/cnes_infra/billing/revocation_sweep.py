"""Varredura limitada de revogações pendentes por conta, retomável por cursor."""

import logging
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from cnes_domain.billing.errors import (
    BillingDependencyError,
    BillingError,
    RetryableBillingError,
)
from cnes_domain.billing.inbox import ReconciliationRequest
from cnes_domain.billing.models import BillingAccount
from cnes_domain.billing.ports import BillingCatalogPort, BillingMetricsPort, ClockPort
from cnes_domain.billing.revocation import RevocationResult
from cnes_domain.billing.validation import optional_id, require_non_negative
from cnes_infra.billing.metrics import BillingMetricName, billing_metric
from cnes_infra.billing.reconciliation_cursor import ReconciliationCursor

__all__ = [
    "REVOCATION_SWEEP_ACTOR_ID",
    "PendingRevocationPort",
    "RevocationSweep",
    "RevocationSweepDependencies",
    "RevocationSweepResult",
    "SweepCursorPort",
]

REVOCATION_SWEEP_ACTOR_ID = "system:revoke_pending"
_RESUMED_REASON = "revocation_resumed"

logger = logging.getLogger(__name__)


@runtime_checkable
class PendingRevocationPort(Protocol):
    def resume_pending(self, billing_account_id: str, actor_id: str) -> RevocationResult | None:
        raise NotImplementedError


@runtime_checkable
class SweepCursorPort(Protocol):
    def load(self) -> ReconciliationCursor:
        raise NotImplementedError

    def save(
        self, expected: ReconciliationCursor, position: str | None
    ) -> ReconciliationCursor | None:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class RevocationSweepDependencies:
    """Portas usadas pela varredura de revogações pendentes."""

    catalog: BillingCatalogPort
    cursor: SweepCursorPort
    enforcer: PendingRevocationPort
    metrics: BillingMetricsPort
    clock: ClockPort


@dataclass(frozen=True, slots=True)
class RevocationSweepResult:
    examined: int
    resumed: int
    fenced: int
    failed_publications: int
    failed: int
    next_cursor: str | None

    def __post_init__(self) -> None:
        for name in ("examined", "resumed", "fenced", "failed_publications", "failed"):
            require_non_negative(getattr(self, name), name)
        optional_id(self.next_cursor, "next_cursor")


@dataclass(slots=True)
class _Tally:
    examined: int = 0
    resumed: int = 0
    fenced: int = 0
    failed_publications: int = 0
    failed: int = 0


@dataclass(slots=True)
class _Sweep:
    stored: ReconciliationCursor
    position: str | None
    tally: _Tally = field(default_factory=_Tally)


class RevocationSweep:
    """Retoma o progresso de revogação incompleto das contas, uma página por ciclo."""

    def __init__(self, dependencies: RevocationSweepDependencies) -> None:
        self._deps = dependencies

    def run(self, request: ReconciliationRequest) -> RevocationSweepResult:
        """Retoma as revogações pendentes de uma página de contas.

        Args: Limite da página e cursor opcional que prevalece sobre o persistido.
        Returns: Contadores e próximo cursor (None conclui o ciclo).
        Raises: RetryableBillingError: cursor disputado; defeitos inesperados propagam.
        """
        stored = self._deps.cursor.load()
        position = request.cursor if request.cursor is not None else stored.position
        page = self._deps.catalog.list_stripe_accounts(request.limit, position)
        sweep = _Sweep(stored, position)
        for account in page.accounts:
            try:
                self._resume_or_fail(account, sweep.tally)
            except BillingDependencyError:
                return self._finish(sweep, sweep.position)
            self._advance(sweep, account.billing_account_id)
        self._advance(sweep, page.next_cursor)
        return self._finish(sweep, page.next_cursor)

    def _resume_or_fail(self, account: BillingAccount, tally: _Tally) -> None:
        tally.examined += 1
        account_id = account.billing_account_id
        try:
            result = self._deps.enforcer.resume_pending(account_id, REVOCATION_SWEEP_ACTOR_ID)
        except BillingError as error:
            tally.failed += 1
            logger.warning(
                "billing_revoke_pending_failed billing_account_id=%s code=%s",
                account_id,
                error.code,
            )
            if isinstance(error, BillingDependencyError):
                raise
            return
        if result is not None:
            tally.resumed += 1
            tally.fenced += len(result.fenced_run_ids)
            tally.failed_publications += len(result.failed_run_ids)

    def _advance(self, sweep: _Sweep, position: str | None) -> None:
        saved = self._deps.cursor.save(sweep.stored, position)
        if saved is None:
            raise RetryableBillingError("revocation_sweep_cursor_contended")
        sweep.stored = saved
        sweep.position = position

    def _finish(self, sweep: _Sweep, next_cursor: str | None) -> RevocationSweepResult:
        tally = sweep.tally
        if tally.fenced:
            self._deps.metrics.emit(
                billing_metric(
                    BillingMetricName.RUNS_CANCELED_BY_REVOCATION,
                    tally.fenced,
                    self._deps.clock(),
                    {"Reason": _RESUMED_REASON},
                )
            )
        logger.info(
            "billing_revoke_pending_completed examined=%d resumed=%d fenced=%d "
            "failed_publications=%d failed=%d",
            tally.examined,
            tally.resumed,
            tally.fenced,
            tally.failed_publications,
            tally.failed,
        )
        return RevocationSweepResult(
            tally.examined,
            tally.resumed,
            tally.fenced,
            tally.failed_publications,
            tally.failed,
            next_cursor,
        )
