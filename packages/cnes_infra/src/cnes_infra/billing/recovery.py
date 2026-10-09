"""Recuperação limitada de webhooks Stripe com cursor durável e fila de retry."""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Protocol, cast
from uuid import uuid4

from cnes_domain.billing.errors import RetryableBillingError
from cnes_domain.billing.inbox import (
    InboxDisposition,
    InboxProcessingState,
    InboxRecoveryRecord,
    ProjectionResult,
    RecoveryRequest,
    RecoveryResult,
    StripeEventListRequest,
    StripeEventPage,
    StripeRecoveryCursor,
)
from cnes_domain.billing.models import ReadConsistency
from cnes_domain.billing.ports import (
    ClockPort,
    RecoveryCursorPort,
    StripeGatewayPort,
    WebhookInboxPort,
)
from cnes_infra.billing.keys import stripe_recovery_due_sort_key

STRIPE_RECOVERY_LOOKBACK_HOURS = 72
STRIPE_RECOVERY_BATCH_SIZE = 100

logger = logging.getLogger(__name__)

_STRONG = ReadConsistency.STRONG
_SETTLED = frozenset({InboxProcessingState.PROCESSED, InboxProcessingState.IGNORED})
_ATTEMPTABLE = frozenset({
    InboxProcessingState.PENDING,
    InboxProcessingState.FAILED_RETRYABLE,
    InboxProcessingState.PROCESSING,
})


class ProjectorPort(Protocol):
    """Porta do projetor de eventos Stripe usada pela recuperação."""

    def process(self, event_id: str) -> ProjectionResult:
        raise NotImplementedError


def _new_cycle_id() -> str:
    return uuid4().hex


@dataclass(frozen=True, slots=True)
class RecoveryDependencies:
    """Portas e fábrica de ciclo usadas pela recuperação de webhooks."""

    inbox: WebhookInboxPort
    projector: ProjectorPort
    stripe: StripeGatewayPort
    cursor: RecoveryCursorPort
    clock: ClockPort
    cycle_id_factory: Callable[[], str] = field(default=_new_cycle_id)


class _Verdict(Enum):
    SETTLED = auto()
    FAILED = auto()
    UNSETTLED = auto()


@dataclass(slots=True)
class _Tally:
    scanned: int = 0
    imported: int = 0
    reprocessed: int = 0
    failed: int = 0
    unsettled: int = 0

    def result(self, next_cursor: str | None) -> RecoveryResult:
        return RecoveryResult(
            self.scanned, self.imported, self.reprocessed, self.failed, next_cursor,
        )


def _classify(
    record: InboxRecoveryRecord | None, event_id: str, now: datetime,
) -> _Verdict:
    if record is None:
        return _Verdict.UNSETTLED
    if record.state in _SETTLED:
        return _Verdict.SETTLED
    if record.state is InboxProcessingState.FAILED_FINAL:
        return _Verdict.FAILED
    if record.state is not InboxProcessingState.FAILED_RETRYABLE:
        return _Verdict.UNSETTLED
    due_at = cast("datetime", record.due_at)
    expected_key = stripe_recovery_due_sort_key(due_at, event_id)
    if due_at > now and record.due_index_key == expected_key:
        return _Verdict.FAILED
    return _Verdict.UNSETTLED


class WebhookRecovery:
    """Recupera webhooks Stripe pela fila do inbox e pelo cursor de eventos."""

    def __init__(self, dependencies: RecoveryDependencies) -> None:
        self._deps = dependencies

    def drain_inbox(self, limit: int) -> RecoveryResult:
        """Reprocessa eventos vencidos da fila durável do inbox.

        Args:
            limit: Máximo de eventos vencidos a tentar.
        Returns:
            Contadores do sweep; next_cursor sempre None.
        """
        return self._drain(limit).result(None)

    def run(self, request: RecoveryRequest) -> RecoveryResult:
        """Drena a fila e processa uma página do ciclo de recuperação.

        Args: Janela de lookback e tamanho de página.
        Returns: Contadores e cursor seguinte (None ao concluir o ciclo).
        Raises: RetryableBillingError se página não liquidada, sem progresso ou conflito.
        """
        tally = self._drain(request.batch_size)
        if tally.unsettled or tally.scanned == request.batch_size:
            return tally.result(None)
        cursor = self._active_cursor(request)
        page = self._deps.stripe.list_events(
            StripeEventListRequest(cursor.created_gte, cursor.starting_after, request.batch_size)
        )
        self._import_page(page, tally)
        return self._move_cursor(cursor, page, tally)

    def _drain(self, limit: int) -> _Tally:
        tally = _Tally()
        events = self._deps.inbox.list_recoverable(self._deps.clock(), limit)
        tally.scanned = len(events)
        for event in events:
            self._attempt(event.event_id, tally)
            self._settle(event.event_id, tally)
        return tally

    def _attempt(self, event_id: str, tally: _Tally) -> None:
        try:
            result = self._deps.projector.process(event_id)
        except RetryableBillingError:
            return
        if result.applied:
            tally.reprocessed += 1

    def _settle(self, event_id: str, tally: _Tally) -> None:
        record = self._deps.inbox.get_recovery_record(event_id, _STRONG)
        verdict = _classify(record, event_id, self._deps.clock())
        if verdict is _Verdict.FAILED:
            tally.failed += 1
        elif verdict is _Verdict.UNSETTLED:
            tally.unsettled += 1

    def _active_cursor(self, request: RecoveryRequest) -> StripeRecoveryCursor:
        cursor = self._deps.cursor.load(_STRONG)
        if cursor is not None:
            return cursor
        lookback = timedelta(hours=request.lookback_hours)
        fresh = StripeRecoveryCursor(
            self._deps.cycle_id_factory(), self._deps.clock() - lookback, None, 1,
        )
        if self._deps.cursor.start(fresh):
            return fresh
        return self._reload_after_lost_start()

    def _reload_after_lost_start(self) -> StripeRecoveryCursor:
        cursor = self._deps.cursor.load(_STRONG)
        if cursor is None:
            raise RetryableBillingError("stripe_cursor_conflict")
        return cursor

    def _import_page(self, page: StripeEventPage, tally: _Tally) -> None:
        tally.scanned += len(page.events)
        unsettled_before = tally.unsettled
        for event in page.events:
            disposition = self._deps.inbox.accept(event).disposition
            if disposition is InboxDisposition.ACCEPTED:
                tally.imported += 1
            if disposition is not InboxDisposition.IGNORED:
                self._attempt_if_open(event.event_id, tally)
                self._settle(event.event_id, tally)
        if tally.unsettled > unsettled_before:
            raise RetryableBillingError("stripe_recovery_page_unsettled")

    def _attempt_if_open(self, event_id: str, tally: _Tally) -> None:
        record = self._deps.inbox.get_recovery_record(event_id, _STRONG)
        if record is not None and record.state in _ATTEMPTABLE:
            self._attempt(event_id, tally)

    def _move_cursor(
        self, cursor: StripeRecoveryCursor, page: StripeEventPage, tally: _Tally,
    ) -> RecoveryResult:
        if not page.has_more:
            self._deps.cursor.complete(cursor, self._deps.clock())
            return tally.result(None)
        last_id = page.events[-1].event_id if page.events else None
        if last_id is None or last_id == cursor.starting_after:
            raise RetryableBillingError("stripe_cursor_not_progressing")
        if not self._deps.cursor.advance(cursor, cursor.advance(last_id)):
            return tally.result(None)
        logger.info("stripe_recovery_advanced cycle_id=%s after=%s", cursor.cycle_id, last_id)
        return tally.result(last_id)
