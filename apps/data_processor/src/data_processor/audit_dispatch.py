"""Fronteira pública do despacho dedicado de auditoria."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import timedelta
from typing import TYPE_CHECKING, cast

from cnes_domain.orchestration.mode_canary import is_mode_canary_namespace
from cnes_domain.outbox_dispatcher import dispatch_once

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.control_plane.entities import OutboxEvent
    from cnes_domain.control_plane.outbox import OutboxCursor, OutboxPage
    from cnes_domain.orchestration.mode_canary import AuditObserveLocator
    from cnes_domain.outbox_dispatcher import DispatchResult
    from cnes_domain.ports.audit import AuditSinkPort
    from cnes_domain.ports.control_plane import ControlPlanePort
    from data_processor.aws_composition import AwsAuditRuntime

logger = logging.getLogger(__name__)


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _cursor(cursor: OutboxCursor | None) -> str:
    if cursor is None:
        return "none"
    body = json.dumps({"position": cursor.position, "version": cursor.version},
                      sort_keys=True, separators=(",", ":"))
    return _digest(body)


def _flag(value: bool) -> str:
    return "true" if value else "false"


class _Observation:
    def __init__(self, locator: AuditObserveLocator | None, retention_days: int | None) -> None:
        self.locator = locator
        self.retention_days = retention_days
        self.page: OutboxPage | None = None
        self.cas = "not_attempted"
        self.appended: set[str] = set()
        self.delivered: set[str] = set()

    def bound(self) -> tuple[OutboxEvent, ...]:
        events = () if self.page is None else self.page.events
        return tuple(event for event in events if is_mode_canary_namespace(event.tenant_id))

    def emit(self, result: DispatchResult | None) -> None:
        bound = self.bound()
        if self.locator is None and not bound:
            return
        for event in bound:
            self._emit_event(event)
        self._emit_cursor(bound, result)

    def _emit_event(self, event: OutboxEvent) -> None:
        body = json.dumps(event.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        created = event.created_at
        retain = "none"
        if self.retention_days is not None:
            retain = (created + timedelta(days=self.retention_days)).isoformat()
        logger.info(
            "mode_canary_audit_event event_id=%s tenant_id=%s event_sha256=%s identity_key=%s "
            "object_key=%s retention_mode=COMPLIANCE retain_until=%s appended=%s delivered=%s",
            event.event_id, event.tenant_id, _digest(body),
            f"audit/.event-id/{event.event_id}.json",
            f"audit/{event.tenant_id}/{created:%Y}/{created:%m}/{created:%d}/{event.event_id}.json",
            retain, _flag(event.event_id in self.appended),
            _flag(event.event_id in self.delivered),
        )

    def _emit_cursor(self, bound: tuple[OutboxEvent, ...], result: DispatchResult | None) -> None:
        page, locator = self.page, self.locator
        seen = {event.event_id for event in bound}
        missing = [] if locator is None else [i for i in locator.event_ids if i not in seen]
        after = page.next_cursor if page is not None and self.cas == "applied" else None
        logger.info(
            "mode_canary_audit_cursor owner_sha256=%s record_sha256=%s before=%s after=%s cas=%s "
            "wrap=%s delivered=%s failed=%s bound=%d missing=%s",
            "none" if locator is None else locator.owner_sha256,
            "none" if locator is None else locator.record_sha256,
            _cursor(None if page is None else page.expected), _cursor(after), self.cas,
            _flag(page is not None and page.next_cursor.position is None),
            "none" if result is None else result.delivered,
            "none" if result is None else result.failed, len(bound),
            ",".join(missing) or "none",
        )


class _ObservedPlane:
    def __init__(self, inner: ControlPlanePort, observation: _Observation) -> None:
        self._inner = inner
        self._observation = observation

    def read_outbox_page(self, cursor: OutboxCursor | None, limit: int) -> OutboxPage:
        page = self._inner.read_outbox_page(cursor, limit)
        self._observation.page = page
        return page

    def get_outbox_event(self, event_id: str) -> OutboxEvent | None:
        return self._inner.get_outbox_event(event_id)

    def mark_outbox_delivered(self, event_id: str, delivered_at: datetime) -> None:
        self._inner.mark_outbox_delivered(event_id, delivered_at)
        self._observation.delivered.add(event_id)

    def advance_outbox_cursor(
        self, expected: OutboxCursor | None, next_cursor: OutboxCursor | None,
    ) -> bool:
        applied = self._inner.advance_outbox_cursor(expected, next_cursor)
        self._observation.cas = "applied" if applied else "conflict"
        return applied


class _ObservedSink:
    def __init__(self, inner: AuditSinkPort, observation: _Observation) -> None:
        self._inner = inner
        self._observation = observation

    def append(self, event: OutboxEvent) -> None:
        self._inner.append(event)
        self._observation.appended.add(event.event_id)


def run_audit_dispatch_once(
    runtime: AwsAuditRuntime, observe: AuditObserveLocator | None = None,
) -> int:
    """Executa uma única página canônica da outbox e observa só eventos sintéticos.

    Args: runtime: Portas e relógio do despacho; observe: locator observacional opcional.
    Returns: 0 em sucesso ou 1 quando algum evento falhou.
    """
    observation = _Observation(observe, runtime.retention_days)
    plane = cast("ControlPlanePort", _ObservedPlane(runtime.control_plane, observation))
    try:
        result = dispatch_once(
            plane, _ObservedSink(runtime.sink, observation), runtime.clock(), limit=100,
        )
    except BaseException:
        observation.emit(None)
        raise
    observation.emit(result)
    return int(result.failed > 0)
