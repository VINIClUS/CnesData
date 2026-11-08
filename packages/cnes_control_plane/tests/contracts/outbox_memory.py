"""Fake de outbox em memória para os contratos de control plane."""

import json
from datetime import datetime
from typing import Any

from cnes_domain.control_plane.outbox import (
    OutboxCursor,
    OutboxPage,
    next_outbox_cursor,
    validate_outbox_advance,
)


class MemoryOutbox:
    def __init__(self) -> None:
        self.outbox: dict[str, Any] = {}
        self.outbox_cursor: OutboxCursor | None = None

    def read_outbox_page(self, cursor: OutboxCursor | None, limit: int) -> OutboxPage:
        if limit < 1:
            raise ValueError("limit=invalid")
        expected = self.outbox_cursor if cursor is None else cursor
        position = None if expected is None else expected.position
        after = None if position is None else tuple(json.loads(position))
        values = sorted(self.outbox.values(), key=lambda item: (item.created_at, item.event_id))
        pending = [
            event for event in values if event.delivered_at is None
            and (after is None or (event.created_at.isoformat(), event.event_id) > after)
        ]
        events = tuple(pending[:limit])
        position = None if len(pending) <= limit else json.dumps([
            events[-1].created_at.isoformat(), events[-1].event_id,
        ])
        return OutboxPage(events, expected, next_outbox_cursor(expected, position))

    def get_outbox_event(self, event_id: str) -> Any | None:
        return self.outbox.get(event_id)

    def advance_outbox_cursor(
        self, expected: OutboxCursor | None, next_cursor: OutboxCursor | None,
    ) -> bool:
        following = validate_outbox_advance(expected, next_cursor)
        if expected != self.outbox_cursor:
            return False
        self.outbox_cursor = following
        return True

    def mark_outbox_delivered(self, event_id: str, delivered_at: datetime) -> None:
        event = self.outbox[event_id]
        self.outbox[event_id] = event.model_copy(update={"delivered_at": delivered_at})
