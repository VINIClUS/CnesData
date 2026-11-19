"""Outbox SQLite com página limitada e cursor transacional."""

import json
from datetime import datetime, timedelta
from typing import Any

from cnes_domain.control_plane.entities import OutboxEvent
from cnes_domain.control_plane.errors import Conflict, NotFound
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_domain.control_plane.outbox import (
    OutboxCursor,
    OutboxPage,
    next_outbox_cursor,
    validate_outbox_advance,
)
from cnes_infra.control_plane.sqlite_schema import deserialize_model, serialize_model


def _load_cursor(connection: Any) -> OutboxCursor | None:
    row = connection.execute(
        "SELECT version, position FROM outbox_cursor WHERE singleton = 1"
    ).fetchone()
    return None if row is None else OutboxCursor(version=row[0], position=row[1])


def read_outbox_page(store: Any, cursor: OutboxCursor | None, limit: int) -> OutboxPage:
    """Args: Store, cursor opcional e limite da página bruta.
    Returns: Eventos, token esperado e próxima geração sem escrita.
    Raises: ValueError ou erros SQLite e de decodificação.
    """
    if limit < 1:
        raise ValueError("limit=invalid")
    with store.read_connection() as connection:
        expected = _load_cursor(connection) if cursor is None else cursor
        position = None if expected is None else expected.position
        parameters: tuple[Any, ...] = (limit,)
        statement = (
            "SELECT created_at, event_id, data FROM outbox_events WHERE delivered_at IS NULL "
            "ORDER BY created_at, event_id LIMIT ?"
        )
        if position is not None:
            created_at, event_id = json.loads(position)
            statement = (
                "SELECT created_at, event_id, data FROM outbox_events "
                "WHERE delivered_at IS NULL AND (created_at, event_id) > (?, ?) "
                "ORDER BY created_at, event_id LIMIT ?"
            )
            parameters = (created_at, event_id, limit)
        rows = connection.execute(statement, parameters).fetchall()
    following = json.dumps(list(rows[-1][:2])) if len(rows) == limit else None
    events = tuple(deserialize_model(row[2], OutboxEvent) for row in rows)
    return OutboxPage(events, expected, next_outbox_cursor(expected, following))


def advance_outbox_cursor(
    store: Any, expected: OutboxCursor | None, next_cursor: OutboxCursor | None,
) -> bool:
    """Args: Store, token esperado e próxima geração ou volta ao início.
    Returns: True para avanço transacional; False para perda do CAS.
    Raises: ValueError para transição inválida ou erros SQLite.
    """
    following = validate_outbox_advance(expected, next_cursor)
    with store.write_transaction() as connection:
        if _load_cursor(connection) != expected:
            return False
        connection.execute(
            "INSERT INTO outbox_cursor (singleton, version, position) VALUES (1, ?, ?) "
            "ON CONFLICT (singleton) DO UPDATE SET "
            "version = excluded.version, position = excluded.position",
            (following.version, following.position),
        )
    return True


def get_outbox_event(store: Any, event_id: str) -> OutboxEvent | None:
    """Args: Store e event_id da identidade global.
    Returns: Evento canônico ou None para identidade ausente ou divergente.
    Raises: Erros SQLite ou de decodificação.
    """
    with store.read_connection() as connection:
        row = connection.execute(
            "SELECT data FROM outbox_events WHERE event_id = ?", (event_id,),
        ).fetchone()
    event = None if row is None else deserialize_model(row[0], OutboxEvent)
    return event if event is None or event.event_id == event_id else None


def mark_outbox_delivered(store: Any, event_id: str, delivered_at: datetime) -> None:
    """Args: Store, identidade global e instante UTC de entrega.
    Returns: None após marcador ou replay do mesmo instante.
    Raises: ValueError, NotFound, Conflict ou erros SQLite.
    """
    if delivered_at.tzinfo is None or delivered_at.utcoffset() != timedelta(0):
        raise ValueError("datetime_not_utc")
    with store.write_transaction() as connection:
        row = connection.execute(
            "SELECT data FROM outbox_events WHERE event_id = ?", (event_id,),
        ).fetchone()
        if row is None:
            raise NotFound(ErrorCode.OUTBOX_EVENT_MISSING)
        event = deserialize_model(row[0], OutboxEvent)
        if event.delivered_at is not None:
            if event.delivered_at != delivered_at:
                raise Conflict(ErrorCode.OUTBOX_DELIVERY_CONFLICT)
            return
        delivered = event.model_copy(update={"delivered_at": delivered_at})
        connection.execute(
            "UPDATE outbox_events SET delivered_at = ?, data = ? WHERE event_id = ?",
            (delivered_at.isoformat(), serialize_model(delivered), event_id),
        )
