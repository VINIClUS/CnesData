"""Fronteiras reais para o contrato de entrega da outbox."""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from threading import RLock
from typing import Any

import boto3
import pytest

from cnes_domain.control_plane.entities import OutboxEvent
from cnes_infra.audit.local_sink import LocalAuditSink
from cnes_infra.audit.s3_object_lock_sink import S3ObjectLockAuditSink
from cnes_infra.billing.dynamodb_items import outbox_item
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_codec import decode_model, encode_marker
from cnes_infra.control_plane.dynamodb_keys import entity_key, item_key, outbox_key
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.control_plane.sqlite_schema import serialize_model
from packages.cnes_infra.tests.contracts.clock import _NOW, _TENANT, _event
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    _create_table,
)


class SdkBoundary:
    def __init__(self, client: Any) -> None:
        self.client = client
        self._operation_lock = RLock()
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.before: Callable[[str, dict[str, Any]], None] | None = None

    def __getattr__(self, name: str) -> Any:
        target = getattr(self.client, name)
        if not callable(target):
            return target

        def call(**request: Any) -> Any:
            with self._operation_lock:
                self.calls.append((name, dict(request)))
                if self.before is not None:
                    self.before(name, request)
                return target(**request)

        return call


class SinkBoundary:
    def __init__(self, sink: Any) -> None:
        self.sink = sink
        self.attempts: list[OutboxEvent] = []
        self.before: Callable[[OutboxEvent], None] | None = None

    def append(self, event: OutboxEvent) -> None:
        self.attempts.append(event)
        if self.before is not None:
            self.before(event)
        self.sink.append(event)


class SqlBoundary:
    def __init__(self, connection: Any, before: Callable[[str], None]) -> None:
        self.connection = connection
        self.before = before

    def execute(self, sql: str, parameters: Any = ()) -> Any:
        self.before(sql)
        return self.connection.execute(sql, parameters)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.connection, name)


def evento(event_id: str, offset: int = 0) -> OutboxEvent:
    return _event(event_id, _NOW + timedelta(seconds=offset))


@dataclass
class OutboxHarness:
    backend: str
    root: Path
    adapter: Any = field(init=False)
    client: Any = field(default=None, init=False)
    sdk: Any = field(default=None, init=False)

    def __post_init__(self) -> None:
        if self.backend == "sqlite":
            self.adapter = self.restart()
            self.adapter.initialize()
        else:
            self.client = boto3.client("dynamodb", region_name="us-east-1")
            _create_table(self.client)
            self.sdk = SdkBoundary(self.client)
            self.adapter = self.restart()

    def restart(self) -> Any:
        if self.backend == "sqlite":
            return SQLiteControlPlane(self.root / "control.sqlite3", lambda: _NOW)
        return DynamoDBControlPlane(self.sdk, _TABLE_NAME, lambda: _NOW)

    def put(self, event: OutboxEvent) -> None:
        if self.backend == "sqlite":
            with self.adapter.write_transaction() as connection:
                self.adapter.put_outbox_event(connection, event, event.tenant_id)
        else:
            self.client.put_item(TableName=_TABLE_NAME, Item=outbox_item(event))

    def stored(self, event_id: str) -> OutboxEvent:
        if self.backend == "sqlite":
            with self.adapter.read_connection() as connection:
                row = connection.execute(
                    "SELECT data FROM outbox_events WHERE event_id = ?", (event_id,)
                ).fetchone()
            assert row is not None, f"event_id={event_id} missing"
            return OutboxEvent.model_validate_json(row[0])
        item = self.client.get_item(
            TableName=_TABLE_NAME, Key=item_key(*outbox_key(event_id)), ConsistentRead=True
        ).get("Item")
        assert item is not None, f"event_id={event_id} missing"
        return decode_model(item, OutboxEvent)

    def mutate(self, event: OutboxEvent | None, original_id: str) -> None:
        if self.backend == "sqlite":
            self._mutate_sqlite(event, original_id)
        elif event is None:
            self.client.delete_item(TableName=_TABLE_NAME, Key=item_key(*outbox_key(original_id)))
        else:
            item = outbox_item(event) | item_key(*outbox_key(original_id))
            self.client.put_item(TableName=_TABLE_NAME, Item=item)

    def _mutate_sqlite(self, event: OutboxEvent | None, original_id: str) -> None:
        with self.adapter.write_transaction() as connection:
            if event is None:
                connection.execute("DELETE FROM outbox_events WHERE event_id = ?", (original_id,))
            else:
                delivered = None if event.delivered_at is None else event.delivered_at.isoformat()
                connection.execute(
                    "UPDATE outbox_events SET created_at=?, delivered_at=?, data=? "
                    "WHERE event_id=?",
                    (event.created_at.isoformat(), delivered, serialize_model(event), original_id),
                )

    def stale_candidate(self, state: str) -> None:
        base = evento("stale", -10)
        if state == "delivered":
            self.put(base)
            self.adapter.mark_outbox_delivered(base.event_id, _NOW)
        elif state == "position":
            self.put(evento("stale", 10))
        marker = encode_marker(
            "STALE_OUTBOX", entity_key(_TENANT, "STALE_OUTBOX", "marker"),
            outbox_key("stale"),
            {"gsi6pk": "OUTBOX#PENDING", "gsi6sk": f"{base.created_at.isoformat()}#stale"},
        )
        self.client.put_item(TableName=_TABLE_NAME, Item=marker)

    def sink(self, kind: str = "local") -> SinkBoundary:
        if kind == "local":
            return SinkBoundary(LocalAuditSink(self.root / "sink"))
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="cursor-audit", ObjectLockEnabledForBucket=True)
        return SinkBoundary(S3ObjectLockAuditSink(s3, "cursor-audit", 3650))

    def local_records(self) -> list[OutboxEvent]:
        return [
            OutboxEvent.model_validate_json(line)
            for path in (self.root / "sink" / "audit").rglob("events.jsonl")
            for line in path.read_text().splitlines()
        ]

    def fail_marker_once(self, monkeypatch: pytest.MonkeyPatch, error: BaseException) -> None:
        remaining = [True]

        def fail() -> None:
            if remaining[0]:
                remaining[0] = False
                raise error

        def before_sql(sql: str) -> None:
            if sql.upper().startswith("UPDATE OUTBOX_EVENTS"):
                fail()

        def before_sdk(name: str, request: dict[str, Any]) -> None:
            if name == "transact_write_items" or (
                name == "update_item" and request.get("Key") == item_key(*outbox_key("first"))
            ):
                fail()

        if self.backend == "dynamodb":
            self.sdk.before = before_sdk
        else:
            connect = self.adapter._connect
            monkeypatch.setattr(
                self.adapter, "_connect", lambda: SqlBoundary(connect(), before_sql)
            )
