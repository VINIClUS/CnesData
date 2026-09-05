"""SQLite idempotency operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cnes_domain.control_plane.commands import IdempotencyOutcome
from cnes_domain.control_plane.entities import IdempotencyRecord
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_infra.control_plane.sqlite_schema import deserialize_model, serialize_model

if TYPE_CHECKING:
    from cnes_domain.control_plane.commands import BeginIdempotency


def begin_idempotency(store: Any, command: BeginIdempotency) -> IdempotencyOutcome:
    with store.write_transaction() as connection:
        row = connection.execute(
            "SELECT data FROM idempotency_records "
            "WHERE tenant_id = ? AND scope = ? AND key = ?",
            (command.tenant_id, command.scope, command.key),
        ).fetchone()
        current = None if row is None else deserialize_model(row[0], IdempotencyRecord)
        if current is not None and current.expires_at > command.now:
            if current.request_hash != command.request_hash:
                raise Conflict(ErrorCode.IDEMPOTENCY_HASH_CONFLICT)
            return IdempotencyOutcome(record=current, created=False)
        record = IdempotencyRecord(
            tenant_id=command.tenant_id,
            scope=command.scope,
            key=command.key,
            request_hash=command.request_hash,
            status="CREATED",
            resource_id=command.resource_id,
            created_at=command.now,
            expires_at=command.expires_at,
        )
        connection.execute(
            "INSERT INTO idempotency_records (tenant_id, scope, key, data) "
            "VALUES (?, ?, ?, ?) ON CONFLICT (tenant_id, scope, key) "
            "DO UPDATE SET data = excluded.data",
            (record.tenant_id, record.scope, record.key, serialize_model(record)),
        )
        return IdempotencyOutcome(record=record, created=True)
