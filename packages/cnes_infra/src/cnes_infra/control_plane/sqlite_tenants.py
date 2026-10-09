"""Criação transacional de tenant faturado no plano de controle SQLite."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cnes_domain.billing.errors import (
    BillingTenantConflict,
    IdempotencyConflict,
    RetryableBillingError,
)
from cnes_domain.control_plane.entities import IdempotencyRecord, Membership, Tenant
from cnes_infra.control_plane.billed_tenant import (
    TENANT_SCOPE,
    billed_tenant_digest,
    completed_record,
    creator_membership,
    require_creatable_tenant_id,
    tenant_created_event,
)
from cnes_infra.control_plane.sqlite_schema import deserialize_model, serialize_model

if TYPE_CHECKING:
    import sqlite3
    from datetime import datetime

    from cnes_domain.billing.commands import CreateBilledTenantCommand


def _select_tenant(connection: sqlite3.Connection, tenant_id: str) -> Tenant | None:
    row = connection.execute(
        "SELECT data FROM tenants WHERE tenant_id = ?", (tenant_id,)
    ).fetchone()
    return None if row is None else deserialize_model(row[0], Tenant)


def _live_record(
    connection: sqlite3.Connection, command: CreateBilledTenantCommand, now: datetime
) -> IdempotencyRecord | None:
    row = connection.execute(
        "SELECT data FROM idempotency_records WHERE tenant_id = ? AND scope = ? AND key = ?",
        (command.tenant.tenant_id, TENANT_SCOPE, command.idempotency_key),
    ).fetchone()
    if row is None:
        return None
    record = deserialize_model(row[0], IdempotencyRecord)
    if record.expires_at <= now:
        return None
    if record.request_hash != billed_tenant_digest(command):
        raise IdempotencyConflict(f"key={command.idempotency_key}")
    return record


def _replayed_tenant(connection: sqlite3.Connection, record: IdempotencyRecord) -> Tenant:
    tenant = _select_tenant(connection, record.resource_id)
    if tenant is None:
        raise RetryableBillingError("billing_idempotency_incomplete")
    return tenant


def _insert_tenant(connection: sqlite3.Connection, tenant: Tenant) -> None:
    connection.execute(
        "INSERT INTO tenants (tenant_id, data) VALUES (?, ?)",
        (tenant.tenant_id, serialize_model(tenant)),
    )


def _occupied(connection: sqlite3.Connection, membership: Membership) -> bool:
    row = connection.execute(
        "SELECT 1 FROM tenants WHERE tenant_id = ? UNION ALL "
        "SELECT 1 FROM memberships WHERE tenant_id = ? AND user_id = ?",
        (membership.tenant_id, membership.tenant_id, membership.user_id),
    ).fetchone()
    return row is not None


def _insert_membership(connection: sqlite3.Connection, membership: Membership) -> None:
    connection.execute(
        "INSERT INTO memberships (tenant_id, user_id, data) VALUES (?, ?, ?)",
        (membership.tenant_id, membership.user_id, serialize_model(membership)),
    )


def _upsert_record(connection: sqlite3.Connection, record: IdempotencyRecord) -> None:
    connection.execute(
        "INSERT INTO idempotency_records (tenant_id, scope, key, data) VALUES (?, ?, ?, ?) "
        "ON CONFLICT (tenant_id, scope, key) DO UPDATE SET data = excluded.data",
        (record.tenant_id, record.scope, record.key, serialize_model(record)),
    )


class SQLiteBilledTenantMixin:
    _clock: Any
    write_transaction: Any
    put_outbox_event: Any

    def create_billed_tenant(self, command: CreateBilledTenantCommand) -> Tenant:
        """Cria tenant, membership do criador, idempotência e evento em uma transação.

        Args: command: Tenant, link, reserva, chave de idempotência e emissor do criador.
        Returns: O tenant criado ou o tenant de um replay idêntico.
        Raises: BillingTenantConflict, IdempotencyConflict, PermanentBillingError.
        """
        from cnes_infra.billing.dynamodb_items import audit_outbox_event

        tenant = command.tenant
        require_creatable_tenant_id(tenant.tenant_id)
        now = self._clock()
        with self.write_transaction() as connection:
            live = _live_record(connection, command, now)
            if live is not None:
                return _replayed_tenant(connection, live)
            membership = creator_membership(command)
            if _occupied(connection, membership):
                raise BillingTenantConflict(f"tenant_id={tenant.tenant_id}")
            _insert_tenant(connection, tenant)
            _insert_membership(connection, membership)
            _upsert_record(connection, completed_record(command, now))
            event = audit_outbox_event(tenant_created_event(command))
            self.put_outbox_event(connection, event, event.tenant_id)
        return tenant
