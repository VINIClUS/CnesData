"""Testes da criação transacional de tenant faturado no plano de controle SQLite."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cnes_domain.billing.commands import CreateBilledTenantCommand
from cnes_domain.billing.errors import (
    BillingTenantConflict,
    IdempotencyConflict,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.control_plane.entities import IdempotencyRecord, Membership, Tenant
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.billing.dynamodb_items import audit_outbox_event
from cnes_infra.control_plane.billed_tenant import (
    TENANT_SCOPE,
    billed_tenant_digest,
    tenant_created_event,
)
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.control_plane.sqlite_schema import serialize_model
from packages.cnes_infra.tests.billing.billing_factories import make_link
from packages.cnes_infra.tests.contracts.clock import MutableClock

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
ACCOUNT = "ba_01"
NEW = "tenant-new"


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(NOW)


@pytest.fixture
def plane(tmp_path: Path, clock: MutableClock) -> SQLiteControlPlane:
    adapter = SQLiteControlPlane(tmp_path / "control-plane.sqlite3", clock.now)
    adapter.initialize()
    return adapter


def make_command(
    tenant_id: str = NEW, key: str = "bt-01", name: str = "Epitacio", at: datetime = NOW
) -> CreateBilledTenantCommand:
    return CreateBilledTenantCommand(
        tenant=Tenant(tenant_id=tenant_id, municipality_name=name, created_at=at),
        link=make_link(ACCOUNT, tenant_id),
        reservation_id="res-01",
        idempotency_key=key,
    )


def stored_record(plane: SQLiteControlPlane, tenant_id: str = NEW) -> str | None:
    with plane.read_connection() as connection:
        row = connection.execute(
            "SELECT data FROM idempotency_records WHERE tenant_id = ? AND scope = ?",
            (tenant_id, TENANT_SCOPE),
        ).fetchone()
    return None if row is None else row[0]


def test_cria_tenant_com_idempotencia_e_outbox(plane: SQLiteControlPlane) -> None:
    command = make_command()

    created = plane.create_billed_tenant(command)

    assert created == command.tenant
    assert plane.get_tenant(NEW) == command.tenant
    record = IdempotencyRecord.model_validate_json(stored_record(plane))
    assert (record.status, record.resource_id) == ("COMPLETED", NEW)
    assert record.request_hash == billed_tenant_digest(command)
    assert record.expires_at == NOW + timedelta(days=1)
    events = plane.pending_outbox(10)
    assert events == (audit_outbox_event(tenant_created_event(command)),)


def test_criador_do_tenant_recebe_membership_de_gestor(plane: SQLiteControlPlane) -> None:
    plane.create_billed_tenant(make_command())

    assert plane.get_membership(NEW, "user-owner") == Membership(
        tenant_id=NEW, user_id="user-owner", role="gestor", created_at=NOW,
    )


def test_replay_com_mesmo_comando_devolve_tenant(plane: SQLiteControlPlane) -> None:
    first = plane.create_billed_tenant(make_command())

    replayed = plane.create_billed_tenant(make_command(at=NOW + timedelta(hours=3)))

    assert replayed == first
    assert len(plane.pending_outbox(10)) == 1


def test_replay_sem_tenant_gravado_pede_nova_tentativa(plane: SQLiteControlPlane) -> None:
    command = make_command()
    record = IdempotencyRecord(
        tenant_id=NEW,
        scope=TENANT_SCOPE,
        key="bt-01",
        request_hash=billed_tenant_digest(command),
        status="COMPLETED",
        resource_id=NEW,
        created_at=NOW,
        expires_at=NOW + timedelta(days=1),
    )
    with plane.write_transaction() as connection:
        connection.execute(
            "INSERT INTO idempotency_records (tenant_id, scope, key, data) VALUES (?, ?, ?, ?)",
            (NEW, TENANT_SCOPE, "bt-01", serialize_model(record)),
        )

    with pytest.raises(RetryableBillingError, match="billing_idempotency_incomplete"):
        plane.create_billed_tenant(command)


def test_idempotencia_com_pedido_diferente_conflita(plane: SQLiteControlPlane) -> None:
    plane.create_billed_tenant(make_command())

    with pytest.raises(IdempotencyConflict, match="key=bt-01"):
        plane.create_billed_tenant(make_command(name="Outro Municipio"))


def test_idempotencia_expirada_e_sobrescrita(
    plane: SQLiteControlPlane, clock: MutableClock
) -> None:
    plane.create_billed_tenant(make_command())
    clock.advance(timedelta(days=2))
    with plane.write_transaction() as connection:
        connection.execute("DELETE FROM tenants WHERE tenant_id = ?", (NEW,))
        connection.execute("DELETE FROM outbox_events")

    plane.create_billed_tenant(make_command(name="Outro Municipio"))

    record = IdempotencyRecord.model_validate_json(stored_record(plane))
    assert record.expires_at == NOW + timedelta(days=3)
    assert plane.get_tenant(NEW).municipality_name == "Outro Municipio"


def test_tenant_existente_conflita(plane: SQLiteControlPlane) -> None:
    plane.put_tenant(Tenant(tenant_id=NEW, municipality_name="Antigo", created_at=NOW))

    with pytest.raises(BillingTenantConflict, match=f"tenant_id={NEW}"):
        plane.create_billed_tenant(make_command())

    assert plane.get_tenant(NEW).municipality_name == "Antigo"
    assert stored_record(plane) is None
    assert plane.pending_outbox(10) == ()


def test_tenant_reservado_e_rejeitado(plane: SQLiteControlPlane) -> None:
    with pytest.raises(PermanentBillingError, match="tenant_id_reserved"):
        plane.create_billed_tenant(make_command("_billing"))

    assert plane.get_tenant("_billing") is None
    assert plane.pending_outbox(10) == ()


def test_falha_no_outbox_desfaz_tenant_e_idempotencia(plane: SQLiteControlPlane) -> None:
    command = make_command()
    duplicate = audit_outbox_event(tenant_created_event(command))
    with plane.write_transaction() as connection:
        plane.put_outbox_event(connection, duplicate, duplicate.tenant_id)

    with pytest.raises(Conflict):
        plane.create_billed_tenant(command)

    assert plane.get_tenant(NEW) is None
    assert stored_record(plane) is None
    assert plane.get_membership(NEW, "user-owner") is None
    assert len(plane.pending_outbox(10)) == 1
