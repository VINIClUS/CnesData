"""Migração aditiva, SQL limitado e concorrência na conexão SQLite real."""

import re
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from cnes_domain.control_plane.entities import Tenant
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.contracts.clock import _NOW, _job
from packages.cnes_infra.tests.control_plane.outbox_cursor_sqlite_race import SQLiteCasInterleaving
from packages.cnes_infra.tests.control_plane.outbox_cursor_support import (
    OutboxHarness,
    SqlBoundary,
    evento,
)

_OLD_SCHEMA = (
    Path(__file__).parents[1] / "fixtures/control_plane/sqlite-before-outbox-cursor-440a894.sql"
)


@pytest.fixture
def sqlite_harness(tmp_path: Path) -> OutboxHarness:
    return OutboxHarness("sqlite", tmp_path)


@pytest.fixture
def legacy_sqlite(tmp_path: Path) -> SQLiteControlPlane:
    path = tmp_path / "control.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.executescript(_OLD_SCHEMA.read_text())
    adapter = SQLiteControlPlane(path, lambda: _NOW)
    adapter.put_tenant(Tenant(
        tenant_id="354130", municipality_name="Sentinela anterior ao cursor", created_at=_NOW
    ))
    adapter.create_job(_job("legacy-pending-job"), evento("legacy-pending"))
    adapter.create_job(_job("legacy-delivered-job"), evento("legacy-delivered", 1))
    adapter.mark_outbox_delivered("legacy-delivered", _NOW)
    return adapter


def _legacy_snapshot(adapter: SQLiteControlPlane) -> dict[str, Any]:
    with adapter.read_connection() as connection:
        return {
            "events": connection.execute(
                "SELECT * FROM outbox_events ORDER BY event_id"
            ).fetchall(),
            "tenants": connection.execute("SELECT * FROM tenants ORDER BY tenant_id").fetchall(),
            "jobs": connection.execute("SELECT * FROM jobs ORDER BY job_id").fetchall(),
            "writes": connection.execute(
                "SELECT * FROM job_creation_writes ORDER BY job_id"
            ).fetchall(),
            "roots": connection.execute(
                "SELECT name,rootpage FROM sqlite_master WHERE name IN "
                "('outbox_events','tenants','jobs','job_creation_writes') ORDER BY name"
            ).fetchall(),
        }


def _schema_snapshot(adapter: SQLiteControlPlane) -> dict[str, tuple[str, int]]:
    with adapter.read_connection() as connection:
        rows = connection.execute(
            "SELECT name,sql,rootpage FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name"
        ).fetchall()
    return {name: (sql, rootpage) for name, sql, rootpage in rows}


def test_fixture_anterior_preserva_pending_delivered_e_sentinelas(
    legacy_sqlite: SQLiteControlPlane,
) -> None:
    with legacy_sqlite.read_connection() as connection:
        rows = connection.execute(
            "SELECT data FROM outbox_events WHERE delivered_at IS NULL "
            "ORDER BY created_at, event_id LIMIT ?", (10,),
        ).fetchall()
    assert tuple(type(evento("legacy-pending")).model_validate_json(row[0]) for row in rows) == (
        evento("legacy-pending"),
    )
    assert legacy_sqlite.get_job("354130", "legacy-pending-job") == _job("legacy-pending-job")
    assert legacy_sqlite.get_job("354130", "legacy-delivered-job") == _job("legacy-delivered-job")
    tenant = legacy_sqlite.get_tenant("354130")
    assert tenant is not None
    assert tenant.municipality_name == "Sentinela anterior ao cursor"
    snapshot = _legacy_snapshot(legacy_sqlite)
    assert len(snapshot["events"]) == len(snapshot["jobs"]) == len(snapshot["writes"]) == 2
    delivered = [row for row in snapshot["events"] if row[0] == "legacy-delivered"]
    assert delivered[0][3] == _NOW.isoformat()


def test_migracao_aditiva_duas_vezes_preserva_schema_anterior_e_funciona_cursor(
    legacy_sqlite: SQLiteControlPlane, monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _legacy_snapshot(legacy_sqlite)
    old_schema = _schema_snapshot(legacy_sqlite)
    destructive: list[tuple[Any, ...]] = []
    connect = legacy_sqlite._connect

    def authorize(*args: Any) -> int:
        if (
            args[0] in {sqlite3.SQLITE_DROP_TABLE, sqlite3.SQLITE_DELETE}
            and args[1] in old_schema
        ):
            destructive.append(args)
        return sqlite3.SQLITE_OK

    def observed_connection() -> Any:
        connection = connect()
        connection.set_authorizer(authorize)
        return connection

    monkeypatch.setattr(legacy_sqlite, "_connect", observed_connection)
    legacy_sqlite.initialize()
    legacy_sqlite.initialize()
    new_schema = _schema_snapshot(legacy_sqlite)
    assert all(new_schema[name] == value for name, value in old_schema.items())
    assert _legacy_snapshot(legacy_sqlite) == before
    assert destructive == []
    adapter: Any = legacy_sqlite
    page = adapter.read_outbox_page(None, 1)
    assert page.expected is None
    assert page.events == (evento("legacy-pending"),)
    assert adapter.advance_outbox_cursor(page.expected, page.next_cursor)
    assert _legacy_snapshot(legacy_sqlite) == before


def test_select_real_limita_candidatos_no_storage_antes_do_retorno(
    sqlite_harness: OutboxHarness, monkeypatch: pytest.MonkeyPatch,
) -> None:
    for index in range(5):
        sqlite_harness.put(evento(f"event-{index}", index))
    statements: list[str] = []
    connect = sqlite_harness.adapter._connect

    def observed_connection() -> Any:
        connection = connect()
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(sqlite_harness.adapter, "_connect", observed_connection)
    page = sqlite_harness.adapter.read_outbox_page(None, 2)
    assert page.events == (evento("event-0"), evento("event-1", 1))
    selects = [
        sql for sql in statements
        if "OUTBOX_EVENTS" in sql.upper() and "ORDER BY" in sql.upper()
    ]
    assert len(selects) == 1
    assert re.search(r"\bLIMIT\s+2\b", selects[0], re.IGNORECASE)
    with sqlite3.connect(sqlite_harness.root / "control.sqlite3") as connection:
        assert len(connection.execute(selects[0]).fetchall()) == 2
    assert sqlite_harness.adapter.advance_outbox_cursor(page.expected, page.next_cursor)
    assert sqlite_harness.adapter.read_outbox_page(None, 1).events == (evento("event-2", 2),)


def test_cas_estabelecido_intercala_escritores_sem_invalidar_begin_immediate(
    sqlite_harness: OutboxHarness, monkeypatch: pytest.MonkeyPatch,
) -> None:
    for index in range(4):
        sqlite_harness.put(evento(f"event-{index}", index))
    initial = sqlite_harness.adapter.read_outbox_page(None, 1)
    assert sqlite_harness.adapter.advance_outbox_cursor(initial.expected, initial.next_cursor)
    race = SQLiteCasInterleaving(sqlite_harness, monkeypatch)
    first = sqlite_harness.adapter.read_outbox_page(None, 1)
    second = race.contender.read_outbox_page(None, 2)
    assert first.expected == second.expected == initial.next_cursor
    assert first.next_cursor.position != second.next_cursor.position
    results = race.compete(first, second)
    assert sorted(results) == [False, True]
    assert len(race.lock_observed) == 1
    if race.lock_observed[0]:
        assert results == [True, False]
    else:
        assert results == [False, True]
    winner = first.next_cursor if results[0] else second.next_cursor
    assert sqlite_harness.restart().read_outbox_page(None, 1).expected == winner


def test_falha_de_io_no_cas_sqlite_propaga_sem_mudar_cursor(
    sqlite_harness: OutboxHarness, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sqlite_harness.put(evento("first"))
    sqlite_harness.put(evento("second", 1))
    page = sqlite_harness.adapter.read_outbox_page(None, 1)
    connect = sqlite_harness.adapter._connect

    def indisponivel(sql: str) -> None:
        if sql.lstrip().upper().startswith(("INSERT", "UPDATE")):
            raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(
        sqlite_harness.adapter, "_connect", lambda: SqlBoundary(connect(), indisponivel)
    )
    with pytest.raises(sqlite3.OperationalError, match="disk I/O error"):
        sqlite_harness.adapter.advance_outbox_cursor(page.expected, page.next_cursor)
    assert sqlite_harness.restart().read_outbox_page(None, 1).expected == page.expected
