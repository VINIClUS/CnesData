"""Consulta de refresh ativo por agente na fronteira SQLite nativa."""

import logging
import sqlite3
from collections.abc import Generator
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from traceback import format_exception
from typing import TypedDict, cast
from uuid import UUID

import pytest

from cnes_domain.ports.refresh_tokens import RefreshTokenStore
from cnes_domain.refresh_tokens import (
    RefreshTokenContext,
    RefreshTokenInvalid,
    RefreshTokenMutation,
    RefreshTokenRecord,
    RefreshTokenUnavailable,
)
from cnes_infra.auth.sqlite_refresh_tokens import SQLiteRefreshTokenStore

_TENANT = "11111111-1111-4111-8111-111111111111"
_OTHER_TENANT = "22222222-2222-4222-8222-222222222222"
_AGENT = "33333333-3333-4333-8333-333333333333"
_OTHER_AGENT = "44444444-4444-4444-8444-444444444444"
_DIGEST = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
_B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
_C = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
_D = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
_FINGERPRINT = "synthetic-active-fingerprint-canary"
_CANARY = "synthetic-active-error-canary"
_PLAINTEXT = "synthetic-active-secret-canary"
_ISSUED = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
_EXPIRES = datetime(2026, 1, 3, 3, 4, 5, 654321, tzinfo=UTC)
_NOW = datetime(2026, 1, 2, 6, 0, 0, tzinfo=UTC)
_CONTEXT = RefreshTokenContext(_TENANT, _NOW)
_ROW = (
    _DIGEST,
    _AGENT,
    _TENANT,
    _FINGERPRINT,
    "2026-01-02T03:04:05.123456+00:00",
    "2026-01-03T03:04:05.654321+00:00",
    "2026-01-02T04:05:06.234567+00:00",
    None,
    7,
)
_REVOKED = (_B, *_ROW[1:7], "2026-01-02T05:00:00.345678+00:00", 7)
_EXPIRED = (_C, *_ROW[1:5], "2026-01-02T05:00:00.000000+00:00", *_ROW[6:])
_FUTURE = (_D, *_ROW[1:4], "2026-01-02T07:00:00.000000+00:00", _ROW[5], None, None, 7)
_SCHEMA = """
CREATE TABLE auth_refresh_tokens (
    token_sha256 TEXT NOT NULL, agent_id TEXT NOT NULL, tenant_id TEXT NOT NULL,
    machine_fingerprint TEXT NOT NULL, issued_at TEXT NOT NULL, expires_at TEXT NOT NULL,
    last_used_at TEXT, revoked_at TEXT, version INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, token_sha256)
)
"""
_INSERT = "INSERT INTO auth_refresh_tokens VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
_SELECT = (
    "SELECT token_sha256, agent_id, tenant_id, machine_fingerprint, issued_at, "
    "expires_at, last_used_at, revoked_at, version FROM auth_refresh_tokens "
    "ORDER BY tenant_id, token_sha256"
)
_FORBIDDEN = {
    sqlite3.SQLITE_INSERT,
    sqlite3.SQLITE_UPDATE,
    sqlite3.SQLITE_DELETE,
    sqlite3.SQLITE_CREATE_TABLE,
    sqlite3.SQLITE_DROP_TABLE,
    sqlite3.SQLITE_ALTER_TABLE,
    sqlite3.SQLITE_CREATE_INDEX,
    sqlite3.SQLITE_DROP_INDEX,
    sqlite3.SQLITE_PRAGMA,
    sqlite3.SQLITE_TRANSACTION,
    sqlite3.SQLITE_SAVEPOINT,
}


class _ConnectOptions(TypedDict, total=False):
    isolation_level: None
    autocommit: bool


class _ObservedConnection(sqlite3.Connection):
    database: "_Database"

    def execute(self, sql: str, parameters: object = (), /) -> sqlite3.Cursor:
        self.database.statements.append((sql, parameters))
        values = cast("tuple[str | int | float | bytes | None, ...]", parameters)
        return super().execute(sql, values)

    def commit(self) -> None:
        self.database.transaction_calls.append("commit")
        super().commit()

    def rollback(self) -> None:
        self.database.transaction_calls.append("rollback")
        super().rollback()

    def close(self) -> None:
        self.database.closes += 1
        self.database.closing = True
        try:
            super().close()
        finally:
            self.database.closing = False
        if self.database.failure == "close":
            raise sqlite3.OperationalError(f"{_CANARY}:{_PLAINTEXT}:{_FINGERPRINT}")


class _Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.attempts = self.secret_calls = self.closes = 0
        self.connections: list[_ObservedConnection] = []
        self.statements: list[tuple[str, object]] = []
        self.trace: list[str] = []
        self.transaction_calls: list[str] = []
        self.forbidden: list[int] = []
        self.failure = ""
        self.mode = "legacy"
        self.closing = False

    def seed(self, *rows: tuple[object, ...]) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.execute(_SCHEMA)
            connection.executemany(_INSERT, rows)
        connection.close()

    def connect(self) -> _ObservedConnection:
        self.attempts += 1
        if self.failure == "factory":
            return cast("_ObservedConnection", sqlite3.connect(self.path / _CANARY))
        options: _ConnectOptions = {}
        if self.mode == "legacy-none":
            options["isolation_level"] = None
        if self.mode in {"autocommit", "transactional"}:
            options["autocommit"] = self.mode == "autocommit"
        connection = sqlite3.connect(
            self.path.as_uri() + "?mode=ro",
            uri=True,
            factory=_ObservedConnection,
            **options,
        )
        connection.database = self
        if self.mode == "begun":
            sqlite3.Connection.execute(connection, "BEGIN")
        self.connections.append(connection)
        connection.set_trace_callback(self.trace.append)
        connection.set_authorizer(lambda action, operation, *_: self.authorize(action, operation))
        return connection

    def authorize(self, action: int, operation: str | None) -> int:
        if action == sqlite3.SQLITE_TRANSACTION and operation == "ROLLBACK" and self.closing:
            return sqlite3.SQLITE_OK
        if action in _FORBIDDEN:
            self.forbidden.append(action)
            return sqlite3.SQLITE_DENY
        if self.failure == "read" and action == sqlite3.SQLITE_READ:
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    def secret(self) -> str:
        self.secret_calls += 1
        pytest.fail("token_factory_called_on_active_query")

    def store(self) -> RefreshTokenStore:
        return SQLiteRefreshTokenStore(self.connect, self.secret)

    def rows(self) -> list[tuple[object, ...]]:
        connection = sqlite3.connect(self.path)
        try:
            return connection.execute(_SELECT).fetchall()
        finally:
            connection.close()

    def assert_closed(self) -> None:
        assert self.closes == len(self.connections)
        assert self.secret_calls == 0
        assert self.transaction_calls == []
        for connection in self.connections:
            with pytest.raises(sqlite3.ProgrammingError):
                sqlite3.Connection.execute(connection, "SELECT 1")

    def assert_readonly(self) -> None:
        self.assert_closed()
        assert self.attempts == len(self.connections)
        assert self.forbidden == []
        assert len(self.statements) == self.attempts
        assert all(sql.lstrip().upper().startswith("SELECT") for sql, _ in self.statements)


@pytest.fixture
def database(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> Generator[_Database]:
    caplog.set_level(logging.DEBUG)
    boundary = _Database(tmp_path / "refresh-active.sqlite")
    try:
        yield boundary
    finally:
        for connection in boundary.connections:
            sqlite3.Connection.close(connection)
        captured = capsys.readouterr()
        observations = caplog.text + captured.out + captured.err + "\n".join(boundary.trace)
        assert all(value not in observations for value in (_PLAINTEXT, _CANARY, _FINGERPRINT))


def _assert_sanitized(error: Exception) -> None:
    public = str(error) + repr(error) + "".join(format_exception(error))
    sensitive = (_PLAINTEXT, _CANARY, _FINGERPRINT, _TENANT, _OTHER_TENANT, _AGENT, _OTHER_AGENT)
    assert all(value not in public for value in sensitive)
    assert error.__cause__ is None
    assert error.__context__ is None or error.__suppress_context__


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        pytest.param((), False, id="vazio"),
        pytest.param((_REVOKED, _EXPIRED, _FUTURE), False, id="somente-inativos"),
        pytest.param((_REVOKED, _EXPIRED, _FUTURE, _ROW), True, id="misto-ativo"),
        pytest.param((_ROW, (_B, *_ROW[1:])), True, id="dois-ativos"),
    ],
)
def test_vazio_e_estados_mistos(
    database: _Database,
    rows: tuple[tuple[object, ...], ...],
    expected: bool,
) -> None:
    database.seed(*rows)
    assert database.store().has_active_for_agent(_AGENT, _CONTEXT) is expected
    assert database.rows() == sorted(rows, key=lambda row: (row[2], row[0]))
    database.assert_readonly()


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        pytest.param(_ISSUED - timedelta(microseconds=1), False, id="antes-emissao"),
        pytest.param(_ISSUED, True, id="emissao-exata"),
        pytest.param(_EXPIRES - timedelta(microseconds=1), True, id="ultimo-microssegundo"),
        pytest.param(_EXPIRES, False, id="prazo-exato"),
        pytest.param(_EXPIRES + timedelta(microseconds=1), False, id="apos-prazo"),
    ],
)
def test_limites_temporais_em_microssegundos(
    database: _Database,
    now: datetime,
    expected: bool,
) -> None:
    database.seed(_ROW)
    context = RefreshTokenContext(_TENANT, now)
    assert database.store().has_active_for_agent(_AGENT, context) is expected
    assert database.rows() == [_ROW]
    database.assert_readonly()


def test_isola_tenant_e_agente(database: _Database) -> None:
    other_tenant = (_DIGEST, _AGENT, _OTHER_TENANT, *_ROW[3:])
    other_agent = (_B, _OTHER_AGENT, *_ROW[2:])
    database.seed(other_tenant, other_agent)
    store = database.store()
    assert store.has_active_for_agent(_AGENT, _CONTEXT) is False
    assert store.has_active_for_agent(_AGENT, RefreshTokenContext(_OTHER_TENANT, _NOW)) is True
    assert store.has_active_for_agent(_OTHER_AGENT, _CONTEXT) is True
    other_context = RefreshTokenContext(_OTHER_TENANT, _NOW)
    assert store.has_active_for_agent(_OTHER_AGENT, other_context) is False
    assert database.rows() == [other_agent, other_tenant]
    database.assert_readonly()


def test_utc_nao_depende_da_grafia(database: _Database) -> None:
    row = (*_ROW[:4], "2026-01-02T03:04:05.123456Z", "2026-01-03 03:04:05.654321+00:00", *_ROW[6:])
    database.seed(row)
    context = RefreshTokenContext(_TENANT, _EXPIRES - timedelta(microseconds=1))
    assert database.store().has_active_for_agent(_AGENT, context) is True
    assert database.rows() == [row]
    database.assert_readonly()


def test_uso_nao_consome_e_revogacao_persiste(database: _Database) -> None:
    database.seed()
    mutator = SQLiteRefreshTokenStore(lambda: sqlite3.connect(database.path), database.secret)
    record = RefreshTokenRecord(
        _DIGEST,
        _AGENT,
        _TENANT,
        _FINGERPRINT,
        _ISSUED,
        _EXPIRES,
        datetime(2026, 1, 2, 4, 5, 6, 234567, tzinfo=UTC),
        None,
        7,
    )
    mutator.import_hashed(record)
    assert database.store().has_active_for_agent(_AGENT, _CONTEXT) is True
    used = datetime(2026, 1, 2, 6, 0, 0, 1, tzinfo=UTC)
    assert mutator.mark_used("abc", RefreshTokenMutation(_TENANT, used, 7)) is True
    assert database.store().has_active_for_agent(_AGENT, RefreshTokenContext(_TENANT, used)) is True
    revoked = datetime(2026, 1, 2, 6, 0, 0, 2, tzinfo=UTC)
    assert mutator.revoke("abc", RefreshTokenMutation(_TENANT, revoked, 8)) is True
    context = RefreshTokenContext(_TENANT, revoked)
    assert database.store().has_active_for_agent(_AGENT, context) is False
    assert database.rows() == [
        (*_ROW[:6], "2026-01-02T06:00:00.000001+00:00", "2026-01-02T06:00:00.000002+00:00", 9)
    ]
    database.assert_readonly()


def test_consulta_reaberta_preserva_prazo(database: _Database) -> None:
    database.seed(_ROW)
    first = database.store()
    assert first.has_active_for_agent(_AGENT, _CONTEXT) is True
    del first
    context = RefreshTokenContext(_TENANT, _EXPIRES)
    assert database.store().has_active_for_agent(_AGENT, context) is False
    assert database.rows() == [_ROW]
    database.assert_readonly()


@pytest.mark.parametrize(
    ("agent", "context"),
    [
        pytest.param(None, _CONTEXT, id="agente-none"),
        pytest.param(b"33333333-3333-4333-8333-333333333333", _CONTEXT, id="agente-bytes"),
        pytest.param("", _CONTEXT, id="agente-vazio"),
        pytest.param("33333333333343338333333333333333", _CONTEXT, id="agente-nao-canonico"),
        pytest.param(UUID(_AGENT), _CONTEXT, id="agente-objeto-uuid"),
        pytest.param("'; SELECT 1;--", _CONTEXT, id="agente-sql"),
        pytest.param(_AGENT, RefreshTokenContext("not-a-uuid", _NOW), id="tenant-invalido"),
        pytest.param(_AGENT, RefreshTokenContext(_TENANT, _NOW.replace(tzinfo=None)), id="naive"),
        pytest.param(
            _AGENT,
            RefreshTokenContext(_TENANT, _NOW.replace(tzinfo=timezone(timedelta(hours=1)))),
            id="offset",
        ),
        pytest.param(_AGENT, RefreshTokenContext(_TENANT, cast("datetime", "now")), id="tipo-now"),
        pytest.param(_AGENT, None, id="contexto-none"),
    ],
)
def test_entrada_invalida_precede_io(
    database: _Database,
    agent: object,
    context: object,
) -> None:
    with pytest.raises(RefreshTokenInvalid) as caught:
        database.store().has_active_for_agent(
            cast("str", agent),
            cast("RefreshTokenContext", context),
        )
    assert database.attempts == database.secret_calls == 0
    _assert_sanitized(caught.value)


@pytest.mark.parametrize(
    ("change", "now"),
    [
        pytest.param((0, "bad"), _NOW, id="digest"),
        pytest.param((3, ""), _EXPIRES, id="fingerprint-expirado"),
        pytest.param((4, _CANARY), _NOW, id="emissao-ilegivel"),
        pytest.param((5, "2026-01-03T04:04:05.654321+01:00"), _NOW, id="offset-revogado"),
        pytest.param((8, -1), _ISSUED - timedelta(microseconds=1), id="versao-futuro"),
        pytest.param((8, "invalid-version"), _NOW, id="versao-texto"),
        pytest.param((6, "2026-01-03T03:04:05.654322+00:00"), _EXPIRES, id="uso-apos-prazo"),
        pytest.param((7, "2026-01-02T03:04:05.123455+00:00"), _NOW, id="revogacao-antes-emissao"),
    ],
)
def test_corrupcao_selecionada_nao_vira_inatividade(
    database: _Database,
    change: tuple[int, object],
    now: datetime,
) -> None:
    row: list[object] = list(_ROW)
    column, value = change
    row[column] = value
    if column == 5:
        row[7] = "2026-01-02T05:00:00.345678+00:00"
    database.seed(tuple(row))
    with pytest.raises(RefreshTokenUnavailable) as caught:
        database.store().has_active_for_agent(_AGENT, RefreshTokenContext(_TENANT, now))
    assert database.rows() == [tuple(row)]
    database.assert_readonly()
    _assert_sanitized(caught.value)


@pytest.mark.parametrize("reverse", [False, True], ids=["boa-primeiro", "corrupta-primeiro"])
def test_corrupcao_prevalece_sobre_outra_linha_ativa(database: _Database, reverse: bool) -> None:
    corrupt = (_B, *_ROW[1:3], "", *_ROW[4:])
    rows = (corrupt, _ROW) if reverse else (_ROW, corrupt)
    database.seed(*rows)
    with pytest.raises(RefreshTokenUnavailable) as caught:
        database.store().has_active_for_agent(_AGENT, _CONTEXT)
    assert database.rows() == [_ROW, corrupt]
    database.assert_readonly()
    _assert_sanitized(caught.value)


def test_corrupcao_nao_selecionada_e_isolada(database: _Database) -> None:
    other_tenant = (_B, _AGENT, _OTHER_TENANT, "", *_ROW[4:])
    other_agent = (_C, _OTHER_AGENT, _TENANT, "", *_ROW[4:])
    database.seed(_ROW, other_tenant, other_agent)
    assert database.store().has_active_for_agent(_AGENT, _CONTEXT) is True
    assert database.rows() == [_ROW, other_agent, other_tenant]
    database.assert_readonly()


@pytest.mark.parametrize("mode", ["legacy", "legacy-none", "autocommit"])
def test_conexao_idle_e_somente_leitura(database: _Database, mode: str) -> None:
    database.seed(_ROW)
    database.mode = mode
    assert database.store().has_active_for_agent(_AGENT, _CONTEXT) is True
    assert database.rows() == [_ROW]
    database.assert_readonly()


@pytest.mark.parametrize("mode", ["begun", "transactional"])
def test_handoff_ativo_e_recusado(database: _Database, mode: str) -> None:
    database.seed(_ROW)
    database.mode = mode
    with pytest.raises(RefreshTokenUnavailable) as caught:
        database.store().has_active_for_agent(_AGENT, _CONTEXT)
    assert database.attempts == len(database.connections) == 1
    assert database.statements == []
    assert database.rows() == [_ROW]
    database.assert_closed()
    _assert_sanitized(caught.value)


@pytest.mark.parametrize("origin", ["factory", "table", "read", "close"])
def test_fronteira_indisponivel_e_sanitizada(database: _Database, origin: str) -> None:
    if origin == "table":
        sqlite3.connect(database.path).close()
    else:
        database.seed(_ROW)
    before = database.path.read_bytes()
    database.failure = origin
    with pytest.raises(RefreshTokenUnavailable) as caught:
        database.store().has_active_for_agent(_AGENT, _CONTEXT)
    assert database.attempts == 1
    assert len(database.connections) == (0 if origin == "factory" else 1)
    assert database.path.read_bytes() == before
    if origin != "table":
        assert database.rows() == [_ROW]
    assert database.forbidden == []
    database.assert_closed()
    _assert_sanitized(caught.value)
