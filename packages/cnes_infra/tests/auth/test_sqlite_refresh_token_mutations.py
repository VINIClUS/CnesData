"""CAS de refresh com estado literal e fronteiras SQLite nativas."""

import logging
import sqlite3
from collections.abc import Generator
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from traceback import format_exception
from typing import cast

import pytest

from cnes_domain.ports.refresh_tokens import RefreshTokenStore
from cnes_domain.refresh_tokens import (
    RefreshTokenContext,
    RefreshTokenInvalid,
    RefreshTokenMutation,
    RefreshTokenUnavailable,
)
from cnes_infra.auth.sqlite_refresh_tokens import SQLiteRefreshTokenStore

PLAINTEXT = "abc"
DIGEST = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
SPACE_DIGEST = "36a9e7f1c95b82ffb99743e0c5c4ce95d83c9a430aac59f84ef3cbfab6145068"
TENANT = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"
CANARY = "synthetic-cas-error-canary"
RAW_ERROR = f"{CANARY}:abc:synthetic-cas-host"
NOW = datetime(2026, 1, 2, 3, 4, 6, 100002, tzinfo=UTC)
ROW = (
    DIGEST,
    "33333333-3333-4333-8333-333333333333",
    TENANT,
    "synthetic-cas-host",
    "2026-01-02T03:04:05.123456Z",
    "2026-01-02 03:04:06.654321+00:00",
    "2026-01-02T03:04:05.900001Z",
    None,
    7,
)
SCHEMA = """
CREATE TABLE auth_refresh_tokens (
    token_sha256 TEXT NOT NULL, agent_id TEXT NOT NULL, tenant_id TEXT NOT NULL,
    machine_fingerprint TEXT NOT NULL, issued_at TEXT NOT NULL, expires_at TEXT NOT NULL,
    last_used_at TEXT, revoked_at TEXT, version INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, token_sha256)
)
"""
SELECT = (
    "SELECT token_sha256, agent_id, tenant_id, machine_fingerprint, issued_at, expires_at, "
    "last_used_at, revoked_at, version FROM auth_refresh_tokens ORDER BY tenant_id, token_sha256"
)


class ObservedConnection(sqlite3.Connection):
    database: "Database"
    closed = False

    def execute(self, sql: str, parameters: object = (), /) -> sqlite3.Cursor:
        self.database.sql.append((sql, parameters))
        if self.database.failure in {"read", "update"}:
            verb = "SELECT" if self.database.failure == "read" else "UPDATE"
            if sql.lstrip().upper().startswith(verb):
                raise sqlite3.OperationalError(RAW_ERROR)
        return super().execute(sql, cast("tuple", parameters))

    def close(self) -> None:
        super().close()
        self.closed = True
        if self.database.failure == "close":
            raise sqlite3.OperationalError(RAW_ERROR)


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.attempts = self.secret_calls = self.commits = self.rollbacks = 0
        self.connections: list[ObservedConnection] = []
        self.sql: list[tuple[str, object]] = []
        self.trace: list[str] = []
        self.failure = ""
        self.mode: bool | int = sqlite3.LEGACY_TRANSACTION_CONTROL
        self.isolation: str | None = ""
        self.active = False
        self.timeout = 3.0
        self.connection_type = ObservedConnection

    def seed(self, *rows: tuple[object, ...]) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.execute(SCHEMA)
            connection.execute("CREATE TABLE canary (value TEXT)")
            connection.executemany(
                "INSERT INTO auth_refresh_tokens VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
        connection.close()

    def connect(self) -> ObservedConnection:
        self.attempts += 1
        if self.failure == "factory":
            raise sqlite3.OperationalError(RAW_ERROR)
        if self.isolation is None:
            connection = sqlite3.connect(
                self.path,
                factory=self.connection_type,
                isolation_level=None,
                autocommit=cast("bool", self.mode),
                timeout=self.timeout,
            )
        else:
            connection = sqlite3.connect(
                self.path,
                factory=self.connection_type,
                autocommit=cast("bool", self.mode),
                timeout=self.timeout,
            )
        connection.database = self
        self.connections.append(connection)
        if self.active:
            sqlite3.Connection.execute(connection, "BEGIN IMMEDIATE")
            sqlite3.Connection.execute(connection, "INSERT INTO canary VALUES (?)", (CANARY,))
        connection.set_trace_callback(self.trace.append)
        connection.set_authorizer(self.authorize)
        return connection

    def authorize(self, action: int, first: str | None, *_rest: object) -> int:
        if action == sqlite3.SQLITE_TRANSACTION:
            if first == "COMMIT":
                self.commits += 1
            if first == "ROLLBACK":
                self.rollbacks += 1
            if first == self.failure.upper():
                return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    def secret(self) -> str:
        self.secret_calls += 1
        pytest.fail("token_factory_called_on_mutation")

    def store(self) -> RefreshTokenStore:
        return SQLiteRefreshTokenStore(self.connect, self.secret)

    def query(self, sql: str) -> list[tuple[object, ...]]:
        connection = sqlite3.connect(self.path)
        try:
            return connection.execute(sql).fetchall()
        finally:
            connection.close()

    def rows(self) -> list[tuple[object, ...]]:
        return self.query(SELECT)

    def execute(self, sql: str) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.executescript(sql)
        connection.close()

    def assert_closed(self) -> None:
        assert self.attempts == len(self.connections)
        assert self.connections
        for connection in self.connections:
            assert connection.closed
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                sqlite3.Connection.execute(connection, "SELECT 1")
        assert self.secret_calls == 0
        assert PLAINTEXT not in "\n".join(self.trace)
        assert all(PLAINTEXT not in repr(parameters) for _, parameters in self.sql)


@pytest.fixture
def database(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> Generator[Database]:
    caplog.set_level(logging.DEBUG)
    boundary = Database(tmp_path / "refresh-cas.sqlite")
    try:
        yield boundary
    finally:
        for connection in boundary.connections:
            sqlite3.Connection.close(connection)
        captured = capsys.readouterr()
        assert_no_sensitive(caplog.text + captured.out + captured.err)


def mutation(now: datetime = NOW, version: int = 7) -> RefreshTokenMutation:
    return RefreshTokenMutation(TENANT, now, version)


def assert_sanitized(error: Exception, observations: str) -> None:
    public = str(error) + repr(error) + "".join(format_exception(error)) + observations
    assert_no_sensitive(public)
    assert error.__cause__ is None
    assert error.__context__ is None or error.__suppress_context__


def assert_no_sensitive(public: str) -> None:
    for sensitive in (PLAINTEXT, CANARY, "synthetic-cas-host"):
        assert sensitive not in public


@pytest.mark.parametrize("isolation", ["", None], ids=["legacy-default", "legacy-none"])
def test_uso_cas_preserva_reuso_e_revogacao_nao_ressuscita(
    database: Database,
    isolation: str | None,
) -> None:
    database.seed(ROW)
    database.isolation = isolation
    assert database.store().mark_used(PLAINTEXT, mutation()) is True
    assert database.rows() == [(*ROW[:6], "2026-01-02T03:04:06.100002+00:00", None, 8)]
    record = database.store().validate(PLAINTEXT, RefreshTokenContext(TENANT, NOW))
    assert record is not None
    assert record.version == 8
    assert record.revoked_at is None
    assert record.last_used_at == datetime(2026, 1, 2, 3, 4, 6, 100002, tzinfo=UTC)
    assert PLAINTEXT not in repr(record) + repr(mutation())
    next_use = mutation(NOW.replace(microsecond=100003), 8)
    assert database.store().mark_used(PLAINTEXT, next_use) is True
    assert database.rows() == [(*ROW[:6], "2026-01-02T03:04:06.100003+00:00", None, 9)]
    assert database.store().revoke(PLAINTEXT, mutation(NOW.replace(microsecond=100004), 9)) is True
    final = (*ROW[:6], "2026-01-02T03:04:06.100003+00:00", "2026-01-02T03:04:06.100004+00:00", 10)
    assert database.rows() == [final]
    assert database.store().validate(PLAINTEXT, RefreshTokenContext(TENANT, NOW)) is None
    assert database.store().revoke(PLAINTEXT, mutation(version=10)) is False
    assert database.store().mark_used(PLAINTEXT, mutation(version=10)) is False
    assert database.rows() == [final]
    assert database.commits == 3
    assert database.rollbacks == 2
    database.assert_closed()


@pytest.mark.parametrize("operation", ["mark_used", "revoke"])
@pytest.mark.parametrize(
    "case",
    ["missing", "wrong-tenant", "stale", "before-issue", "expiry", "after", "revoked"],
)
def test_cas_inativo_ou_stale_nao_altera_linha(
    database: Database,
    operation: str,
    case: str,
) -> None:
    row = (*ROW[:7], "2026-01-02T03:04:06.000001Z", 7) if case == "revoked" else ROW
    database.seed(row)
    now = {
        "before-issue": datetime(2026, 1, 2, 3, 4, 5, 123455, tzinfo=UTC),
        "expiry": datetime(2026, 1, 2, 3, 4, 6, 654321, tzinfo=UTC),
        "after": datetime(2026, 1, 2, 3, 4, 6, 654322, tzinfo=UTC),
    }.get(case, NOW)
    context = RefreshTokenMutation(
        OTHER if case == "wrong-tenant" else TENANT, now, 6 if case == "stale" else 7
    )
    token = "unknown'; SELECT 1;--" if case == "missing" else PLAINTEXT
    assert getattr(database.store(), operation)(token, context) is False
    assert database.rows() == [row]
    assert database.commits == 0
    assert database.rollbacks == 1
    database.assert_closed()


def test_cas_isola_tenant_mesmo_com_digest_e_versoes_distintas(database: Database) -> None:
    other = (*ROW[:2], OTHER, *ROW[3:6], None, None, 2)
    database.seed(ROW, other)
    assert database.store().mark_used(PLAINTEXT, RefreshTokenMutation(OTHER, NOW, 2)) is True
    changed = (*other[:6], "2026-01-02T03:04:06.100002+00:00", None, 3)
    assert database.rows() == [ROW, changed]
    assert database.store().revoke(PLAINTEXT, mutation()) is True
    assert database.rows() == [(*ROW[:7], "2026-01-02T03:04:06.100002+00:00", 8), changed]
    database.assert_closed()


@pytest.mark.parametrize(
    ("operation", "micros", "expected"),
    [
        ("mark_used", 123456, "2026-01-02T03:04:05.123456+00:00"),
        ("revoke", 123456, "2026-01-02T03:04:05.123456+00:00"),
        ("mark_used", 654320, "2026-01-02T03:04:06.654320+00:00"),
        ("revoke", 654320, "2026-01-02T03:04:06.654320+00:00"),
    ],
)
def test_cas_aceita_emissao_e_ultimo_microssegundo_sem_uso_anterior(
    database: Database,
    operation: str,
    micros: int,
    expected: str,
) -> None:
    database.seed((*ROW[:6], None, None, 7))
    now = datetime(2026, 1, 2, 3, 4, 5 if micros == 123456 else 6, micros, tzinfo=UTC)
    assert getattr(database.store(), operation)(PLAINTEXT, mutation(now)) is True
    tail = (expected, None, 8) if operation == "mark_used" else (None, expected, 8)
    assert database.rows() == [(*ROW[:6], *tail)]
    database.assert_closed()


@pytest.mark.parametrize(
    ("operation", "micros", "expected"),
    [
        ("mark_used", 900000, False),
        ("mark_used", 900001, True),
        ("mark_used", 900002, True),
        ("revoke", 900000, True),
    ],
)
def test_cas_preserva_microssegundos_e_grafias_utc(
    database: Database,
    operation: str,
    micros: int,
    expected: bool,
) -> None:
    database.seed(ROW)
    now = datetime(2026, 1, 2, 3, 4, 5, micros, tzinfo=UTC)
    assert getattr(database.store(), operation)(PLAINTEXT, mutation(now)) is expected
    literal = {
        900000: "2026-01-02T03:04:05.900000+00:00",
        900001: "2026-01-02T03:04:05.900001+00:00",
        900002: "2026-01-02T03:04:05.900002+00:00",
    }[micros]
    final = (*ROW[:6], literal, None, 8) if operation == "mark_used" else (*ROW[:7], literal, 8)
    assert database.rows() == [final if expected else ROW]
    assert database.commits == int(expected)
    database.assert_closed()


@pytest.mark.parametrize(
    ("token", "field", "value"),
    [
        (None, "expected_version", 7),
        ("", "expected_version", 7),
        (PLAINTEXT, "tenant_id", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"),
        (PLAINTEXT, "now", NOW.replace(tzinfo=None)),
        (PLAINTEXT, "now", NOW.astimezone(timezone(timedelta(hours=1)))),
        (PLAINTEXT, "now", "2026-01-02T03:04:06Z"),
        (PLAINTEXT, "expected_version", -1),
        (PLAINTEXT, "expected_version", True),
        (PLAINTEXT, "expected_version", 7.0),
    ],
)
def test_mutacao_invalida_falha_antes_de_io(
    database: Database,
    token: object,
    field: str,
    value: object,
) -> None:
    values = {"tenant_id": TENANT, "now": NOW, "expected_version": 7, field: value}
    context = RefreshTokenMutation(**cast("dict", values))
    for operation in (database.store().mark_used, database.store().revoke):
        with pytest.raises(RefreshTokenInvalid):
            operation(cast("str", token), context)
    assert database.attempts == database.secret_calls == 0


def test_whitespace_opaco_nao_chama_factory_de_segredo(database: Database) -> None:
    database.seed((SPACE_DIGEST, *ROW[1:]))
    assert database.store().mark_used(" ", mutation()) is True
    assert database.rows() == [
        (SPACE_DIGEST, *ROW[1:6], "2026-01-02T03:04:06.100002+00:00", None, 8)
    ]
    assert PLAINTEXT not in repr(mutation())
    database.assert_closed()


@pytest.mark.parametrize(
    "corruption",
    [
        (4, CANARY, 7, NOW),
        (8, -1, 7, NOW),
        (8, 7.5, 7, NOW),
        (6, "2026-01-02T03:04:05.123455Z", 6, NOW),
        (8, -1, 7, datetime(2026, 1, 2, 3, 4, 6, 654321, tzinfo=UTC)),
    ],
)
def test_corrupcao_precede_guardas_inativas_e_stale(
    database: Database,
    corruption: tuple[int, object, int, datetime],
) -> None:
    position, value, version, now = corruption
    row = (*ROW[:position], value, *ROW[position + 1 :])
    database.seed(row)
    with pytest.raises(RefreshTokenUnavailable) as caught:
        database.store().mark_used(PLAINTEXT, mutation(now, version))
    assert_sanitized(caught.value, "")
    assert database.rows() == [row]
    assert database.commits == 0
    assert database.rollbacks == 1
    database.assert_closed()


@pytest.mark.parametrize("failure", ["factory", "read", "update", "commit", "rollback", "close"])
def test_falha_de_fronteira_e_sanitizada_sem_commit(
    database: Database,
    failure: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database.seed(ROW)
    database.failure = failure
    context = mutation(version=6) if failure in {"rollback", "close"} else mutation()
    with pytest.raises(RefreshTokenUnavailable) as caught:
        database.store().revoke(PLAINTEXT, context)
    captured = capsys.readouterr()
    assert_sanitized(caught.value, caplog.text + captured.out + captured.err)
    assert database.rows() == [ROW]
    assert database.query("SELECT value FROM canary") == []
    if failure != "factory":
        database.assert_closed()
    assert database.commits == (1 if failure == "commit" else 0)


@pytest.mark.parametrize(
    "action",
    [
        "CREATE TRIGGER fail_update BEFORE UPDATE ON auth_refresh_tokens BEGIN "
        "INSERT INTO canary VALUES ('synthetic-cas-error-canary'); "
        "SELECT RAISE(ABORT, 'synthetic-cas-error-canary'); END;",
        "CREATE TRIGGER fail_update BEFORE UPDATE ON auth_refresh_tokens BEGIN "
        "INSERT INTO canary VALUES ('synthetic-cas-error-canary'); SELECT RAISE(IGNORE); END;",
    ],
)
def test_trigger_falho_ou_suprimido_reverte_linha_e_canary(database: Database, action: str) -> None:
    database.seed(ROW)
    database.execute(action)
    with pytest.raises(RefreshTokenUnavailable) as caught:
        database.store().mark_used(PLAINTEXT, mutation())
    assert_sanitized(caught.value, "")
    assert database.rows() == [ROW]
    assert database.query("SELECT value FROM canary") == []
    assert database.commits == 0
    assert database.rollbacks == 1
    database.assert_closed()


@pytest.mark.parametrize("case", ["eligible", "stale", "inactive", "large-caller"])
def test_limite_sqlite_nao_corrompe_versao_nem_restringe_inteiro_do_chamador(
    database: Database,
    case: str,
) -> None:
    row = (*ROW[:8], 9223372036854775807)
    database.seed(row)
    version = {
        "eligible": 9223372036854775807,
        "inactive": 9223372036854775807,
        "stale": 7,
        "large-caller": 9223372036854775808,
    }[case]
    now = datetime(2026, 1, 2, 3, 4, 6, 654321, tzinfo=UTC) if case == "inactive" else NOW
    if case == "eligible":
        with pytest.raises(RefreshTokenUnavailable) as caught:
            database.store().revoke(PLAINTEXT, mutation(now, version))
        assert_sanitized(caught.value, "")
    else:
        assert database.store().revoke(PLAINTEXT, mutation(now, version)) is False
    assert database.rows() == [row]
    assert database.commits == 0
    assert database.rollbacks == 1
    database.assert_closed()


@pytest.mark.parametrize("mode", ["autocommit-true", "autocommit-false", "active"])
def test_recusa_conexao_fora_do_contrato_sem_adotar_transacao(
    database: Database,
    mode: str,
) -> None:
    database.seed(ROW)
    if mode == "active":
        database.active = True
    else:
        database.mode = mode == "autocommit-true"
    with pytest.raises(RefreshTokenUnavailable) as caught:
        database.store().mark_used(PLAINTEXT, mutation())
    assert_sanitized(caught.value, "")
    assert database.rows() == [ROW]
    assert database.query("SELECT value FROM canary") == []
    assert database.commits == 0
    assert database.sql == []
    forbidden = ("BEGIN", "SELECT", "UPDATE", "COMMIT")
    assert not any(sql.startswith(forbidden) for sql in database.trace)
    database.assert_closed()
