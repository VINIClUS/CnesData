"""Falhas externas à identidade do refresh permanecem indisponibilidade."""

import logging
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from traceback import format_exception

import pytest

from cnes_domain.refresh_tokens import (
    RefreshTokenInput,
    RefreshTokenRecord,
    RefreshTokenUnavailable,
)
from cnes_infra.auth.sqlite_refresh_tokens import SQLiteRefreshTokenStore

_PLAINTEXT = "synthetic-refresh-secret-ONLY"
_CANARY = "synthetic_refresh_failure_canary"
_DIGEST = "b5a004605ca635e03bd70434f90603c27a354fa0b585b7fac11050033b01be5e"
_SEED = (
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "44444444-4444-4444-8444-444444444444",
    "11111111-1111-4111-8111-111111111111",
    "seed-host",
    "2026-01-02T03:04:05.123456+00:00",
    "2026-01-03T03:04:05.654321+00:00",
    "2026-01-02T04:05:06.234567+00:00",
    "2026-01-04T05:06:07.345678+00:00",
    7,
)
_SCHEMA = """
CREATE TABLE auth_refresh_tokens (
    token_sha256 TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    machine_fingerprint TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    last_used_at TEXT,
    revoked_at TEXT,
    version INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, token_sha256)
);
CREATE TABLE synthetic_refresh_failure_canary (key INTEGER PRIMARY KEY, state TEXT NOT NULL);
INSERT INTO synthetic_refresh_failure_canary VALUES (1, 'original');
"""
_TRIGGER = """
CREATE TRIGGER fail_auxiliary BEFORE INSERT ON auth_refresh_tokens BEGIN
    UPDATE auth_refresh_tokens SET machine_fingerprint = 'corrupt';
    UPDATE synthetic_refresh_failure_canary SET state = 'corrupt';
    INSERT OR FAIL INTO synthetic_refresh_failure_canary VALUES (1, 'duplicate');
END
"""
_SELECT = (
    "SELECT token_sha256, agent_id, tenant_id, machine_fingerprint, issued_at, "
    "expires_at, last_used_at, revoked_at, version FROM auth_refresh_tokens "
    "ORDER BY tenant_id, token_sha256"
)


@dataclass(frozen=True, slots=True)
class _Snapshot:
    refresh: list[tuple[object, ...]]
    auxiliary: list[tuple[object, ...]]


class _Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.attempts = 0
        self.delivered = 0
        self.connections: list[sqlite3.Connection] = []
        self.trace: list[str] = []

    def prepare(self, origin: str) -> None:
        connection = sqlite3.connect(self.path)
        try:
            connection.executescript(_SCHEMA)
            connection.execute(
                "INSERT INTO auth_refresh_tokens VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", _SEED
            )
            if origin == "trigger-auxiliary-pk":
                connection.execute(_TRIGGER)
            connection.commit()
        finally:
            connection.close()

    def connect(self, origin: str) -> sqlite3.Connection:
        self.attempts += 1
        if origin == "factory-integrity-without-metadata":
            raise sqlite3.IntegrityError(_CANARY)
        connection = sqlite3.connect(self.path)
        self.connections.append(connection)
        connection.set_trace_callback(self.trace.append)
        if origin == "factory-native-pk":
            try:
                connection.execute("UPDATE synthetic_refresh_failure_canary SET state = 'corrupt'")
                connection.execute(
                    "INSERT OR FAIL INTO synthetic_refresh_failure_canary VALUES (1, 'duplicate')"
                )
            finally:
                connection.close()
        self.delivered += 1
        return connection

    def snapshot(self) -> _Snapshot:
        connection = sqlite3.connect(self.path)
        try:
            return _Snapshot(
                connection.execute(_SELECT).fetchall(),
                connection.execute(
                    "SELECT key, state FROM synthetic_refresh_failure_canary ORDER BY key"
                ).fetchall(),
            )
        finally:
            connection.close()

    def assert_closed(self, origin: str) -> None:
        assert len(self.connections) == (0 if origin == "factory-integrity-without-metadata" else 1)
        for connection in self.connections:
            with pytest.raises(sqlite3.ProgrammingError):
                connection.execute("SELECT 1")


class _SecretFactory:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> str:
        self.calls += 1
        return _PLAINTEXT


@pytest.fixture
def database(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.DEBUG)
    boundary = _Database(tmp_path / "failure-origins.sqlite")
    try:
        yield boundary
    finally:
        for connection in boundary.connections:
            connection.close()


def _input() -> RefreshTokenInput:
    return RefreshTokenInput(
        agent_id="33333333-3333-4333-8333-333333333333",
        tenant_id="11111111-1111-4111-8111-111111111111",
        machine_fingerprint="candidate-host",
        issued_at=datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC),
        expires_at=datetime(2026, 1, 3, 3, 4, 5, 654321, tzinfo=UTC),
    )


def _record() -> RefreshTokenRecord:
    return RefreshTokenRecord(
        token_sha256=_DIGEST,
        agent_id="33333333-3333-4333-8333-333333333333",
        tenant_id="11111111-1111-4111-8111-111111111111",
        machine_fingerprint="candidate-host",
        issued_at=datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC),
        expires_at=datetime(2026, 1, 3, 3, 4, 5, 654321, tzinfo=UTC),
        last_used_at=None,
        revoked_at=None,
        version=0,
    )


def _invoke(store: SQLiteRefreshTokenStore, operation: str) -> None:
    if operation == "create":
        store.create(_input())
    else:
        store.import_hashed(_record())


@pytest.mark.parametrize("operation", ["create", "import"])
@pytest.mark.parametrize(
    "origin",
    ["trigger-auxiliary-pk", "factory-native-pk", "factory-integrity-without-metadata"],
)
def test_falha_fora_da_identidade_vira_unavailable_e_preserva_estado(
    database: _Database,
    origin: str,
    operation: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    database.prepare(origin)
    assert database.snapshot() == _Snapshot([_SEED], [(1, "original")])
    factory = _SecretFactory()
    store = SQLiteRefreshTokenStore(lambda: database.connect(origin), factory)
    with pytest.raises(Exception) as caught:
        _invoke(store, operation)
    assert database.attempts == 1
    assert database.delivered == (1 if origin == "trigger-auxiliary-pk" else 0)
    assert database.snapshot() == _Snapshot([_SEED], [(1, "original")])
    assert factory.calls == (1 if operation == "create" else 0)
    database.assert_closed(origin)
    if origin != "trigger-auxiliary-pk":
        assert not any("auth_refresh_tokens" in statement.lower() for statement in database.trace)
    assert isinstance(caught.value, RefreshTokenUnavailable)
    for sensitive in (_PLAINTEXT, _CANARY):
        assert sensitive not in str(caught.value)
        assert sensitive not in "".join(format_exception(caught.value))
        assert sensitive not in caplog.text
