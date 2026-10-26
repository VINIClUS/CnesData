"""Criação e importação de refresh no SQLite com persistência real."""

import logging
import sqlite3
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from traceback import format_exception

import pytest

from cnes_domain.ports.refresh_tokens import RefreshTokenStore
from cnes_domain.refresh_tokens import (
    RefreshTokenConflict,
    RefreshTokenInput,
    RefreshTokenInvalid,
    RefreshTokenRecord,
    RefreshTokenUnavailable,
)
from cnes_infra.auth.sqlite_refresh_tokens import SQLiteRefreshTokenStore

_PLAINTEXT = "synthetic-refresh-secret-ONLY"
_DIGEST = "b5a004605ca635e03bd70434f90603c27a354fa0b585b7fac11050033b01be5e"
_TENANT = "11111111-1111-4111-8111-111111111111"
_OTHER_TENANT = "22222222-2222-4222-8222-222222222222"
_AGENT = "33333333-3333-4333-8333-333333333333"
_FINGERPRINT = "host'; DROP TABLE auth_refresh_tokens;--"
_ISSUED = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
_EXPIRES = datetime(2026, 1, 3, 3, 4, 5, 654321, tzinfo=UTC)
_LAST_USED = datetime(2026, 1, 2, 4, 5, 6, 234567, tzinfo=UTC)
_REVOKED = datetime(2026, 1, 4, 5, 6, 7, 345678, tzinfo=UTC)
_CREATED_ROW = (
    _DIGEST,
    _AGENT,
    _TENANT,
    _FINGERPRINT,
    "2026-01-02T03:04:05.123456+00:00",
    "2026-01-03T03:04:05.654321+00:00",
    None,
    None,
    0,
)
_IMPORTED_ROW = (
    _DIGEST,
    _AGENT,
    _TENANT,
    "imported-host",
    "2026-01-02T03:04:05.123456+00:00",
    "2026-01-03T03:04:05.654321+00:00",
    "2026-01-02T04:05:06.234567+00:00",
    "2026-01-04T05:06:07.345678+00:00",
    7,
)
_SELECT = (
    "SELECT token_sha256, agent_id, tenant_id, machine_fingerprint, issued_at, "
    "expires_at, last_used_at, revoked_at, version FROM auth_refresh_tokens "
    "ORDER BY tenant_id, token_sha256"
)
_INSERT = "INSERT INTO auth_refresh_tokens VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
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
)
"""


class _SQLiteBoundary:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.attempts = 0
        self.connections: list[sqlite3.Connection] = []
        self.trace: list[str] = []
        self.fail = False

    def connect(self) -> sqlite3.Connection:
        self.attempts += 1
        if self.fail:
            raise sqlite3.OperationalError(_PLAINTEXT)
        connection = sqlite3.connect(self.path)
        connection.set_trace_callback(self.trace.append)
        self.connections.append(connection)
        return connection

    def run(self, sql: str, parameters: tuple[object, ...] = ()) -> None:
        connection = sqlite3.connect(self.path)
        try:
            connection.execute(sql, parameters)
            connection.commit()
        finally:
            connection.close()

    def rows(self) -> list[tuple[object, ...]]:
        connection = sqlite3.connect(self.path)
        try:
            return connection.execute(_SELECT).fetchall()
        finally:
            connection.close()

    def assert_closed(self) -> None:
        assert self.connections
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
    boundary = _SQLiteBoundary(tmp_path / "refresh.sqlite")
    boundary.run(_SCHEMA)
    try:
        yield boundary
    finally:
        for connection in boundary.connections:
            connection.close()


def _input() -> RefreshTokenInput:
    return RefreshTokenInput(
        agent_id=_AGENT,
        tenant_id=_TENANT,
        machine_fingerprint=_FINGERPRINT,
        issued_at=_ISSUED,
        expires_at=_EXPIRES,
    )


def _record() -> RefreshTokenRecord:
    return RefreshTokenRecord(
        token_sha256=_DIGEST,
        agent_id=_AGENT,
        tenant_id=_TENANT,
        machine_fingerprint="imported-host",
        issued_at=_ISSUED,
        expires_at=_EXPIRES,
        last_used_at=_LAST_USED,
        revoked_at=_REVOKED,
        version=7,
    )


def _store(database: _SQLiteBoundary, factory: Callable[[], str]) -> RefreshTokenStore:
    return SQLiteRefreshTokenStore(database.connect, factory)


def _write(store: RefreshTokenStore, operation: str) -> None:
    if operation == "create":
        store.create(_input())
    else:
        store.import_hashed(replace(_record(), machine_fingerprint="replacement-host", version=8))


def _forbidden_secret() -> str:
    pytest.fail("token_factory_called_on_import")


def _failed_secret() -> str:
    raise RuntimeError(_PLAINTEXT)


def test_create_retorna_segredo_e_persiste_somente_hash_com_prazo_explicito(
    database: _SQLiteBoundary,
    caplog: pytest.LogCaptureFixture,
) -> None:
    factory = _SecretFactory()
    store = _store(database, factory)
    assert store.create(_input()) == _PLAINTEXT
    assert factory.calls == 1
    assert database.rows() == [_CREATED_ROW]
    assert _PLAINTEXT not in repr(database.rows())
    assert _PLAINTEXT not in repr(_input())
    assert _PLAINTEXT not in "\n".join(database.trace)
    assert _PLAINTEXT not in caplog.text
    assert _PLAINTEXT.encode() not in database.path.read_bytes()
    database.assert_closed()


def test_import_preserva_linha_completa_e_reabertura_observa_mesmo_registro(
    database: _SQLiteBoundary,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _store(database, _forbidden_secret).import_hashed(_record())
    assert database.rows() == [_IMPORTED_ROW]
    reopened = _SQLiteBoundary(database.path)
    with pytest.raises(RefreshTokenConflict):
        _store(reopened, _forbidden_secret).import_hashed(_record())
    assert reopened.rows() == [_IMPORTED_ROW]
    assert _PLAINTEXT not in repr(_record())
    assert _PLAINTEXT not in caplog.text
    database.assert_closed()
    reopened.assert_closed()


@pytest.mark.parametrize("operation", ["create", "import"])
def test_colisao_rejeita_sem_regenerar_ou_alterar_qualquer_campo(
    database: _SQLiteBoundary,
    operation: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    database.run(_INSERT, _IMPORTED_ROW)
    factory = _SecretFactory()
    with pytest.raises(RefreshTokenConflict) as caught:
        _write(_store(database, factory), operation)
    assert database.rows() == [_IMPORTED_ROW]
    assert factory.calls == (1 if operation == "create" else 0)
    assert _PLAINTEXT not in str(caught.value)
    assert _PLAINTEXT not in "\n".join(database.trace)
    assert _PLAINTEXT not in caplog.text
    database.assert_closed()


def test_mesmo_digest_em_outro_tenant_nao_colide(database: _SQLiteBoundary) -> None:
    database.run(_INSERT, _IMPORTED_ROW)
    record = replace(_record(), tenant_id=_OTHER_TENANT)
    _store(database, _forbidden_secret).import_hashed(record)
    assert database.rows() == [
        _IMPORTED_ROW,
        (
            _DIGEST,
            _AGENT,
            _OTHER_TENANT,
            "imported-host",
            "2026-01-02T03:04:05.123456+00:00",
            "2026-01-03T03:04:05.654321+00:00",
            "2026-01-02T04:05:06.234567+00:00",
            "2026-01-04T05:06:07.345678+00:00",
            7,
        ),
    ]
    database.assert_closed()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("tenant_id", "", id="tenant-vazio"),
        pytest.param("tenant_id", "not-a-uuid", id="tenant-invalido"),
        pytest.param("agent_id", "", id="agent-vazio"),
        pytest.param("agent_id", "33333333333343338333333333333333", id="agent-nao-canonico"),
        pytest.param("machine_fingerprint", "", id="fingerprint-vazio"),
        pytest.param("issued_at", _ISSUED.replace(tzinfo=None), id="issued-naive"),
        pytest.param(
            "expires_at",
            _EXPIRES.replace(tzinfo=timezone(timedelta(hours=1))),
            id="expires-offset-nao-zero",
        ),
        pytest.param("expires_at", _ISSUED, id="prazo-zero"),
        pytest.param("expires_at", _ISSUED - timedelta(microseconds=1), id="prazo-negativo"),
    ],
)
def test_create_rejeita_entrada_invalida_antes_de_abrir_conexao(
    database: _SQLiteBoundary,
    field: str,
    value: object,
) -> None:
    factory = _SecretFactory()
    database.fail = True
    candidate = replace(_input(), **{field: value})
    with pytest.raises(RefreshTokenInvalid):
        _store(database, factory).create(candidate)
    assert database.attempts == 0
    assert factory.calls == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("tenant_id", "not-a-uuid", id="tenant-invalido"),
        pytest.param("agent_id", "", id="agent-vazio"),
        pytest.param("machine_fingerprint", "", id="fingerprint-vazio"),
        pytest.param("issued_at", _ISSUED.replace(tzinfo=None), id="issued-naive"),
        pytest.param("expires_at", _ISSUED, id="prazo-zero"),
        pytest.param("token_sha256", _DIGEST.upper(), id="digest-uppercase"),
        pytest.param("token_sha256", "g" * 64, id="digest-nao-hex"),
        pytest.param("token_sha256", "a" * 63, id="digest-curto"),
        pytest.param("version", True, id="version-bool"),
        pytest.param("version", -1, id="version-negativa"),
        pytest.param("last_used_at", _LAST_USED.replace(tzinfo=None), id="last-naive"),
        pytest.param(
            "revoked_at",
            _REVOKED.replace(tzinfo=timezone(timedelta(hours=-1))),
            id="revoked-offset-nao-zero",
        ),
        pytest.param("last_used_at", _ISSUED - timedelta(microseconds=1), id="last-antes-issued"),
        pytest.param("last_used_at", _EXPIRES + timedelta(microseconds=1), id="last-apos-expiry"),
        pytest.param("revoked_at", _ISSUED - timedelta(microseconds=1), id="revoke-antes-issued"),
    ],
)
def test_import_rejeita_record_invalido_antes_de_abrir_conexao(
    database: _SQLiteBoundary,
    field: str,
    value: object,
) -> None:
    database.fail = True
    candidate = replace(_record(), **{field: value})
    with pytest.raises(RefreshTokenInvalid):
        _store(database, _forbidden_secret).import_hashed(candidate)
    assert database.attempts == 0


@pytest.mark.parametrize("operation", ["create", "import"])
def test_falha_da_factory_de_conexao_vira_unavailable_sem_segredo(
    database: _SQLiteBoundary,
    operation: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    database.run(_INSERT, _IMPORTED_ROW)
    database.fail = True
    with pytest.raises(RefreshTokenUnavailable) as caught:
        _write(_store(database, _SecretFactory()), operation)
    assert database.attempts == 1
    assert database.rows() == [_IMPORTED_ROW]
    assert _PLAINTEXT not in str(caught.value)
    assert _PLAINTEXT not in "".join(format_exception(caught.value))
    assert _PLAINTEXT not in caplog.text


@pytest.mark.parametrize("operation", ["create", "import"])
def test_falha_de_transacao_faz_rollback_e_preserva_linha_antiga(
    database: _SQLiteBoundary,
    operation: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    database.run(_INSERT, _IMPORTED_ROW)
    database.run(
        "CREATE TRIGGER reject_refresh BEFORE INSERT ON auth_refresh_tokens BEGIN "
        "UPDATE auth_refresh_tokens SET machine_fingerprint = 'corrupt'; "
        "SELECT RAISE(FAIL, 'synthetic-refresh-secret-ONLY'); END"
    )
    with pytest.raises(RefreshTokenUnavailable) as caught:
        _write(_store(database, _SecretFactory()), operation)
    assert database.rows() == [_IMPORTED_ROW]
    assert _PLAINTEXT not in str(caught.value)
    assert _PLAINTEXT not in "".join(format_exception(caught.value))
    assert _PLAINTEXT not in caplog.text
    database.assert_closed()


def test_falha_da_factory_de_segredo_vira_unavailable_sem_segredo(
    database: _SQLiteBoundary,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with pytest.raises(RefreshTokenUnavailable) as caught:
        _store(database, _failed_secret).create(_input())
    assert database.rows() == []
    assert _PLAINTEXT not in str(caught.value)
    assert _PLAINTEXT not in "".join(format_exception(caught.value))
    assert _PLAINTEXT not in caplog.text


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        pytest.param(
            replace(_record(), last_used_at=None, revoked_at=None, version=0),
            (
                _DIGEST,
                _AGENT,
                _TENANT,
                "imported-host",
                "2026-01-02T03:04:05.123456+00:00",
                "2026-01-03T03:04:05.654321+00:00",
                None,
                None,
                0,
            ),
            id="ativo-sem-uso-version-zero",
        ),
        pytest.param(
            replace(_record(), revoked_at=None, version=2),
            (
                _DIGEST,
                _AGENT,
                _TENANT,
                "imported-host",
                "2026-01-02T03:04:05.123456+00:00",
                "2026-01-03T03:04:05.654321+00:00",
                "2026-01-02T04:05:06.234567+00:00",
                None,
                2,
            ),
            id="usado-sem-revogacao",
        ),
        pytest.param(
            replace(_record(), last_used_at=None, version=3),
            (
                _DIGEST,
                _AGENT,
                _TENANT,
                "imported-host",
                "2026-01-02T03:04:05.123456+00:00",
                "2026-01-03T03:04:05.654321+00:00",
                None,
                "2026-01-04T05:06:07.345678+00:00",
                3,
            ),
            id="revogado-sem-uso",
        ),
    ],
)
def test_import_preserva_timestamps_opcionais_nulos_e_reabertura(
    database: _SQLiteBoundary,
    record: RefreshTokenRecord,
    expected: tuple[object, ...],
) -> None:
    _store(database, _forbidden_secret).import_hashed(record)
    assert database.rows() == [expected]
    reopened = _SQLiteBoundary(database.path)
    with pytest.raises(RefreshTokenConflict):
        _store(reopened, _forbidden_secret).import_hashed(record)
    assert reopened.rows() == [expected]
    database.assert_closed()
    reopened.assert_closed()
