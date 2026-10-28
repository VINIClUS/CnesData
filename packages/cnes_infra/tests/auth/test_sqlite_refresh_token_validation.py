"""Validação de refresh por tenant e tempo explícitos no SQLite real."""

import logging
import sqlite3
from collections.abc import Generator
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from traceback import format_exception
from typing import cast
from uuid import UUID

import pytest

from cnes_domain.ports.refresh_tokens import RefreshTokenStore
from cnes_domain.refresh_tokens import (
    RefreshTokenContext,
    RefreshTokenInvalid,
    RefreshTokenRecord,
    RefreshTokenUnavailable,
)
from cnes_infra.auth.sqlite_refresh_tokens import SQLiteRefreshTokenStore

_PLAINTEXT = "abc"
_DIGEST = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
_SPACE_DIGEST = "36a9e7f1c95b82ffb99743e0c5c4ce95d83c9a430aac59f84ef3cbfab6145068"
_TENANT = "11111111-1111-4111-8111-111111111111"
_OTHER_TENANT = "22222222-2222-4222-8222-222222222222"
_AGENT = "33333333-3333-4333-8333-333333333333"
_CANARY = "synthetic-validation-error-canary"
_FINGERPRINT = "synthetic-row-fingerprint-canary"
_RAW_FAILURE = f"{_CANARY}:{_PLAINTEXT}:{_FINGERPRINT}"
_ISSUED = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
_EXPIRES = datetime(2026, 1, 3, 3, 4, 5, 654321, tzinfo=UTC)
_NOW = datetime(2026, 1, 2, 6, 0, 0, tzinfo=UTC)
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
_OTHER_ROW = (
    _DIGEST,
    "44444444-4444-4444-8444-444444444444",
    _OTHER_TENANT,
    "other-host",
    "2026-01-02T03:04:05.123456+00:00",
    "2026-01-03T03:04:05.654321+00:00",
    None,
    None,
    2,
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
)
"""
_INSERT = "INSERT INTO auth_refresh_tokens VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
_SELECT = (
    "SELECT token_sha256, agent_id, tenant_id, machine_fingerprint, issued_at, "
    "expires_at, last_used_at, revoked_at, version FROM auth_refresh_tokens "
    "ORDER BY tenant_id, token_sha256"
)
_WRITES = {
    sqlite3.SQLITE_INSERT,
    sqlite3.SQLITE_UPDATE,
    sqlite3.SQLITE_DELETE,
    sqlite3.SQLITE_CREATE_TABLE,
    sqlite3.SQLITE_DROP_TABLE,
    sqlite3.SQLITE_ALTER_TABLE,
    sqlite3.SQLITE_CREATE_INDEX,
    sqlite3.SQLITE_DROP_INDEX,
}


class _ReadFailureConnection(sqlite3.Connection):
    def execute(self, sql: str, parameters: object = (), /) -> sqlite3.Cursor:
        raise sqlite3.OperationalError(_RAW_FAILURE)


class _Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.attempts = 0
        self.secret_calls = 0
        self.connections: list[sqlite3.Connection] = []
        self.trace: list[str] = []
        self.writes: list[int] = []
        self.failure = ""

    def seed(self, *rows: tuple[object, ...]) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.execute(_SCHEMA)
            connection.executemany(_INSERT, rows)
        connection.close()

    def connect(self) -> sqlite3.Connection:
        self.attempts += 1
        if self.failure == "factory":
            raise sqlite3.OperationalError(_RAW_FAILURE)
        factory = _ReadFailureConnection if self.failure == "read" else sqlite3.Connection
        connection = sqlite3.connect(self.path, factory=factory)
        self.connections.append(connection)
        connection.set_trace_callback(self.trace.append)
        connection.set_authorizer(lambda action, *_: self.authorize(action))
        return connection

    def authorize(self, action: int) -> int:
        if action in _WRITES:
            self.writes.append(action)
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    def secret(self) -> str:
        self.secret_calls += 1
        pytest.fail("token_factory_called_on_validate")

    def store(self) -> RefreshTokenStore:
        return SQLiteRefreshTokenStore(self.connect, self.secret)

    def rows(self) -> list[tuple[object, ...]]:
        connection = sqlite3.connect(self.path)
        try:
            return connection.execute(_SELECT).fetchall()
        finally:
            connection.close()

    def assert_read_only_closed(self) -> None:
        assert self.attempts == len(self.connections)
        assert self.connections
        assert self.writes == []
        assert self.secret_calls == 0
        assert _PLAINTEXT not in "\n".join(self.trace)
        for connection in self.connections:
            with pytest.raises(sqlite3.ProgrammingError):
                sqlite3.Connection.execute(connection, "SELECT 1")


@pytest.fixture
def database(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> Generator[_Database]:
    caplog.set_level(logging.DEBUG)
    boundary = _Database(tmp_path / "refresh-validation.sqlite")
    try:
        yield boundary
    finally:
        for connection in boundary.connections:
            connection.close()


def _expected() -> RefreshTokenRecord:
    return RefreshTokenRecord(
        token_sha256=_DIGEST,
        agent_id=_AGENT,
        tenant_id=_TENANT,
        machine_fingerprint=_FINGERPRINT,
        issued_at=datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC),
        expires_at=datetime(2026, 1, 3, 3, 4, 5, 654321, tzinfo=UTC),
        last_used_at=datetime(2026, 1, 2, 4, 5, 6, 234567, tzinfo=UTC),
        revoked_at=None,
        version=7,
    )


def _assert_sanitized(error: Exception, observations: str) -> None:
    public = str(error) + repr(error) + "".join(format_exception(error)) + observations
    for sensitive in (_PLAINTEXT, _CANARY, _FINGERPRINT):
        assert sensitive not in public
    assert error.__cause__ is None
    assert error.__context__ is None or error.__suppress_context__


def test_releitura_reutiliza_registro_completo_sem_alterar_uso_prazo_ou_versao(
    database: _Database,
    caplog: pytest.LogCaptureFixture,
) -> None:
    database.seed(_ROW)
    context = RefreshTokenContext(_TENANT, _NOW)
    assert database.store().validate(_PLAINTEXT, context) == _expected()
    second_store = database.store()
    assert second_store.validate(_PLAINTEXT, context) == _expected()
    assert database.rows() == [_ROW]
    assert _PLAINTEXT not in repr(_expected()) + repr(context) + caplog.text
    database.assert_read_only_closed()
    assert len(database.connections) == 2


@pytest.mark.parametrize("now", [_ISSUED, _EXPIRES - timedelta(microseconds=1)])
def test_aceita_emissao_inclusiva_e_ultimo_microssegundo_antes_do_prazo(
    database: _Database,
    now: datetime,
) -> None:
    database.seed(_ROW)
    assert database.store().validate(_PLAINTEXT, RefreshTokenContext(_TENANT, now)) == _expected()
    assert database.rows() == [_ROW]
    database.assert_read_only_closed()


@pytest.mark.parametrize(
    ("now", "revoked"),
    [
        pytest.param(_ISSUED - timedelta(microseconds=1), None, id="antes-emissao"),
        pytest.param(_EXPIRES, None, id="prazo-exato"),
        pytest.param(_EXPIRES + timedelta(microseconds=1), None, id="expirado"),
        pytest.param(_NOW, "2026-01-02T05:00:00.345678+00:00", id="revogado"),
    ],
)
def test_inativo_retorna_ausencia_mantendo_os_nove_campos_fisicamente_presentes(
    database: _Database,
    now: datetime,
    revoked: str | None,
) -> None:
    row = (*_ROW[:7], revoked, 7)
    database.seed(row)
    assert database.store().validate(_PLAINTEXT, RefreshTokenContext(_TENANT, now)) is None
    assert database.rows() == [row]
    database.assert_read_only_closed()


def test_mesmo_hash_em_dois_tenants_retorna_somente_o_registro_do_contexto(
    database: _Database,
) -> None:
    database.seed(_ROW, _OTHER_ROW)
    store = database.store()
    assert store.validate(_PLAINTEXT, RefreshTokenContext(_TENANT, _NOW)) == _expected()
    other_expected = RefreshTokenRecord(
        token_sha256=_DIGEST,
        agent_id="44444444-4444-4444-8444-444444444444",
        tenant_id=_OTHER_TENANT,
        machine_fingerprint="other-host",
        issued_at=datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC),
        expires_at=datetime(2026, 1, 3, 3, 4, 5, 654321, tzinfo=UTC),
        last_used_at=None,
        revoked_at=None,
        version=2,
    )
    assert store.validate(_PLAINTEXT, RefreshTokenContext(_OTHER_TENANT, _NOW)) == other_expected
    for tenant in (_TENANT, _OTHER_TENANT):
        assert any(tenant in sql and _DIGEST in sql for sql in database.trace)
    assert database.rows() == [_ROW, _OTHER_ROW]
    database.assert_read_only_closed()


@pytest.mark.parametrize(
    ("token", "tenant"),
    [
        pytest.param(_PLAINTEXT, _OTHER_TENANT, id="tenant-errado"),
        pytest.param("opaque-unknown'; SELECT 1;--", _TENANT, id="hash-ausente"),
    ],
)
def test_identidade_desconhecida_retorna_none_com_contexto_valido(
    database: _Database,
    token: str,
    tenant: str,
) -> None:
    database.seed(_ROW)
    assert database.store().validate(token, RefreshTokenContext(tenant, _NOW)) is None
    assert database.rows() == [_ROW]
    assert token not in "\n".join(database.trace)
    database.assert_read_only_closed()


def test_segredo_whitespace_e_opaco_e_nao_e_removido_ou_rejeitado(database: _Database) -> None:
    row = (_SPACE_DIGEST, *_ROW[1:])
    database.seed(row)
    assert database.store().validate(" ", RefreshTokenContext(_TENANT, _NOW)) == replace(
        _expected(),
        token_sha256=_SPACE_DIGEST,
    )
    assert database.rows() == [row]
    database.assert_read_only_closed()


def test_leitura_aceita_utc_valido_sem_exigir_a_grafia_canonica_do_writer(
    database: _Database,
) -> None:
    row = (*_ROW[:4], "2026-01-02T03:04:05.123456Z", "2026-01-03 03:04:05.654321+00:00", *_ROW[6:])
    database.seed(row)
    assert database.store().validate(_PLAINTEXT, RefreshTokenContext(_TENANT, _NOW)) == _expected()
    assert database.rows() == [row]
    database.assert_read_only_closed()


@pytest.mark.parametrize("token", [None, b"abc", 7, ""])
def test_token_invalido_e_rejeitado_antes_de_abrir_conexao(
    database: _Database,
    token: object,
) -> None:
    with pytest.raises(RefreshTokenInvalid):
        database.store().validate(cast("str", token), RefreshTokenContext(_TENANT, _NOW))
    assert database.attempts == 0
    assert database.secret_calls == 0


@pytest.mark.parametrize(
    "tenant",
    [
        "not-a-uuid",
        "11111111111141118111111111111111",
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        UUID(_TENANT),
    ],
)
def test_tenant_nao_canonico_nao_e_coagido_e_falha_antes_da_conexao(
    database: _Database,
    tenant: object,
) -> None:
    context = RefreshTokenContext(cast("str", tenant), _NOW)
    with pytest.raises(RefreshTokenInvalid):
        database.store().validate(_PLAINTEXT, context)
    assert database.attempts == 0
    assert database.secret_calls == 0


@pytest.mark.parametrize(
    "now",
    [
        _NOW.replace(tzinfo=None),
        _NOW.replace(tzinfo=timezone(timedelta(hours=1))),
        "2026-01-02T06:00:00+00:00",
        None,
    ],
)
def test_relogio_invalido_e_rejeitado_antes_de_abrir_conexao(
    database: _Database,
    now: object,
) -> None:
    context = RefreshTokenContext(_TENANT, cast("datetime", now))
    with pytest.raises(RefreshTokenInvalid):
        database.store().validate(_PLAINTEXT, context)
    assert database.attempts == 0
    assert database.secret_calls == 0


@pytest.mark.parametrize(
    "corruption",
    [
        pytest.param((4, _CANARY), id="timestamp-ilegivel"),
        pytest.param((1, "not-a-uuid"), id="identidade-invalida"),
        pytest.param((1, b"33333333-3333-4333-8333-333333333333"), id="identidade-blob"),
        pytest.param((8, -1), id="versao-negativa"),
        pytest.param((8, "invalid-version"), id="versao-texto"),
        pytest.param((6, "2026-01-03T03:04:05.654322+00:00"), id="uso-apos-prazo"),
        pytest.param((7, "2026-01-02T03:04:05.123455+00:00"), id="revogacao-antes-emissao"),
    ],
)
def test_linha_corrompida_selecionada_e_unavailable_mesmo_quando_inativa(
    database: _Database,
    corruption: tuple[int, object],
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    column, value = corruption
    row: list[object] = list(_ROW)
    row[column] = value
    database.seed(tuple(row), _OTHER_ROW)
    store = database.store()
    assert store.validate(_PLAINTEXT, RefreshTokenContext(_OTHER_TENANT, _NOW)) is not None
    with pytest.raises(RefreshTokenUnavailable) as caught:
        store.validate(_PLAINTEXT, RefreshTokenContext(_TENANT, _EXPIRES))
    assert database.rows() == [tuple(row), _OTHER_ROW]
    captured = capsys.readouterr()
    _assert_sanitized(caught.value, caplog.text + captured.out + captured.err)
    database.assert_read_only_closed()


@pytest.mark.parametrize("origin", ["factory", "read"])
def test_falha_de_fronteira_e_sanitizada_e_conexao_entregue_e_fechada(
    database: _Database,
    origin: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database.seed(_ROW)
    database.failure = origin
    with pytest.raises(RefreshTokenUnavailable) as caught:
        database.store().validate(_PLAINTEXT, RefreshTokenContext(_TENANT, _NOW))
    assert database.attempts == 1
    assert database.rows() == [_ROW]
    assert database.secret_calls == 0
    assert database.writes == []
    assert len(database.connections) == (1 if origin == "read" else 0)
    if origin == "read":
        connection = database.connections[0]
        with pytest.raises(sqlite3.ProgrammingError):
            sqlite3.Connection.execute(connection, "SELECT 1")
    captured = capsys.readouterr()
    _assert_sanitized(caught.value, caplog.text + captured.out + captured.err)
    assert _PLAINTEXT not in "\n".join(database.trace)
