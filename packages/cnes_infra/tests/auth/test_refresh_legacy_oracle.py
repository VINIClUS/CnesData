"""Oráculo sintético do ciclo de vida do refresh legado."""

from __future__ import annotations

import datetime as dt
import shutil
import subprocess
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import Engine, create_engine, text

from cnes_domain.tenant import tenant_id_ctx
from cnes_infra.auth.refresh_tokens import RefreshTokenStore
from cnes_infra.storage.rls import install_rls_listener

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.postgres

_IMAGE = "postgres:16-alpine"
_COMMAND_TIMEOUT = 20
_ADMIN_USER = "cnesdata"
_ADMIN_VALUE = "cnesdata_test"
_DATABASE = "cnesdata_test"
_APP_USER = "legacy_reader"
_APP_VALUE = "legacy_reader_test"
_TENANT_A = "tenant-a"
_TENANT_B = "tenant-b"
_OLD = dt.datetime(2020, 1, 2, 3, 4, 5, tzinfo=dt.UTC)
_PLAINTEXT_A = "legacy-refresh-token-a"
_PLAINTEXT_C = "legacy-refresh-token-c"
_HASH_A = "5469238bd17b7ca5423cba42e9dc39af8ef936c010e7eaac827b2c0a7e634a06"
_HASH_B = "e3b4bf3ca58931f731aade35a99c3b7347693687c9120ac4ed1ff2cc25dc08dd"
_HASH_C = "1c2aef7ffb62e9d20a7581b81f74b015cb01eaed05344962dd63c1e89f3c5ced"
_HASH_D = "05fc27ccac3679cdc61b57cdefc42a93b7dee83d48d7162958e543e41bfe7d36"


@dataclass(frozen=True)
class _PostgresTarget:
    admin_url: str
    app_url: str


@dataclass(frozen=True)
class _SeedRow:
    token_sha256: str
    agent_id: str
    tenant_id: str
    machine_fingerprint: str
    revoked_at: dt.datetime | None = None


@dataclass(frozen=True)
class _LegacyState:
    token_sha256: str
    agent_id: str
    tenant_id: str
    machine_fingerprint: str
    issued_at: dt.datetime
    last_used_at: dt.datetime | None
    revoked_at: dt.datetime | None


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(  # noqa: S603
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=_COMMAND_TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        stderr = exc.stderr if isinstance(exc.stderr, str) else "timeout"
        return subprocess.CompletedProcess(command, 124, "", stderr)


def _wait_for_postgres(container: str, deadline: float) -> None:
    while time.monotonic() < deadline:
        result = _run(
            [
                "docker",
                "exec",
                container,
                "pg_isready",
                "-U",
                _ADMIN_USER,
                "-d",
                _DATABASE,
            ],
        )
        if result.returncode == 0:
            return
        time.sleep(0.2)
    raise RuntimeError(f"postgres não ficou pronto container={container}")


def _port(container: str) -> int:
    result = _run(["docker", "port", container, "5432/tcp"])
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return int(result.stdout.strip().rsplit(":", 1)[1])


def _admin_url(port: int) -> str:
    return f"postgresql+psycopg://{_ADMIN_USER}:{_ADMIN_VALUE}@127.0.0.1:{port}/{_DATABASE}"


def _app_url(port: int) -> str:
    return f"postgresql+psycopg://{_APP_USER}:{_APP_VALUE}@127.0.0.1:{port}/{_DATABASE}"


def _create_legacy_table(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE auth_refresh_tokens ("
                "token_sha256 VARCHAR(64) PRIMARY KEY, "
                "agent_id VARCHAR(64) NOT NULL, "
                "tenant_id VARCHAR(36) NOT NULL, "
                "machine_fingerprint VARCHAR(128) NOT NULL, "
                "issued_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
                "last_used_at TIMESTAMPTZ, revoked_at TIMESTAMPTZ"
                ")",
            ),
        )
        connection.execute(text("ALTER TABLE auth_refresh_tokens ENABLE ROW LEVEL SECURITY"))
        connection.execute(
            text(
                "CREATE POLICY auth_refresh_tokens_tenant_isolation "
                "ON auth_refresh_tokens FOR ALL "
                "USING (tenant_id = current_setting('app.tenant_id', true))",
            ),
        )
        connection.execute(text("GRANT USAGE ON SCHEMA public TO legacy_reader"))
        connection.execute(
            text(
                "GRANT SELECT, INSERT, UPDATE ON TABLE auth_refresh_tokens "
                "TO legacy_reader",
            ),
        )


def _wait_for_connection(engine: Engine, deadline: float) -> None:
    while time.monotonic() < deadline:
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("conexão administrativa Postgres não ficou pronta")


def _make_app_engine(url: str) -> Engine:
    engine = create_engine(url, pool_pre_ping=True)
    install_rls_listener(engine)
    return engine


@contextmanager
def _tenant(tenant_id: str) -> Iterator[None]:
    token = tenant_id_ctx.set(tenant_id)
    try:
        yield
    finally:
        tenant_id_ctx.reset(token)


def _literal_row(spec: _SeedRow) -> dict[str, object]:
    return {
        "token_sha256": spec.token_sha256,
        "agent_id": spec.agent_id,
        "tenant_id": spec.tenant_id,
        "machine_fingerprint": spec.machine_fingerprint,
        "issued_at": _OLD,
        "last_used_at": None,
        "revoked_at": spec.revoked_at,
    }


def _start_container(container: str, label: str) -> None:
    result = _run(
        [
            "docker",
            "run",
            "--detach",
            "--name",
            container,
            "--label",
            label,
            "--env",
            f"POSTGRES_USER={_ADMIN_USER}",
            "--env",
            f"POSTGRES_PASSWORD={_ADMIN_VALUE}",
            "--env",
            f"POSTGRES_DB={_DATABASE}",
            "--publish",
            "127.0.0.1::5432",
            "--pull=never",
            _IMAGE,
        ],
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())


def _prepare_database(container: str, port: int) -> tuple[_PostgresTarget, Engine]:
    admin_engine = create_engine(_admin_url(port), pool_pre_ping=True)
    _wait_for_connection(admin_engine, time.monotonic() + 15)
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "CREATE ROLE legacy_reader LOGIN PASSWORD 'legacy_reader_test' "
                "NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT",
            ),
        )
    _create_legacy_table(admin_engine)
    return _PostgresTarget(_admin_url(port), _app_url(port)), admin_engine


def _cleanup_container(
    container: str,
    label: str,
    admin_engine: Engine | None,
    started: bool,
) -> None:
    if admin_engine is not None:
        admin_engine.dispose()
    cleanup_errors: list[str] = []
    if started:
        removed = _run(["docker", "rm", "--force", container])
        if removed.returncode != 0:
            cleanup_errors.append(f"rm exit={removed.returncode} stderr={removed.stderr.strip()}")
    inventory = _run(
        ["docker", "ps", "--all", "--filter", f"label={label}", "--format", "{{.Names}}"],
    )
    if inventory.returncode != 0:
        cleanup_errors.append(
            f"ps exit={inventory.returncode} stderr={inventory.stderr.strip()}"
        )
    elif inventory.stdout.strip():
        cleanup_errors.append(f"container órfão label={label}")
    if cleanup_errors:
        raise AssertionError("; ".join(cleanup_errors))


@pytest.fixture(scope="module")
def postgres_target() -> Iterator[_PostgresTarget]:
    if shutil.which("docker") is None:
        pytest.skip("docker indisponível para o Postgres local do oráculo")
    suffix = uuid.uuid4().hex
    container = f"cnes-refresh-oracle-{suffix}"
    label = f"cnes-refresh-oracle={suffix}"
    admin_engine: Engine | None = None
    started = False
    try:
        _start_container(container, label)
        started = True
        _wait_for_postgres(container, time.monotonic() + 30)
        port = _port(container)
        target, admin_engine = _prepare_database(container, port)
        yield target
    finally:
        _cleanup_container(container, label, admin_engine, started)


@pytest.fixture
def stores(
    postgres_target: _PostgresTarget,
) -> Iterator[tuple[RefreshTokenStore, RefreshTokenStore, Engine]]:
    admin_engine = create_engine(postgres_target.admin_url, pool_pre_ping=True)
    with admin_engine.begin() as connection:
        connection.execute(text("DELETE FROM auth_refresh_tokens"))
    engine_a = _make_app_engine(postgres_target.app_url)
    engine_b = _make_app_engine(postgres_target.app_url)
    try:
        yield RefreshTokenStore(engine_a), RefreshTokenStore(engine_b), admin_engine
    finally:
        engine_a.dispose()
        engine_b.dispose()
        admin_engine.dispose()


def _seed(engine: Engine, rows: tuple[_SeedRow, ...]) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO auth_refresh_tokens "
                "(token_sha256, agent_id, tenant_id, machine_fingerprint, issued_at, "
                "last_used_at, revoked_at) VALUES "
                "(:token_sha256, :agent_id, :tenant_id, :machine_fingerprint, :issued_at, "
                ":last_used_at, :revoked_at)",
            ),
            [_literal_row(row) for row in rows],
        )


def _readback(engine: Engine, token_sha256: str) -> _LegacyState:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT token_sha256, agent_id, tenant_id, machine_fingerprint, issued_at, "
                "last_used_at, revoked_at FROM auth_refresh_tokens "
                "WHERE token_sha256 = :token"
            ),
            {"token": token_sha256},
        ).one()
    return _LegacyState(*row)


def _validate(store: RefreshTokenStore, tenant_id: str, plaintext: str):
    with _tenant(tenant_id):
        return store.validate(plaintext, tenant_id=tenant_id)


def _active(store: RefreshTokenStore, tenant_id: str, agent_id: str) -> bool:
    with _tenant(tenant_id):
        return store.has_active_for_agent(agent_id)


def _mark_used(store: RefreshTokenStore, tenant_id: str, plaintext: str) -> None:
    with _tenant(tenant_id):
        store.mark_used(plaintext)


def _revoke(store: RefreshTokenStore, tenant_id: str, plaintext: str) -> None:
    with _tenant(tenant_id):
        store.revoke(plaintext)


def _seed_validation_rows(engine: Engine) -> None:
    _seed(
        engine,
        (
            _SeedRow(_HASH_A, "agent-a", _TENANT_A, "fp:synthetic-a"),
            _SeedRow(_HASH_B, "agent-b", _TENANT_B, "fp:synthetic-b"),
            _SeedRow(
                _HASH_C,
                "agent-revoked",
                _TENANT_A,
                "fp:synthetic-c",
                dt.datetime(2024, 1, 2, tzinfo=dt.UTC),
            ),
        ),
    )


def test_validacao_legada_reusa_linha_antiga_sem_expiracao(stores) -> None:
    store_a, store_b, admin_engine = stores
    _seed_validation_rows(admin_engine)

    valid = _validate(store_a, _TENANT_A, _PLAINTEXT_A)
    assert valid is not None
    assert valid.agent_id == "agent-a"
    assert valid.tenant_id == _TENANT_A
    assert valid.machine_fingerprint == "fp:synthetic-a"
    assert valid.issued_at == _OLD
    assert valid.last_used_at is None
    assert _validate(store_a, _TENANT_A, "synthetic-wrong-token") is None
    assert _validate(store_b, _TENANT_B, _PLAINTEXT_A) is None
    assert _validate(store_a, _TENANT_A, _PLAINTEXT_C) is None


def test_mark_used_preserva_reuso_e_revoke_e_idempotente(stores) -> None:
    store_a, _, admin_engine = stores
    _seed(
        admin_engine,
        (_SeedRow(_HASH_A, "agent-reuse", _TENANT_A, "fp:synthetic-reuse"),),
    )

    before = _readback(admin_engine, _HASH_A)
    _mark_used(store_a, _TENANT_A, _PLAINTEXT_A)
    after_mark = _readback(admin_engine, _HASH_A)
    assert after_mark.token_sha256 == before.token_sha256
    assert after_mark.agent_id == before.agent_id
    assert after_mark.tenant_id == before.tenant_id
    assert after_mark.machine_fingerprint == before.machine_fingerprint
    assert after_mark.issued_at == before.issued_at
    assert after_mark.revoked_at == before.revoked_at
    assert after_mark.last_used_at is not None
    valid = _validate(store_a, _TENANT_A, _PLAINTEXT_A)
    assert valid is not None
    assert valid.last_used_at is not None
    _revoke(store_a, _TENANT_A, _PLAINTEXT_A)
    after_revoke = _readback(admin_engine, _HASH_A)
    assert after_revoke.token_sha256 == before.token_sha256
    assert after_revoke.agent_id == before.agent_id
    assert after_revoke.tenant_id == before.tenant_id
    assert after_revoke.machine_fingerprint == before.machine_fingerprint
    assert after_revoke.issued_at == before.issued_at
    assert after_revoke.last_used_at == after_mark.last_used_at
    assert after_revoke.revoked_at is not None
    first_revocation = after_revoke.revoked_at
    _revoke(store_a, _TENANT_A, _PLAINTEXT_A)
    assert _readback(admin_engine, _HASH_A).revoked_at == first_revocation
    assert _validate(store_a, _TENANT_A, _PLAINTEXT_A) is None


def test_has_active_considera_zero_uma_varias_e_todas_revogadas(stores) -> None:
    store_a, store_b, admin_engine = stores
    _seed(
        admin_engine,
        (
            _SeedRow(_HASH_A, "agent-one", _TENANT_A, "fp:one"),
            _SeedRow(
                _HASH_B,
                "agent-many",
                _TENANT_A,
                "fp:many-a",
                dt.datetime(2024, 1, 2, tzinfo=dt.UTC),
            ),
            _SeedRow(_HASH_C, "agent-many", _TENANT_A, "fp:many-b"),
            _SeedRow(
                _HASH_D,
                "agent-none",
                _TENANT_A,
                "fp:none",
                dt.datetime(2024, 1, 2, tzinfo=dt.UTC),
            ),
        ),
    )

    with admin_engine.connect() as connection:
        count = connection.execute(text("SELECT count(*) FROM auth_refresh_tokens")).scalar_one()
    assert count == 4
    assert _active(store_a, _TENANT_A, "agent-zero") is False
    assert _active(store_a, _TENANT_A, "agent-one") is True
    assert _active(store_a, _TENANT_A, "agent-many") is True
    assert _active(store_a, _TENANT_A, "agent-none") is False
    assert _active(store_a, _TENANT_A, "agent-cross") is False
    assert _active(store_b, _TENANT_B, "agent-one") is False
