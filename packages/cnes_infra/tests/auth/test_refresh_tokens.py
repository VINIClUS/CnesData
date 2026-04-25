"""RefreshTokenStore: Postgres CRUD + RLS by tenant."""
import datetime as dt

import pytest
from sqlalchemy import text

from cnes_infra.auth.refresh_tokens import RefreshTokenStore


def _set_tenant(engine, tenant_id: str):
    """Helper: set app.tenant_id session-scoped on a connection."""
    return text(f"SET app.tenant_id = '{tenant_id}'")


@pytest.mark.postgres
def test_cria_refresh_token_e_persiste(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(_set_tenant(pg_engine, "presidente-epitacio"))
        store = RefreshTokenStore(pg_engine)
        token = store.create(
            agent_id="agent-001",
            tenant_id="presidente-epitacio",
            machine_fingerprint="fp:aabbcc",
        )
        assert len(token) > 0

    valid = store.validate(token, tenant_id="presidente-epitacio")
    assert valid is not None
    assert valid.agent_id == "agent-001"
    assert valid.machine_fingerprint == "fp:aabbcc"
    assert valid.revoked_at is None


@pytest.mark.postgres
def test_validate_token_revogado_retorna_none(pg_engine):
    store = RefreshTokenStore(pg_engine)
    token = store.create(
        agent_id="agent-002",
        tenant_id="presidente-epitacio",
        machine_fingerprint="fp:bbccdd",
    )
    store.revoke(token)
    assert store.validate(token, tenant_id="presidente-epitacio") is None


@pytest.mark.postgres
def test_mark_used_atualiza_last_used_at(pg_engine):
    store = RefreshTokenStore(pg_engine)
    token = store.create(
        agent_id="agent-003",
        tenant_id="presidente-epitacio",
        machine_fingerprint="fp:ccddee",
    )
    before = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1)
    store.mark_used(token)
    after = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=1)

    valid = store.validate(token, tenant_id="presidente-epitacio")
    assert valid is not None
    assert valid.last_used_at is not None
    assert before <= valid.last_used_at <= after


@pytest.mark.postgres
def test_validate_token_inexistente_retorna_none(pg_engine):
    store = RefreshTokenStore(pg_engine)
    assert store.validate("a" * 64, tenant_id="presidente-epitacio") is None


@pytest.mark.postgres
def test_validate_com_tenant_id_diferente_retorna_none(pg_engine):
    """Cross-tenant validate returns None (defense in depth even without
    RLS active in test connection: query has explicit tenant_id filter)."""
    store = RefreshTokenStore(pg_engine)
    token = store.create(
        agent_id="agent-004",
        tenant_id="tenant-a",
        machine_fingerprint="fp:eeff00",
    )
    assert store.validate(token, tenant_id="tenant-b") is None
