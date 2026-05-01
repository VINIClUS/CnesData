"""Migration 014 creates auth_refresh_tokens + auth_provisioned_certs."""
import pytest
from sqlalchemy import inspect


@pytest.mark.postgres
def test_migracao_014_cria_tabela_auth_refresh_tokens(pg_engine):
    insp = inspect(pg_engine)
    assert "auth_refresh_tokens" in insp.get_table_names(schema="public")
    cols = {c["name"] for c in insp.get_columns("auth_refresh_tokens", schema="public")}
    expected = {
        "token_sha256", "agent_id", "tenant_id", "machine_fingerprint",
        "issued_at", "last_used_at", "revoked_at",
    }
    assert expected.issubset(cols)


@pytest.mark.postgres
def test_migracao_014_cria_tabela_auth_provisioned_certs(pg_engine):
    insp = inspect(pg_engine)
    assert "auth_provisioned_certs" in insp.get_table_names(schema="public")
    cols = {c["name"] for c in insp.get_columns("auth_provisioned_certs", schema="public")}
    expected = {
        "agent_id", "tenant_id", "subject_cn", "ca_serial",
        "issued_at", "expires_at", "revoked_at",
    }
    assert expected.issubset(cols)


@pytest.mark.postgres
def test_migracao_014_aplica_rls_em_auth_refresh_tokens(pg_engine):
    """RLS policy must filter by tenant_id."""
    with pg_engine.connect() as conn:
        result = conn.exec_driver_sql(
            "SELECT relrowsecurity FROM pg_class WHERE relname = 'auth_refresh_tokens'"
        ).scalar()
        assert result is True
