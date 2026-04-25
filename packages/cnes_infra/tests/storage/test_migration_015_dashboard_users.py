"""Tests for migration 015_dashboard_users — schema + RLS + round-trip."""
from __future__ import annotations

import secrets

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError


@pytest.mark.postgres
def test_cria_schema_dashboard_e_tabelas(pg_conn) -> None:
    rows = pg_conn.execute(text(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'dashboard' ORDER BY table_name"
    )).scalars().all()
    assert rows == ["audit_log", "user_tenants", "users"]


@pytest.mark.postgres
def test_users_tem_constraint_unique_oidc(pg_conn) -> None:
    pg_conn.execute(text(
        "INSERT INTO dashboard.users (oidc_subject, oidc_issuer, email) "
        "VALUES ('sub-1', 'https://kc.local', 'a@b')"
    ))
    with pytest.raises(IntegrityError):
        pg_conn.execute(text(
            "INSERT INTO dashboard.users (oidc_subject, oidc_issuer, email) "
            "VALUES ('sub-1', 'https://kc.local', 'c@d')"
        ))


@pytest.mark.postgres
def test_user_tenants_rls_filtra_por_app_tenant_id(pg_conn) -> None:
    role = f"rls_test_user_{secrets.token_hex(6)}"
    uid = pg_conn.execute(text(
        "INSERT INTO dashboard.users (oidc_subject, oidc_issuer, email) "
        "VALUES ('sub-2', 'https://kc.local', 'g@m') RETURNING id"
    )).scalar_one()
    pg_conn.execute(text(
        "INSERT INTO dashboard.user_tenants VALUES (:u, '354130'), (:u, '999999')"
    ), {"u": uid})
    pg_conn.execute(text(f"CREATE ROLE {role} NOLOGIN NOSUPERUSER NOBYPASSRLS"))
    pg_conn.execute(text(f"GRANT USAGE ON SCHEMA dashboard TO {role}"))
    pg_conn.execute(text(f"GRANT SELECT ON dashboard.user_tenants TO {role}"))
    pg_conn.execute(text("SET LOCAL app.tenant_id = '354130'"))
    pg_conn.execute(text(f"SET LOCAL ROLE {role}"))
    rows = pg_conn.execute(text(
        "SELECT tenant_id FROM dashboard.user_tenants WHERE user_id = :u"
    ), {"u": uid}).scalars().all()
    assert rows == ["354130"]


@pytest.mark.postgres
def test_audit_log_aceita_apenas_acoes_validas(pg_conn) -> None:
    with pytest.raises(IntegrityError):
        pg_conn.execute(text(
            "INSERT INTO dashboard.audit_log (action) VALUES ('hack')"
        ))
