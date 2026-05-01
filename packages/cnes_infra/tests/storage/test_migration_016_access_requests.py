"""Tests for migration 016_access_requests — schema + chk_action extend."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


@pytest.mark.postgres
def test_cria_tabela_access_requests(pg_engine: Engine) -> None:
    with pg_engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'dashboard' AND table_name = 'access_requests' "
            "ORDER BY column_name"
        )).scalars().all()
    assert set(rows) == {
        "id", "user_id", "tenant_id", "motivation", "status",
        "requested_at", "reviewed_at", "reviewed_by", "review_notes",
    }


@pytest.mark.postgres
def test_unique_constraint_user_tenant(pg_conn) -> None:
    pg_conn.execute(text(
        "INSERT INTO dashboard.users (oidc_subject, oidc_issuer, email) "
        "VALUES ('u-016-1', 'i', 'e@m')"
    ))
    uid = pg_conn.execute(text(
        "SELECT id FROM dashboard.users WHERE oidc_subject='u-016-1'"
    )).scalar_one()
    pg_conn.execute(text(
        "INSERT INTO dashboard.access_requests (user_id, tenant_id, motivation) "
        "VALUES (:u, '354130', 'first')"
    ), {"u": uid})
    with pytest.raises(IntegrityError):
        pg_conn.execute(text(
            "INSERT INTO dashboard.access_requests (user_id, tenant_id, motivation) "
            "VALUES (:u, '354130', 'second')"
        ), {"u": uid})


@pytest.mark.postgres
def test_chk_motivation_length(pg_conn) -> None:
    pg_conn.execute(text(
        "INSERT INTO dashboard.users (oidc_subject, oidc_issuer, email) "
        "VALUES ('u-016-2', 'i', 'e@m')"
    ))
    uid = pg_conn.execute(text(
        "SELECT id FROM dashboard.users WHERE oidc_subject='u-016-2'"
    )).scalar_one()
    long_text = "x" * 501
    with pytest.raises(IntegrityError):
        pg_conn.execute(text(
            "INSERT INTO dashboard.access_requests (user_id, tenant_id, motivation) "
            "VALUES (:u, '354130', :m)"
        ), {"u": uid, "m": long_text})


@pytest.mark.postgres
def test_audit_log_aceita_novas_acoes(pg_conn) -> None:
    pg_conn.execute(text(
        "INSERT INTO dashboard.users (oidc_subject, oidc_issuer, email) "
        "VALUES ('u-016-3', 'i', 'e@m')"
    ))
    uid = pg_conn.execute(text(
        "SELECT id FROM dashboard.users WHERE oidc_subject='u-016-3'"
    )).scalar_one()
    for action in [
        "request_access", "approve_access", "reject_access",
        "view_overview", "view_faturamento",
    ]:
        pg_conn.execute(text(
            "INSERT INTO dashboard.audit_log (user_id, action) VALUES (:u, :a)"
        ), {"u": uid, "a": action})
