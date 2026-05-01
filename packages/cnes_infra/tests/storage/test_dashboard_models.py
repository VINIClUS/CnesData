"""Tests for dashboard SQLAlchemy ORM models."""
from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from cnes_infra.storage.dashboard_models import (
    DashboardAuditLog,
    DashboardUser,
    DashboardUserTenant,
)


@pytest.mark.postgres
def test_persiste_user_e_recupera_por_oidc(pg_conn) -> None:
    s = Session(bind=pg_conn)
    u = DashboardUser(
        oidc_subject="sub-x", oidc_issuer="https://kc.local",
        email="x@y", role="gestor",
    )
    s.add(u)
    s.flush()
    loaded = s.query(DashboardUser).filter_by(oidc_subject="sub-x").one()
    assert loaded.email == "x@y"
    assert loaded.role == "gestor"


@pytest.mark.postgres
def test_relacionamento_user_tenants_cascade_delete(pg_conn) -> None:
    s = Session(bind=pg_conn)
    u = DashboardUser(oidc_subject="del-1", oidc_issuer="i", email="d@e")
    s.add(u)
    s.flush()
    s.add(DashboardUserTenant(user_id=u.id, tenant_id="354130"))
    s.flush()
    user_id = u.id
    s.delete(u)
    s.flush()
    rows = s.query(DashboardUserTenant).filter_by(user_id=user_id).count()
    assert rows == 0


@pytest.mark.postgres
def test_audit_log_grava_action_login(pg_conn) -> None:
    s = Session(bind=pg_conn)
    u = DashboardUser(oidc_subject="aud-1", oidc_issuer="i", email="a@u")
    s.add(u)
    s.flush()
    entry = DashboardAuditLog(
        user_id=u.id, tenant_id=None, action="login",
        metadata_={"ip": "127.0.0.1"},
    )
    s.add(entry)
    s.flush()
    rows = s.query(DashboardAuditLog).filter_by(user_id=u.id).all()
    assert len(rows) == 1
    assert rows[0].action == "login"
    assert rows[0].metadata_ == {"ip": "127.0.0.1"}
