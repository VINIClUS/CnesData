"""Tests for DashboardRepo."""

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from central_api.repositories.dashboard_repo import DashboardRepo
from cnes_infra.storage.dim_lookup import upsert_dim_municipio


@pytest.fixture
def repo(pg_engine: Engine) -> DashboardRepo:
    return DashboardRepo(pg_engine)


@pytest.fixture
def cleanup_dashboard(pg_engine: Engine):
    yield
    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM dashboard.audit_log"))
        conn.execute(text("DELETE FROM dashboard.user_tenants"))
        conn.execute(text("DELETE FROM dashboard.users"))


@pytest.mark.postgres
def test_upsert_user_cria_quando_nao_existe(
    repo: DashboardRepo, cleanup_dashboard: None,
) -> None:
    u = repo.upsert_user(
        oidc_subject="sub-a", oidc_issuer="https://kc",
        email="a@m", display_name="A",
    )
    assert u.email == "a@m"
    assert u.role == "gestor"
    assert u.tenant_ids == []
    assert u.last_login_at is not None


@pytest.mark.postgres
def test_upsert_user_atualiza_email_e_last_login(
    repo: DashboardRepo, cleanup_dashboard: None,
) -> None:
    u1 = repo.upsert_user(
        oidc_subject="sub-b", oidc_issuer="https://kc",
        email="old@m", display_name=None,
    )
    u2 = repo.upsert_user(
        oidc_subject="sub-b", oidc_issuer="https://kc",
        email="new@m", display_name="B",
    )
    assert u1.user_id == u2.user_id
    assert u2.email == "new@m"
    assert u2.display_name == "B"
    assert u2.last_login_at is not None
    assert u2.last_login_at >= u1.last_login_at


@pytest.mark.postgres
def test_upsert_user_carrega_tenant_ids_existentes(
    repo: DashboardRepo, pg_engine: Engine, cleanup_dashboard: None,
) -> None:
    u = repo.upsert_user(
        oidc_subject="sub-c", oidc_issuer="https://kc",
        email="c@m", display_name=None,
    )
    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO dashboard.user_tenants VALUES (:u, '354130'), (:u, '350000')"
        ), {"u": u.user_id})
    u2 = repo.upsert_user(
        oidc_subject="sub-c", oidc_issuer="https://kc",
        email="c@m", display_name=None,
    )
    assert sorted(u2.tenant_ids) == ["350000", "354130"]


@pytest.mark.postgres
def test_list_tenants_retorna_apenas_alocados(
    repo: DashboardRepo, pg_engine: Engine, cleanup_dashboard: None,
) -> None:
    with pg_engine.begin() as conn:
        upsert_dim_municipio(conn, {
            "ibge6": "354130", "ibge7": "3541308",
            "nome": "Presidente Epitácio", "uf": "SP",
        })
    u = repo.upsert_user(
        oidc_subject="sub-d", oidc_issuer="https://kc",
        email="d@m", display_name=None,
    )
    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO dashboard.user_tenants VALUES (:u, '354130')"
        ), {"u": u.user_id})
    tenants = repo.list_tenants(user_id=u.user_id)
    assert len(tenants) == 1
    assert tenants[0].ibge6 == "354130"
    assert tenants[0].nome == "Presidente Epitácio"
    assert tenants[0].uf == "SP"


@pytest.mark.postgres
def test_log_action_grava_audit_log(
    repo: DashboardRepo, pg_engine: Engine, cleanup_dashboard: None,
) -> None:
    u = repo.upsert_user(
        oidc_subject="sub-e", oidc_issuer="https://kc",
        email="e@m", display_name=None,
    )
    repo.log_action(
        user_id=u.user_id, tenant_id="354130", action="login",
        metadata={"ip": "10.0.0.1"},
    )
    with pg_engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT action, tenant_id, metadata FROM dashboard.audit_log "
            "WHERE user_id = :u"
        ), {"u": u.user_id}).all()
    assert len(rows) == 1
    assert rows[0].action == "login"
    assert rows[0].tenant_id == "354130"
    assert rows[0].metadata == {"ip": "10.0.0.1"}


@pytest.mark.postgres
def test_log_action_aceita_metadata_none(
    repo: DashboardRepo, pg_engine: Engine, cleanup_dashboard: None,
) -> None:
    u = repo.upsert_user(
        oidc_subject="sub-f", oidc_issuer="https://kc",
        email="f@m", display_name=None,
    )
    repo.log_action(
        user_id=u.user_id, tenant_id=None, action="logout", metadata=None,
    )
    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT metadata, tenant_id FROM dashboard.audit_log WHERE user_id = :u"
        ), {"u": u.user_id}).one()
    assert row.metadata is None
    assert row.tenant_id is None


@pytest.mark.postgres
def test_log_action_rejeita_acao_invalida(
    repo: DashboardRepo, cleanup_dashboard: None,
) -> None:
    from sqlalchemy.exc import IntegrityError
    u = repo.upsert_user(
        oidc_subject="sub-g", oidc_issuer="https://kc",
        email="g@m", display_name=None,
    )
    with pytest.raises(IntegrityError):
        repo.log_action(
            user_id=u.user_id, tenant_id=None, action="hack", metadata=None,
        )
