"""Tests for DashboardRepo signup methods."""

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from central_api.repositories.dashboard_repo import DashboardRepo
from cnes_infra.storage.dim_lookup import upsert_dim_municipio


@pytest.fixture
def repo(pg_engine: Engine) -> DashboardRepo:
    return DashboardRepo(pg_engine)


@pytest.fixture
def cleanup_signup(pg_engine: Engine):
    yield
    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM dashboard.access_requests"))
        conn.execute(text("DELETE FROM dashboard.audit_log"))
        conn.execute(text("DELETE FROM dashboard.user_tenants"))
        conn.execute(text("DELETE FROM dashboard.users"))


@pytest.mark.postgres
def test_submit_access_request_cria_pendente(
    repo: DashboardRepo, cleanup_signup: None,
) -> None:
    user = repo.upsert_user(
        oidc_subject="s1", oidc_issuer="i", email="a@m", display_name=None,
    )
    req_id = repo.submit_access_request(
        user_id=user.user_id, tenant_id="354130", motivation="Sou gestor da SMS",
    )
    assert req_id is not None
    requests = repo.list_access_requests(user_id=user.user_id)
    assert len(requests) == 1
    assert requests[0].tenant_id == "354130"
    assert requests[0].status == "pending"


@pytest.mark.postgres
def test_submit_access_request_duplicado_levanta(
    repo: DashboardRepo, cleanup_signup: None,
) -> None:
    user = repo.upsert_user(
        oidc_subject="s2", oidc_issuer="i", email="a@m", display_name=None,
    )
    repo.submit_access_request(
        user_id=user.user_id, tenant_id="354130", motivation="primeira",
    )
    with pytest.raises(IntegrityError):
        repo.submit_access_request(
            user_id=user.user_id, tenant_id="354130", motivation="segunda",
        )


@pytest.mark.postgres
def test_has_pending_request_retorna_true_quando_pendente(
    repo: DashboardRepo, cleanup_signup: None,
) -> None:
    user = repo.upsert_user(
        oidc_subject="s3", oidc_issuer="i", email="a@m", display_name=None,
    )
    assert repo.has_pending_request(user_id=user.user_id) is False
    repo.submit_access_request(
        user_id=user.user_id, tenant_id="354130", motivation="x",
    )
    assert repo.has_pending_request(user_id=user.user_id) is True


@pytest.mark.postgres
def test_has_pending_request_false_se_aprovado(
    repo: DashboardRepo, pg_engine: Engine, cleanup_signup: None,
) -> None:
    user = repo.upsert_user(
        oidc_subject="s4", oidc_issuer="i", email="a@m", display_name=None,
    )
    repo.submit_access_request(
        user_id=user.user_id, tenant_id="354130", motivation="x",
    )
    with pg_engine.begin() as conn:
        conn.execute(text(
            "UPDATE dashboard.access_requests SET status='approved' WHERE user_id=:u"
        ), {"u": user.user_id})
    assert repo.has_pending_request(user_id=user.user_id) is False


@pytest.mark.postgres
def test_list_available_tenants_exclui_alocados_e_pendentes(
    repo: DashboardRepo, pg_engine: Engine, cleanup_signup: None,
) -> None:
    with pg_engine.begin() as conn:
        upsert_dim_municipio(conn, {
            "ibge6": "354130", "ibge7": "3541308",
            "nome": "Presidente Epitácio", "uf": "SP",
        })
        upsert_dim_municipio(conn, {
            "ibge6": "350000", "ibge7": "3500000",
            "nome": "Outro Município", "uf": "SP",
        })
        upsert_dim_municipio(conn, {
            "ibge6": "351000", "ibge7": "3510000",
            "nome": "Terceiro", "uf": "SP",
        })
    user = repo.upsert_user(
        oidc_subject="s5", oidc_issuer="i", email="a@m", display_name=None,
    )
    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO dashboard.user_tenants VALUES (:u, '354130')"
        ), {"u": user.user_id})
    repo.submit_access_request(
        user_id=user.user_id, tenant_id="350000", motivation="x",
    )
    available = repo.list_available_tenants_for_user(user_id=user.user_id)
    ibges = {t.ibge6 for t in available}
    assert "354130" not in ibges
    assert "350000" not in ibges
    assert "351000" in ibges
