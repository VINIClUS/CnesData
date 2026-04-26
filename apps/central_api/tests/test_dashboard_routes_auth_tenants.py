"""Tests for dashboard /auth/me + /tenants routes."""
from unittest.mock import MagicMock
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from central_api.middleware import AuthenticatedUser
from central_api.routes import dashboard


def _build(user, repo) -> TestClient:
    app = FastAPI()
    app.state.dashboard_repo = repo

    @app.middleware("http")
    async def inject(request: Request, call_next):
        if user is not None:
            request.state.user = user
        return await call_next(request)

    app.include_router(dashboard.router, prefix="/api/v1/dashboard")
    return TestClient(app)


def _user(tenants: list[str]) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=uuid4(), email="g@m", display_name="Gestor",
        role="gestor", tenant_ids=tenants,
    )


def test_auth_me_retorna_perfil_e_tenants() -> None:
    user = _user(["354130"])
    repo = MagicMock()
    repo.has_pending_request.return_value = False
    c = _build(user, repo)
    r = c.get("/api/v1/dashboard/auth/me")
    assert r.status_code == 200
    body = r.json()
    assert body["email"] == "g@m"
    assert body["display_name"] == "Gestor"
    assert body["role"] == "gestor"
    assert body["tenant_ids"] == ["354130"]
    assert body["user_id"] == str(user.user_id)


def test_auth_me_grava_audit_login() -> None:
    user = _user(["354130"])
    repo = MagicMock()
    repo.has_pending_request.return_value = False
    c = _build(user, repo)
    c.get("/api/v1/dashboard/auth/me")
    repo.log_action.assert_called_once()
    kwargs = repo.log_action.call_args.kwargs
    assert kwargs["action"] == "login"
    assert kwargs["user_id"] == user.user_id
    assert kwargs["tenant_id"] is None


def test_auth_me_responde_401_sem_user() -> None:
    c = _build(None, MagicMock())
    r = c.get("/api/v1/dashboard/auth/me")
    assert r.status_code == 401


def test_auth_me_retorna_has_pending_request_true_quando_pendente() -> None:
    user = _user(["354130"])
    repo = MagicMock()
    repo.has_pending_request.return_value = True
    c = _build(user, repo)
    r = c.get("/api/v1/dashboard/auth/me")
    assert r.status_code == 200
    assert r.json()["has_pending_request"] is True


def test_auth_me_retorna_has_pending_request_false_normalmente() -> None:
    user = _user(["354130"])
    repo = MagicMock()
    repo.has_pending_request.return_value = False
    c = _build(user, repo)
    r = c.get("/api/v1/dashboard/auth/me")
    assert r.status_code == 200
    assert r.json()["has_pending_request"] is False


def test_tenants_lista_municipios_alocados() -> None:
    from central_api.repositories.dashboard_repo import TenantRow
    user = _user(["354130"])
    repo = MagicMock()
    repo.list_tenants.return_value = [
        TenantRow(
            ibge6="354130", ibge7="3541308",
            nome="Presidente Epitácio", uf="SP",
        ),
    ]
    c = _build(user, repo)
    r = c.get("/api/v1/dashboard/tenants")
    assert r.status_code == 200
    assert r.json() == [{
        "ibge6": "354130", "ibge7": "3541308",
        "nome": "Presidente Epitácio", "uf": "SP",
    }]
    repo.list_tenants.assert_called_once_with(user_id=user.user_id)


def test_tenants_grava_audit_view_tenants() -> None:
    user = _user(["354130"])
    repo = MagicMock()
    repo.list_tenants.return_value = []
    c = _build(user, repo)
    c.get("/api/v1/dashboard/tenants")
    actions = [call.kwargs["action"] for call in repo.log_action.call_args_list]
    assert "view_tenants" in actions


def test_tenants_responde_401_sem_user() -> None:
    c = _build(None, MagicMock())
    r = c.get("/api/v1/dashboard/tenants")
    assert r.status_code == 401
