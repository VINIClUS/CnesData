"""Tests for the retired /api/v1/dashboard/overview + /faturamento/by-establishment routes."""
from unittest.mock import MagicMock
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from central_api.middleware import AuthenticatedUser
from central_api.routes import overview


def _build(user, repo) -> TestClient:
    app = FastAPI()
    app.state.dashboard_repo = repo

    @app.middleware("http")
    async def inject(request: Request, call_next):
        if user is not None:
            request.state.user = user
        return await call_next(request)

    app.include_router(overview.router, prefix="/api/v1/dashboard")
    return TestClient(app)


def _user(tenants: list[str]) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=uuid4(), email="g@m", display_name=None,
        role="gestor", tenant_ids=tenants,
    )


def test_overview_retorna_410_sem_tocar_o_repositorio() -> None:
    repo = MagicMock()
    c = _build(_user(["354130"]), repo)
    r = c.get("/api/v1/dashboard/overview", headers={"X-Tenant-Id": "354130"})
    assert r.status_code == 410
    assert r.json() == {"detail": "legacy_route_retired"}
    assert repo.mock_calls == []


def test_faturamento_chart_retorna_410_sem_tocar_o_repositorio() -> None:
    repo = MagicMock()
    c = _build(_user(["354130"]), repo)
    r = c.get(
        "/api/v1/dashboard/faturamento/by-establishment?months=12",
        headers={"X-Tenant-Id": "354130"},
    )
    assert r.status_code == 410
    assert r.json() == {"detail": "legacy_route_retired"}
    assert repo.mock_calls == []


def test_overview_responde_403_tenant_nao_pertence() -> None:
    user = _user(["354130"])
    c = _build(user, MagicMock())
    r = c.get("/api/v1/dashboard/overview", headers={"X-Tenant-Id": "999999"})
    assert r.status_code == 403


def test_overview_responde_401_sem_user() -> None:
    c = _build(None, MagicMock())
    r = c.get("/api/v1/dashboard/overview", headers={"X-Tenant-Id": "354130"})
    assert r.status_code == 401


def test_faturamento_chart_responde_400_se_months_invalido() -> None:
    user = _user(["354130"])
    repo = MagicMock()
    c = _build(user, repo)
    r = c.get(
        "/api/v1/dashboard/faturamento/by-establishment?months=0",
        headers={"X-Tenant-Id": "354130"},
    )
    assert r.status_code == 422
