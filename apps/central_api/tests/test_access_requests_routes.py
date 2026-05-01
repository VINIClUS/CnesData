"""Tests for /api/v1/dashboard/access-requests/* routes."""
from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError

from central_api.middleware import AuthenticatedUser
from central_api.repositories.dashboard_repo import AccessRequestRow, TenantRow
from central_api.routes import access_requests


def _build(user, repo) -> TestClient:
    app = FastAPI()
    app.state.dashboard_repo = repo

    @app.middleware("http")
    async def inject(request: Request, call_next):
        if user is not None:
            request.state.user = user
        return await call_next(request)

    app.include_router(
        access_requests.router,
        prefix="/api/v1/dashboard/access-requests",
    )
    return TestClient(app)


def _user() -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=uuid4(), email="g@m", display_name=None,
        role="gestor", tenant_ids=[],
    )


def test_get_mine_retorna_requests_do_user() -> None:
    user = _user()
    repo = MagicMock()
    repo.list_access_requests.return_value = [
        AccessRequestRow(
            id=uuid4(), tenant_id="354130",
            tenant_nome="Presidente Epitácio",
            motivation="m", status="pending",
            requested_at=datetime.now(UTC),
            reviewed_at=None, review_notes=None,
        ),
    ]
    c = _build(user, repo)
    r = c.get("/api/v1/dashboard/access-requests/mine")
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 1
    assert body[0]["status"] == "pending"


def test_post_cria_request_e_audita() -> None:
    user = _user()
    repo = MagicMock()
    repo.submit_access_request.return_value = uuid4()
    c = _build(user, repo)
    r = c.post("/api/v1/dashboard/access-requests", json={
        "tenant_id": "354130", "motivation": "Sou gestor",
    })
    assert r.status_code == 201
    assert "request_id" in r.json()
    actions = [c.kwargs["action"] for c in repo.log_action.call_args_list]
    assert "request_access" in actions


def test_post_responde_409_se_duplicado() -> None:
    user = _user()
    repo = MagicMock()
    repo.submit_access_request.side_effect = IntegrityError("dup", {}, Exception())
    c = _build(user, repo)
    r = c.post("/api/v1/dashboard/access-requests", json={
        "tenant_id": "354130", "motivation": "x",
    })
    assert r.status_code == 409
    assert r.json()["detail"] == "duplicate_request"


def test_post_responde_422_motivation_vazia() -> None:
    user = _user()
    c = _build(user, MagicMock())
    r = c.post("/api/v1/dashboard/access-requests", json={
        "tenant_id": "354130", "motivation": "",
    })
    assert r.status_code == 422


def test_post_responde_422_motivation_longa() -> None:
    user = _user()
    c = _build(user, MagicMock())
    r = c.post("/api/v1/dashboard/access-requests", json={
        "tenant_id": "354130", "motivation": "x" * 501,
    })
    assert r.status_code == 422


def test_get_available_tenants_lista_municipios_disponiveis() -> None:
    user = _user()
    repo = MagicMock()
    repo.list_available_tenants_for_user.return_value = [
        TenantRow(ibge6="350000", ibge7="3500000", nome="Outro", uf="SP"),
    ]
    c = _build(user, repo)
    r = c.get("/api/v1/dashboard/access-requests/available-tenants")
    assert r.status_code == 200
    assert r.json()[0]["ibge6"] == "350000"


def test_responde_401_sem_user() -> None:
    c = _build(None, MagicMock())
    r = c.get("/api/v1/dashboard/access-requests/mine")
    assert r.status_code == 401
