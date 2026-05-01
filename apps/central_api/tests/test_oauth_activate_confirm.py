"""Tests for /activate/confirm — Bearer JWT + tenant gate + audit + rate limit."""
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from central_api.middleware import AuthenticatedUser
from central_api.routes import oauth


def _build(user, store, repo) -> TestClient:
    app = FastAPI()
    app.state.device_code_store = store
    app.state.dashboard_repo = repo
    app.state.limiter = oauth.limiter

    @app.middleware("http")
    async def inject(request: Request, call_next):
        if user is not None:
            request.state.user = user
        return await call_next(request)

    app.include_router(oauth.router)
    return TestClient(app)


def _user(tenants: list[str]) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=uuid4(), email="g@m", display_name=None,
        role="gestor", tenant_ids=tenants,
    )


def test_aprova_user_code_valido_e_grava_audit() -> None:
    store = MagicMock()
    store.redeem_user_code = AsyncMock(return_value=True)
    repo = MagicMock()
    user = _user(["354130"])
    c = _build(user, store, repo)
    r = c.post("/activate/confirm", json={
        "user_code": "WDJB-MJHT", "tenant_id": "354130",
    })
    assert r.status_code == 200
    assert r.json()["status"] == "approved"
    store.redeem_user_code.assert_awaited_once_with("WDJB-MJHT", tenant_id="354130")
    repo.log_action.assert_called_once()
    assert repo.log_action.call_args.kwargs["action"] == "activate_agent"


def test_responde_403_se_tenant_nao_pertence_ao_user() -> None:
    user = _user(["354130"])
    store = MagicMock()
    store.redeem_user_code = AsyncMock(return_value=True)
    c = _build(user, store, MagicMock())
    r = c.post("/activate/confirm", json={
        "user_code": "WDJB-MJHT", "tenant_id": "999999",
    })
    assert r.status_code == 403


def test_responde_400_se_user_code_invalido_ou_expirado() -> None:
    store = MagicMock()
    store.redeem_user_code = AsyncMock(return_value=False)
    user = _user(["354130"])
    c = _build(user, store, MagicMock())
    r = c.post("/activate/confirm", json={
        "user_code": "EXPIRED1", "tenant_id": "354130",
    })
    assert r.status_code == 400
    assert r.json()["detail"] == "invalid_or_expired_user_code"


def test_responde_401_sem_user() -> None:
    store = MagicMock()
    store.redeem_user_code = AsyncMock(return_value=True)
    c = _build(None, store, MagicMock())
    r = c.post("/activate/confirm", json={
        "user_code": "WDJB-MJHT", "tenant_id": "354130",
    })
    assert r.status_code == 401


def test_valida_formato_user_code() -> None:
    user = _user(["354130"])
    store = MagicMock()
    store.redeem_user_code = AsyncMock(return_value=True)
    c = _build(user, store, MagicMock())
    r = c.post("/activate/confirm", json={
        "user_code": "TOO-SHORT-12345", "tenant_id": "354130",
    })
    assert r.status_code == 422
