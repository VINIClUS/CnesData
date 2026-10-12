"""Tests for /api/v1/activate/confirm — Bearer JWT + tenant gate + audit + rate limit."""
from collections.abc import Iterator
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from slowapi.errors import RateLimitExceeded

from central_api.middleware import AuthenticatedUser
from central_api.ratelimit import rate_limit_handler
from central_api.routes import oauth

if TYPE_CHECKING:
    from starlette.types import ExceptionHandler

_ACTIVATE = "/api/v1/activate/confirm"


def _build(user, store, repo) -> TestClient:
    app = FastAPI()
    app.state.device_code_store = store
    app.state.dashboard_repo = repo
    app.state.limiter = oauth.limiter
    app.add_exception_handler(RateLimitExceeded, cast("ExceptionHandler", rate_limit_handler))

    @app.middleware("http")
    async def inject(request: Request, call_next):
        if user is not None:
            request.state.user = user
        return await call_next(request)

    app.include_router(oauth.activation_router, prefix="/api/v1")
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
    r = c.post(_ACTIVATE, json={
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
    r = c.post(_ACTIVATE, json={
        "user_code": "WDJB-MJHT", "tenant_id": "999999",
    })
    assert r.status_code == 403


def test_responde_400_se_user_code_invalido_ou_expirado() -> None:
    store = MagicMock()
    store.redeem_user_code = AsyncMock(return_value=False)
    user = _user(["354130"])
    c = _build(user, store, MagicMock())
    r = c.post(_ACTIVATE, json={
        "user_code": "EXPIRED1", "tenant_id": "354130",
    })
    assert r.status_code == 400
    assert r.json()["detail"] == "invalid_or_expired_user_code"


def test_responde_401_sem_user() -> None:
    store = MagicMock()
    store.redeem_user_code = AsyncMock(return_value=True)
    c = _build(None, store, MagicMock())
    r = c.post(_ACTIVATE, json={
        "user_code": "WDJB-MJHT", "tenant_id": "354130",
    })
    assert r.status_code == 401


def test_valida_formato_user_code() -> None:
    user = _user(["354130"])
    store = MagicMock()
    store.redeem_user_code = AsyncMock(return_value=True)
    c = _build(user, store, MagicMock())
    r = c.post(_ACTIVATE, json={
        "user_code": "TOO-SHORT-12345", "tenant_id": "354130",
    })
    assert r.status_code == 422


def test_vincula_tenant_do_corpo_antes_de_auditar() -> None:
    from cnes_domain.tenant import tenant_id_ctx

    store = MagicMock()
    store.redeem_user_code = AsyncMock(return_value=True)
    repo = MagicMock()
    seen: list[str | None] = []
    repo.log_action.side_effect = lambda **_kw: seen.append(tenant_id_ctx.get(None))
    token = tenant_id_ctx.set("000000")
    try:
        r = _build(_user(["354130"]), store, repo).post(_ACTIVATE, json={
            "user_code": "WDJB-MJHT", "tenant_id": "354130",
        })
    finally:
        tenant_id_ctx.reset(token)
    assert r.status_code == 200
    assert seen == ["354130"]


@pytest.fixture
def _limiter_zerado() -> Iterator[None]:
    oauth.limiter.reset()
    yield
    oauth.limiter.reset()


@pytest.mark.usefixtures("_limiter_zerado")
def test_rota_versionada_aplica_rate_limit_de_10_por_minuto() -> None:
    store = MagicMock()
    store.redeem_user_code = AsyncMock(return_value=False)
    c = _build(_user(["354130"]), store, MagicMock())
    payload = {"user_code": "WDJB-MJHT", "tenant_id": "354130"}

    statuses = [c.post(_ACTIVATE, json=payload).status_code for _ in range(10)]
    blocked = c.post(_ACTIVATE, json=payload)

    assert statuses == [400] * 10
    assert blocked.status_code == 429
    assert blocked.json() == {"detail": "rate_limited", "retry_after": 60}
    assert blocked.headers["retry-after"] == "60"
