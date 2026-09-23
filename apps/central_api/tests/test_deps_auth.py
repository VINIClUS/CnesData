"""Tests for require_auth + require_tenant_header dependencies."""
from unittest.mock import MagicMock
from uuid import uuid4

from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from central_api.deps import require_auth, require_tenant_header
from central_api.middleware import AuthenticatedUser
from cnes_domain.tenant import get_tenant_id


def _user(tenants: list[str]) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=uuid4(), email="g@m", display_name=None,
        role="gestor", tenant_ids=tenants,
    )


def _build_app_protected() -> FastAPI:
    app = FastAPI()

    @app.get("/protected")
    def protected(user: AuthenticatedUser = Depends(require_auth)) -> dict:
        return {"email": user.email}

    return app


def _build_app_scoped() -> FastAPI:
    app = FastAPI()

    @app.get("/scoped")
    def scoped(
        user: AuthenticatedUser = Depends(require_auth),
        tenant: str = Depends(require_tenant_header),
    ) -> dict:
        return {"tenant": tenant}

    return app


def _inject_user(app: FastAPI, user: AuthenticatedUser | None) -> None:
    @app.middleware("http")
    async def inject(request: Request, call_next):
        if user is not None:
            request.state.user = user
        return await call_next(request)


def test_require_auth_responde_401_sem_user() -> None:
    app = _build_app_protected()
    r = TestClient(app).get("/protected")
    assert r.status_code == 401
    assert r.json()["detail"] == "auth_required"


def test_require_auth_passa_quando_state_user_presente() -> None:
    app = _build_app_protected()
    _inject_user(app, _user(["354130"]))
    r = TestClient(app).get("/protected")
    assert r.status_code == 200
    assert r.json() == {"email": "g@m"}


def test_require_tenant_header_responde_400_sem_header() -> None:
    app = _build_app_scoped()
    _inject_user(app, _user(["354130"]))
    r = TestClient(app).get("/scoped")
    assert r.status_code == 400
    assert r.json()["detail"] == "tenant_header_required"


def test_require_tenant_header_responde_403_se_tenant_nao_pertence() -> None:
    app = _build_app_scoped()
    _inject_user(app, _user(["354130"]))
    r = TestClient(app).get("/scoped", headers={"X-Tenant-Id": "999999"})
    assert r.status_code == 403
    assert r.json()["detail"] == "tenant_not_allowed"


def test_require_tenant_header_passa_quando_pertence() -> None:
    app = _build_app_scoped()
    _inject_user(app, _user(["354130"]))
    r = TestClient(app).get("/scoped", headers={"X-Tenant-Id": "354130"})
    assert r.status_code == 200
    assert r.json() == {"tenant": "354130"}


def test_require_tenant_header_define_tenant_visivel_no_endpoint_sync() -> None:
    app = FastAPI()

    @app.get("/ctx")
    def ctx(tenant: str = Depends(require_tenant_header)) -> dict:
        return {"ctx": get_tenant_id()}

    _inject_user(app, _user(["354130"]))
    r = TestClient(app).get("/ctx", headers={"X-Tenant-Id": "354130"})
    assert r.status_code == 200
    assert r.json() == {"ctx": "354130"}


def test_install_cert_authority_avisa_quando_mtls_desligado(monkeypatch, caplog):
    from central_api.deps import _install_cert_authority

    monkeypatch.delenv("AUTH_CA_CERT_PATH", raising=False)
    monkeypatch.setattr("cnes_infra.config.AGENT_MTLS_REQUIRED", False)
    app = MagicMock()
    with caplog.at_level("WARNING", logger="central_api.deps"):
        _install_cert_authority(app)
    assert app.state.cert_authority is None
    assert "agent_mtls required=false" in caplog.text


def test_install_cert_authority_nao_avisa_quando_mtls_obrigatorio(monkeypatch, caplog):
    from central_api.deps import _install_cert_authority

    monkeypatch.delenv("AUTH_CA_CERT_PATH", raising=False)
    monkeypatch.setattr("cnes_infra.config.AGENT_MTLS_REQUIRED", True)
    with caplog.at_level("WARNING", logger="central_api.deps"):
        _install_cert_authority(MagicMock())
    assert "agent_mtls" not in caplog.text
