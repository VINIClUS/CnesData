"""Testes de integração leve para central_api via TestClient (Gold v2)."""
from typing import TYPE_CHECKING, cast
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine

if TYPE_CHECKING:
    from fastapi import FastAPI


def _make_app():
    with (
        patch("central_api.app.init_telemetry"),
        patch("central_api.deps.install_rls_listener"),
        patch("central_api.deps.instrument_engine"),
        patch("central_api.deps.install_query_counter"),
        patch("central_api.deps.create_engine"),
    ):
        from central_api.app import create_app
        return create_app()


@pytest.fixture
def app():
    return _make_app()


@pytest.fixture
def mock_engine():
    engine = MagicMock(spec=Engine)
    con = MagicMock()
    con.__enter__ = MagicMock(return_value=con)
    con.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = con
    return engine


@pytest.fixture
def client_with_engine(app, mock_engine):
    from central_api.deps import get_health_engine
    app.dependency_overrides[get_health_engine] = lambda: mock_engine
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def failing_engine():
    engine = MagicMock(spec=Engine)
    engine.connect.side_effect = Exception("db_down")
    return engine


class TestHealthEndpoint:
    def test_health_retorna_ok_quando_db_conecta(
        self, client_with_engine, assert_query_limit,
    ):
        resp = client_with_engine.get("/api/v1/system/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["db_connected"] is True
        assert_query_limit(resp, 15)

    def test_health_retorna_degraded_quando_db_falha(
        self, app, failing_engine,
    ):
        from central_api.deps import get_health_engine
        app.dependency_overrides[get_health_engine] = lambda: failing_engine
        with TestClient(app, raise_server_exceptions=True) as c:
            resp = c.get("/api/v1/system/health")
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "degraded"
        assert body["db_connected"] is False

    def test_health_local_nao_constroi_engine_sql(self, monkeypatch):
        from central_api.deps import get_health_engine

        monkeypatch.setenv("PROFILE", "local")
        with patch("central_api.deps.create_engine") as create_engine:
            assert get_health_engine() is None
        create_engine.assert_not_called()

    def test_health_fora_do_profile_local_retorna_engine(self, monkeypatch):
        from central_api.deps import get_health_engine

        monkeypatch.delenv("PROFILE", raising=False)
        engine = MagicMock(spec=Engine)
        with patch("central_api.deps.get_engine", return_value=engine):
            assert get_health_engine() is engine

    def test_health_contem_timestamp(self, client_with_engine):
        resp = client_with_engine.get("/api/v1/system/health")
        assert "timestamp" in resp.json()


class TestLocalCompositionDependencies:
    def test_lifespan_local_instala_auth_e_serving(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PROFILE", "local")
        monkeypatch.setenv("TENANT_ID", "354130")
        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        from central_api.routes.local_auth import get_local_auth_service
        from central_api.routes.serving import (
            get_serving_access,
            get_serving_object_store,
            get_serving_principal,
        )

        with TestClient(_make_app()) as client:
            app = cast("FastAPI", client.app)

            assert app.state.local_auth_service is not None
            assert app.state.serving_access is not None
            assert get_local_auth_service in app.dependency_overrides
            assert get_serving_access in app.dependency_overrides
            assert get_serving_object_store in app.dependency_overrides
            assert get_serving_principal in app.dependency_overrides

    def test_resolver_de_principal_rejeita_cookie_ausente(self):
        from fastapi import HTTPException
        from starlette.requests import Request

        from central_api.deps import _serving_principal_resolver

        request = Request({"type": "http", "headers": []})
        resolver = _serving_principal_resolver(MagicMock())

        with pytest.raises(HTTPException, match="session_required"):
            resolver(request)

    def test_resolver_de_principal_retorna_sessao_valida(self):
        from starlette.requests import Request

        from central_api.deps import _serving_principal_resolver
        from central_api.routes.serving import ServingPrincipal

        auth_service = MagicMock()
        auth_service.resolve_session.return_value = MagicMock(
            tenant_id="354130", user_id="user-1"
        )
        request = Request({
            "type": "http",
            "headers": [(b"cookie", b"cnesdata_session=session-token")],
        })

        principal = _serving_principal_resolver(auth_service)(request)

        assert principal == ServingPrincipal(tenant_id="354130", user_id="user-1")

    def test_resolver_de_principal_rejeita_sessao_invalida(self):
        from fastapi import HTTPException
        from starlette.requests import Request

        from central_api.deps import _serving_principal_resolver
        from cnes_infra.auth.local_auth import AuthenticationRejected, AuthRejectionCode

        auth_service = MagicMock()
        auth_service.resolve_session.side_effect = AuthenticationRejected(
            AuthRejectionCode.SESSION_INVALID
        )
        request = Request({
            "type": "http",
            "headers": [(b"cookie", b"cnesdata_session=session-token")],
        })

        with pytest.raises(HTTPException, match="session_invalid"):
            _serving_principal_resolver(auth_service)(request)


_ADMIN_KEY = "token-configurado"
_ENQUEUE_BODY = {
    "source_type": "BPA_MAG", "tenant_id": "354130", "competencia": "2026-02-01",
}


@pytest.fixture
def admin_token(monkeypatch):
    from cnes_infra import config
    monkeypatch.setattr(config, "ADMIN_TOKEN", _ADMIN_KEY)
    return _ADMIN_KEY


class TestAdminTokenGuard:
    @pytest.fixture
    def reap_expired(self):
        with patch(
            "cnes_infra.storage.extractions_repo.reap_expired", return_value=0,
        ) as m:
            yield m

    @pytest.fixture
    def enqueue(self):
        with patch("cnes_infra.storage.extractions_repo.enqueue") as m:
            yield m

    @pytest.mark.parametrize("headers", [
        {},
        {"X-Admin-Token": "test-admin"},
        {"X-Admin-Token": ""},
    ])
    def test_reap_leases_rejeita_token_ausente_ou_invalido(
        self, app, admin_token, reap_expired, headers,
    ):
        with TestClient(app) as c:
            resp = c.post("/api/v1/admin/reap-leases", headers=headers)
        assert resp.status_code == 401
        assert resp.json()["detail"] == "admin_token_required"
        reap_expired.assert_not_called()

    @pytest.mark.parametrize("headers", [
        {},
        {"X-Admin-Token": "test-admin"},
        {"X-Admin-Token": ""},
    ])
    def test_enqueue_rejeita_token_ausente_ou_invalido(
        self, app, admin_token, enqueue, headers,
    ):
        with TestClient(app) as c:
            resp = c.post(
                "/api/v1/extractions/enqueue", json=_ENQUEUE_BODY, headers=headers,
            )
        assert resp.status_code == 401
        assert resp.json()["detail"] == "admin_token_required"
        enqueue.assert_not_called()

    @pytest.mark.parametrize("path", [
        "/api/v1/admin/reap-leases",
        "/api/v1/extractions/enqueue",
    ])
    def test_rotas_admin_desativadas_sem_token_configurado(
        self, app, monkeypatch, reap_expired, enqueue, path,
    ):
        from cnes_infra import config
        monkeypatch.setattr(config, "ADMIN_TOKEN", "")
        with TestClient(app) as c:
            resp = c.post(path, json=_ENQUEUE_BODY, headers={"X-Admin-Token": ""})
        assert resp.status_code == 503
        assert resp.json()["detail"] == "admin_disabled"
        reap_expired.assert_not_called()
        enqueue.assert_not_called()


class TestTenantHeader:
    def test_header_x_tenant_id_sozinho_nao_define_tenant(self, app):
        from cnes_domain.tenant import tenant_id_ctx

        @app.get("/__tenant_probe")
        def probe() -> dict:
            return {"tenant": tenant_id_ctx.get(None)}

        token = tenant_id_ctx.set("000000")
        try:
            resp = TestClient(app).get(
                "/__tenant_probe", headers={"X-Tenant-Id": "354130"},
            )
        finally:
            tenant_id_ctx.reset(token)
        assert resp.status_code == 200
        assert resp.json() == {"tenant": "000000"}


class TestGetEngine:
    def test_get_engine_reutiliza_instancia_existente(self):
        import central_api.deps as deps_mod
        deps_mod._engine = None
        with patch("central_api.deps.create_engine") as mock_create:
            mock_create.return_value = MagicMock(spec=Engine)
            e1 = deps_mod.get_engine()
            e2 = deps_mod.get_engine()
        assert e1 is e2
        mock_create.assert_called_once()
        deps_mod._engine = None
