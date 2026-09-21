"""Testes de integração leve para central_api via TestClient (Gold v2)."""
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine


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
            app = client.app

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


class TestAdminEndpoint:
    def test_reap_leases_retorna_contagem(self, app, assert_query_limit):
        from central_api.deps import get_conn
        fake_conn = MagicMock()
        app.dependency_overrides[get_conn] = lambda: fake_conn
        with (
            TestClient(app) as c,
            patch(
                "central_api.routes.admin.extractions_repo.reap_expired",
                return_value=3,
            ),
        ):
            resp = c.post("/api/v1/admin/reap-leases")
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        assert resp.json() == {"reaped": 3}
        assert_query_limit(resp, 15)

    def test_reap_leases_retorna_zero_quando_sem_leases(self, app):
        from central_api.deps import get_conn
        fake_conn = MagicMock()
        app.dependency_overrides[get_conn] = lambda: fake_conn
        with (
            TestClient(app) as c,
            patch(
                "central_api.routes.admin.extractions_repo.reap_expired",
                return_value=0,
            ),
        ):
            resp = c.post("/api/v1/admin/reap-leases")
        app.dependency_overrides.clear()
        assert resp.json() == {"reaped": 0}


class TestTenantMiddleware:
    def test_middleware_define_tenant_id_do_header(
        self, client_with_engine,
    ):
        with patch("central_api.middleware.set_tenant_id") as mock_set:
            client_with_engine.get(
                "/api/v1/system/health",
                headers={"X-Tenant-Id": "354130"},
            )
        mock_set.assert_called_with("354130")

    def test_middleware_ignora_requisicao_sem_tenant(
        self, client_with_engine,
    ):
        with patch("central_api.middleware.set_tenant_id") as mock_set:
            client_with_engine.get("/api/v1/system/health")
        mock_set.assert_not_called()


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


class TestGetObjectStorage:
    def test_get_object_storage_expoe_object_storage_port(self):
        from central_api import deps as deps_mod
        deps_mod._object_storage_instance = None
        storage = deps_mod.get_object_storage()
        assert hasattr(storage, "generate_presigned_upload_url")
        assert hasattr(storage, "object_exists")
        assert hasattr(storage, "get_presigned_download_url")
        deps_mod._object_storage_instance = None

    def test_get_object_storage_e_singleton(self):
        """Regressão: o antigo MinioWrapper construía um client novo a cada
        chamada de presigned_put. O factory tem que reusar o mesmo client."""
        from central_api import deps as deps_mod
        deps_mod._object_storage_instance = None
        first = deps_mod.get_object_storage()
        second = deps_mod.get_object_storage()
        assert first is second
        deps_mod._object_storage_instance = None


class TestLeaseReaperLoop:
    @pytest.mark.asyncio
    async def test_reaper_loop_registra_leases_reaped(self):
        import asyncio

        from central_api.deps import _lease_reaper_loop
        engine = MagicMock()

        with (
            patch("central_api.deps._REAPER_INTERVAL", 0.01),
            patch(
                "central_api.deps._reap_expired_sync", return_value=5,
            ),
        ):
            task = asyncio.create_task(_lease_reaper_loop(engine))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_reaper_loop_captura_excecao(self):
        import asyncio

        import central_api.deps as deps_mod

        engine = MagicMock()

        with (
            patch.object(deps_mod, "_REAPER_INTERVAL", 0.01),
            patch(
                "central_api.deps._reap_expired_sync",
                side_effect=Exception("db_error"),
            ),
        ):
            task = asyncio.create_task(
                deps_mod._lease_reaper_loop(engine),
            )
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
