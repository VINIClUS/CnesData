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
    from central_api.deps import get_engine
    app.dependency_overrides[get_engine] = lambda: mock_engine
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
        from central_api.deps import get_engine
        app.dependency_overrides[get_engine] = lambda: failing_engine
        with TestClient(app, raise_server_exceptions=True) as c:
            resp = c.get("/api/v1/system/health")
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "degraded"
        assert body["db_connected"] is False

    def test_health_contem_timestamp(self, client_with_engine):
        resp = client_with_engine.get("/api/v1/system/health")
        assert "timestamp" in resp.json()


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


class TestGetMinio:
    def test_get_minio_retorna_wrapper_com_bucket(self):
        from central_api.deps import get_minio
        wrapper = get_minio()
        assert wrapper.bucket
        assert hasattr(wrapper, "presigned_put")


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
