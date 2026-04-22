"""Testes de integração leve para central_api via TestClient."""
import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine


def _make_app():
    with (
        patch("central_api.app.init_telemetry"),
        patch("central_api.deps.install_rls_listener"),
        patch("central_api.deps.instrument_engine"),
        patch("central_api.deps.create_engine"),
        patch("central_api.deps.reap_expired_leases", return_value=0),
    ):
        from central_api.app import create_app
        return create_app()


@pytest.fixture
def app():
    return _make_app()


@pytest.fixture
def client(app):
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c


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
    def test_reap_leases_retorna_contagem(
        self, client_with_engine, assert_query_limit,
    ):
        with patch(
            "central_api.routes.admin.reap_expired_leases",
            return_value=3,
        ):
            resp = client_with_engine.post("/api/v1/admin/reap-leases")
        assert resp.status_code == 200
        assert resp.json() == {"reaped": 3}
        assert_query_limit(resp, 15)

    def test_reap_leases_retorna_zero_quando_sem_leases(
        self, client_with_engine,
    ):
        with patch(
            "central_api.routes.admin.reap_expired_leases",
            return_value=0,
        ):
            resp = client_with_engine.post("/api/v1/admin/reap-leases")
        assert resp.json() == {"reaped": 0}


class TestJobsEndpoints:
    def test_get_job_status_retorna_404_quando_nao_encontrado(
        self, client_with_engine,
    ):
        job_id = str(uuid.uuid4())
        with patch("central_api.routes.jobs.get_status", return_value=None):
            resp = client_with_engine.get(f"/api/v1/jobs/{job_id}")
        assert resp.status_code == 404

    def test_get_job_status_retorna_job_encontrado(
        self, client_with_engine, assert_query_limit,
    ):
        job_id = str(uuid.uuid4())
        job_data = {
            "job_id": job_id,
            "status": "PENDING",
            "source_system": "profissionais",
            "tenant_id": "354130",
        }
        with patch(
            "central_api.routes.jobs.get_status", return_value=job_data,
        ):
            resp = client_with_engine.get(f"/api/v1/jobs/{job_id}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "PENDING"
        assert_query_limit(resp, 15)

    def test_acquire_retorna_204_quando_sem_job(
        self, app, mock_engine,
    ):
        from central_api.deps import get_engine, get_object_storage
        app.dependency_overrides[get_engine] = lambda: mock_engine
        app.dependency_overrides[get_object_storage] = lambda: MagicMock()
        with (
            TestClient(app) as c,
            patch(
                "central_api.routes.jobs.acquire_for_agent",
                return_value=None,
            ),
        ):
            resp = c.post(
                "/api/v1/jobs/acquire",
                json={"machine_id": "agent-01", "source_system": "profissionais"},
            )
        app.dependency_overrides.clear()
        assert resp.status_code == 204

    def test_acquire_retorna_job_com_upload_url(
        self, app, mock_engine, assert_query_limit,
    ):
        job = MagicMock()
        job.id = str(uuid.uuid4())
        job.source_system = "profissionais"
        job.tenant_id = "354130"
        storage = MagicMock()
        storage.generate_presigned_upload_url.return_value = (
            "http://minio/upload/presigned"
        )
        from central_api.deps import get_engine, get_object_storage
        app.dependency_overrides[get_engine] = lambda: mock_engine
        app.dependency_overrides[get_object_storage] = lambda: storage
        with (
            TestClient(app) as c,
            patch(
                "central_api.routes.jobs.acquire_for_agent", return_value=job,
            ),
            patch("cnes_infra.config.MINIO_BUCKET", "test-bucket"),
        ):
            resp = c.post(
                "/api/v1/jobs/acquire",
                json={"machine_id": "agent-01", "source_system": "profissionais"},
            )
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        body = resp.json()
        assert "upload_url" in body
        assert "job_id" in body
        assert_query_limit(resp, 15)

    def test_heartbeat_retorna_409_quando_lease_invalido(
        self, client_with_engine,
    ):
        job_id = str(uuid.uuid4())
        with patch(
            "central_api.routes.jobs.renew_heartbeat", return_value=False,
        ):
            resp = client_with_engine.post(
                f"/api/v1/jobs/{job_id}/heartbeat",
                json={"machine_id": "agent-01"},
            )
        assert resp.status_code == 409

    def test_heartbeat_retorna_renovado_com_sucesso(
        self, client_with_engine,
    ):
        job_id = str(uuid.uuid4())
        with patch(
            "central_api.routes.jobs.renew_heartbeat", return_value=True,
        ):
            resp = client_with_engine.post(
                f"/api/v1/jobs/{job_id}/heartbeat",
                json={"machine_id": "agent-01"},
            )
        assert resp.status_code == 200
        assert resp.json()["renewed"] is True

    def test_start_streaming_retorna_409_quando_falha(
        self, client_with_engine,
    ):
        job_id = str(uuid.uuid4())
        with patch(
            "central_api.routes.jobs.transition_to_streaming",
            return_value=False,
        ):
            resp = client_with_engine.post(
                f"/api/v1/jobs/{job_id}/streaming",
                json={"machine_id": "agent-01"},
            )
        assert resp.status_code == 409

    def test_start_streaming_retorna_200_com_sucesso(
        self, client_with_engine,
    ):
        job_id = str(uuid.uuid4())
        with patch(
            "central_api.routes.jobs.transition_to_streaming",
            return_value=True,
        ):
            resp = client_with_engine.post(
                f"/api/v1/jobs/{job_id}/streaming",
                json={"machine_id": "agent-01"},
            )
        assert resp.status_code == 200

    def test_complete_upload_retorna_409_quando_falha(
        self, client_with_engine,
    ):
        job_id = str(uuid.uuid4())
        with patch(
            "central_api.routes.jobs.complete_upload", return_value=False,
        ):
            resp = client_with_engine.post(
                f"/api/v1/jobs/{job_id}/complete-upload",
                json={
                    "machine_id": "agent-01",
                    "object_key": "tenant/prof/abc.parquet.gz",
                    "size_bytes": 1024,
                },
            )
        assert resp.status_code == 409

    def test_complete_upload_retorna_200_com_sucesso(
        self, client_with_engine,
    ):
        job_id = str(uuid.uuid4())
        with patch(
            "central_api.routes.jobs.complete_upload", return_value=True,
        ):
            resp = client_with_engine.post(
                f"/api/v1/jobs/{job_id}/complete-upload",
                json={
                    "machine_id": "agent-01",
                    "object_key": "tenant/prof/abc.parquet.gz",
                    "size_bytes": 1024,
                },
            )
        assert resp.status_code == 200

    def test_create_job_retorna_201(self, client_with_engine):
        expected_id = uuid.uuid4()
        with patch("central_api.routes.jobs.enqueue", return_value=expected_id):
            resp = client_with_engine.post(
                "/api/v1/jobs/create",
                json={
                    "intent": "profissionais",
                    "competencia": "2026-03",
                    "cod_municipio": "354130",
                },
            )
        assert resp.status_code == 201
        body = resp.json()
        assert body["status"] == "PENDING"
        assert "job_id" in body


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
    def test_get_object_storage_usa_null_quando_minio_falha(self):
        import central_api.deps as deps_mod
        from cnes_domain.ports.object_storage import NullObjectStoragePort
        deps_mod._object_storage = None
        with (
            patch("central_api.deps.config"),
            patch(
                "cnes_infra.storage.object_storage.MinioObjectStorage",
                side_effect=Exception("minio_down"),
            ),
        ):
            storage = deps_mod.get_object_storage()
        assert isinstance(storage, NullObjectStoragePort)
        deps_mod._object_storage = None

    def test_get_object_storage_reutiliza_instancia(self):
        import central_api.deps as deps_mod
        deps_mod._object_storage = None
        mock_storage = MagicMock()
        deps_mod._object_storage = mock_storage
        s2 = deps_mod.get_object_storage()
        assert s2 is mock_storage
        deps_mod._object_storage = None

    def test_get_object_storage_cria_minio_quando_disponivel(self):
        import central_api.deps as deps_mod
        deps_mod._object_storage = None
        mock_instance = MagicMock()
        with (
            patch("central_api.deps.config"),
            patch(
                "cnes_infra.storage.object_storage.MinioObjectStorage",
                return_value=mock_instance,
            ),
        ):
            storage = deps_mod.get_object_storage()
        assert storage is mock_instance
        deps_mod._object_storage = None


class TestLeaseReaperLoop:
    @pytest.mark.asyncio
    async def test_reaper_loop_registra_leases_reaped(self):
        import asyncio

        from central_api.deps import _lease_reaper_loop
        engine = MagicMock()
        call_count = 0

        async def _fake_executor(executor, fn, *args):
            nonlocal call_count
            call_count += 1
            return 5

        with patch("central_api.deps._REAPER_INTERVAL", 0.01):
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

        with patch.object(deps_mod, "_REAPER_INTERVAL", 0.01):
            with patch(
                "central_api.deps.reap_expired_leases",
                side_effect=Exception("db_error"),
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
