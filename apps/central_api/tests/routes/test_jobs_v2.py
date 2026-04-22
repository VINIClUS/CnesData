"""Tests for /api/v1/jobs/* v2 routes (landing.extractions)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient


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
def fake_conn():
    return MagicMock()


@pytest.fixture
def fake_minio():
    minio = MagicMock()
    minio.bucket = "cnesdata-landing"
    minio.presigned_put.return_value = "http://fake/upload"
    return minio


@pytest.fixture
def client(app, fake_conn, fake_minio):
    from central_api import deps
    app.dependency_overrides[deps.get_conn] = lambda: fake_conn
    app.dependency_overrides[deps.get_minio] = lambda: fake_minio
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def _register_payload() -> dict:
    return {
        "tenant_id": "354130",
        "fonte_sistema": "CNES_LOCAL",
        "tipo_extracao": "cnes_profissionais",
        "competencia": 202601,
        "job_id": "00000000-0000-0000-0000-000000000001",
        "agent_version": "v0.2.0",
        "machine_id": "m-1",
    }


class TestRegisterEndpoint:
    def test_register_cria_extraction_e_retorna_upload_url(self, client):
        fake_id = uuid4()
        object_key = f"354130/CNES_LOCAL/202601/{fake_id}.parquet.gz"
        with patch(
            "central_api.routes.jobs.extractions_repo.register",
            return_value=(fake_id, object_key),
        ):
            r = client.post(
                "/api/v1/jobs/register",
                json=_register_payload(),
                headers={"X-Tenant-Id": "354130"},
            )
        assert r.status_code == 201, r.text
        body = r.json()
        assert UUID(body["extraction_id"]) == fake_id
        assert body["upload_url"].startswith("http://")

    def test_register_retorna_503_quando_minio_falha(
        self, app, fake_conn,
    ):
        from central_api import deps
        broken_minio = MagicMock()
        broken_minio.bucket = "cnesdata-landing"
        broken_minio.presigned_put.side_effect = RuntimeError("minio_down")
        app.dependency_overrides[deps.get_conn] = lambda: fake_conn
        app.dependency_overrides[deps.get_minio] = lambda: broken_minio
        with (
            TestClient(app) as c,
            patch(
                "central_api.routes.jobs.extractions_repo.register",
                return_value=(uuid4(), "k"),
            ),
        ):
            r = c.post(
                "/api/v1/jobs/register",
                json=_register_payload(),
                headers={"X-Tenant-Id": "354130"},
            )
        app.dependency_overrides.clear()
        assert r.status_code == 503
        assert r.json()["detail"] == "minio_presign_failed"

    def test_register_rejeita_payload_invalido(self, client):
        r = client.post(
            "/api/v1/jobs/register",
            json={"tenant_id": "abc"},
            headers={"X-Tenant-Id": "354130"},
        )
        assert r.status_code == 422


class TestNextEndpoint:
    def test_next_sem_trabalho_retorna_null(self, client):
        with patch(
            "central_api.routes.jobs.extractions_repo.claim_next",
            return_value=None,
        ):
            r = client.get(
                "/api/v1/jobs/next?processor_id=p1",
                headers={"X-Tenant-Id": "354130"},
            )
        assert r.status_code == 200
        assert r.json() == {"extraction": None}

    def test_next_retorna_extraction_quando_claim(self, client):
        from datetime import UTC, datetime

        from cnes_contracts.jobs import JobStatus
        from cnes_contracts.landing import Extraction

        ext = Extraction(
            id=uuid4(),
            job_id=uuid4(),
            tenant_id="354130",
            fonte_sistema="CNES_LOCAL",
            tipo_extracao="cnes_profissionais",
            competencia=202601,
            status=JobStatus.PROCESSING,
            created_at=datetime(2026, 1, 15, tzinfo=UTC),
        )
        with patch(
            "central_api.routes.jobs.extractions_repo.claim_next",
            return_value=ext,
        ):
            r = client.get(
                "/api/v1/jobs/next?processor_id=p1&lease_secs=600",
                headers={"X-Tenant-Id": "354130"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["extraction"]["tenant_id"] == "354130"


class TestCompleteEndpoint:
    def test_complete_marca_uploaded(self, client):
        ext_id = uuid4()
        with patch(
            "central_api.routes.jobs.extractions_repo.mark_uploaded",
        ) as mock_mark:
            r = client.post(
                f"/api/v1/jobs/{ext_id}/complete",
                json={"sha256": "a" * 64, "row_count": 100},
                headers={"X-Tenant-Id": "354130"},
            )
        assert r.status_code == 200
        assert r.json() == {"status": "UPLOADED"}
        assert mock_mark.call_count == 1


class TestFailEndpoint:
    def test_fail_marca_erro(self, client):
        ext_id = uuid4()
        with patch(
            "central_api.routes.jobs.extractions_repo.fail",
        ) as mock_fail:
            r = client.post(
                f"/api/v1/jobs/{ext_id}/fail",
                json={"error": "parquet_corrupt"},
                headers={"X-Tenant-Id": "354130"},
            )
        assert r.status_code == 200
        assert r.json() == {"status": "FAILED"}
        assert mock_fail.call_args.args[2] == "parquet_corrupt"


class TestHeartbeatEndpoint:
    def test_heartbeat_ok(self, client):
        ext_id = uuid4()
        with patch(
            "central_api.routes.jobs.extractions_repo.heartbeat",
        ) as mock_hb:
            r = client.post(
                f"/api/v1/jobs/{ext_id}/heartbeat?processor_id=p1",
                headers={"X-Tenant-Id": "354130"},
            )
        assert r.status_code == 200
        assert r.json() == {"status": "heartbeat_ok"}
        assert mock_hb.call_count == 1
