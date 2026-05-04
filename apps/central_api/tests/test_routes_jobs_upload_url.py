"""Testes da rota POST /api/v1/jobs/upload-url."""
from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import uuid4

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
def client(monkeypatch):
    app = _make_app()
    fake_storage = MagicMock()
    fake_storage.presigned_put.return_value = "https://minio/presigned?sig=x"
    monkeypatch.setattr(
        "central_api.routes.jobs._object_storage",
        lambda: fake_storage,
    )
    from central_api.deps import get_engine
    app.dependency_overrides[get_engine] = lambda: MagicMock()
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    app.dependency_overrides.clear()


def test_aceita_request_valido(client, monkeypatch):
    job_id = str(uuid4())
    monkeypatch.setattr(
        "central_api.routes.jobs.extractions_repo.mint_upload_url",
        lambda *a, **kw: kw["job_id"],
    )
    resp = client.post(
        "/api/v1/jobs/upload-url",
        headers={"X-Tenant-Id": "354130"},
        json={
            "job_id": job_id,
            "tenant_id": "354130",
            "source_type": "CNES_LOCAL",
            "tipo_extracao": "profissionais",
            "competencia": "2026-01-01",
            "intent": "cnes_profissionais",
        },
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["extraction_id"] == job_id
    assert body["upload_url"].startswith("https://minio/presigned")
    assert body["minio_key"].endswith(".parquet.gz")


def test_rejeita_duplicate_job_id(client, monkeypatch):
    monkeypatch.setattr(
        "central_api.routes.jobs.extractions_repo.mint_upload_url",
        lambda *a, **kw: None,
    )
    resp = client.post(
        "/api/v1/jobs/upload-url",
        headers={"X-Tenant-Id": "354130"},
        json={
            "job_id": str(uuid4()),
            "tenant_id": "354130",
            "source_type": "CNES_LOCAL",
            "tipo_extracao": "profissionais",
            "competencia": "2026-01-01",
            "intent": "cnes_profissionais",
        },
    )
    assert resp.status_code == 409


def test_rejeita_payload_invalido(client):
    resp = client.post(
        "/api/v1/jobs/upload-url",
        headers={"X-Tenant-Id": "354130"},
        json={"job_id": "not-a-uuid"},
    )
    assert resp.status_code == 422


def test_rejeita_source_intent_desconhecido(client, monkeypatch):
    resp = client.post(
        "/api/v1/jobs/upload-url",
        headers={"X-Tenant-Id": "354130"},
        json={
            "job_id": str(uuid4()),
            "tenant_id": "354130",
            "source_type": "CNES_LOCAL",
            "tipo_extracao": "profissionais",
            "competencia": "2026-01-01",
            "intent": "cnes_unknown",
        },
    )
    assert resp.status_code == 422
    assert "unsupported_source_intent" in resp.text
