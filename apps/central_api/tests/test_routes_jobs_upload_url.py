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


def test_erro_422_de_intent_desconhecido_segue_schema_httpvalidationerror(client):
    """Regression for H11 (docs/edge-agent-audit-2026-09-20.md).

    Every route's OpenAPI schema documents 422 as HTTPValidationError
    (detail: list[ValidationError]) because FastAPI auto-adds that response
    for any route. Before the fix, `_resolve_fato_subtype` raised a bare
    string `detail`, which the generated Go client crashed trying to
    unmarshal as that list — discarding the real error message under an
    opaque secondary parse-failure log line. `detail` must always be a list
    of {loc, msg, type} objects, never a string, for any 422 this route can
    emit.
    """
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
    detail = resp.json()["detail"]
    assert isinstance(detail, list), f"detail must be a list, got {type(detail)}: {detail!r}"
    assert detail, "detail list must not be empty"
    for item in detail:
        assert set(item) >= {"loc", "msg", "type"}
        assert isinstance(item["loc"], list)
        assert isinstance(item["msg"], str)
        assert isinstance(item["type"], str)
    assert "unsupported_source_intent" in detail[0]["msg"]
