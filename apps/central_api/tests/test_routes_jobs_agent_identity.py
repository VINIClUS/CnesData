"""Rotas /api/v1/jobs/* vinculadas à identidade mTLS do agente (#253)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from central_api.agent_auth import AgentCertIdentity, agent_identity_if_required

_IDENTITY = AgentCertIdentity(tenant_id="354130", agent_id="agent-1", machine_id="a1b2c3d4")


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


def _upload_body(**overrides) -> dict:
    body = {
        "job_id": str(uuid4()),
        "tenant_id": "354130",
        "source_type": "CNES_LOCAL",
        "tipo_extracao": "profissionais",
        "competencia": "2026-01-01",
        "intent": "cnes_profissionais",
        "machine_id": "a1b2c3d4",
    }
    return body | overrides


def _register_body(**overrides) -> dict:
    files = [{
        "minio_key": "354130/CNES_VINCULO/2026-01-01/job.parquet.gz",
        "fato_subtype": "CNES_VINCULO",
        "size_bytes": 1,
        "sha256": "a" * 64,
    }]
    return {"job_id": str(uuid4()), "files": files, "machine_id": "a1b2c3d4"} | overrides


@pytest.fixture
def repo(monkeypatch):
    fake = MagicMock()
    fake.mint_upload_url.side_effect = lambda *a, **kw: kw["job_id"]
    fake.register.side_effect = lambda *a, **kw: kw["job_id"]
    monkeypatch.setattr("central_api.routes.jobs.extractions_repo", fake)
    storage = MagicMock()
    storage.generate_presigned_upload_url.return_value = "https://s3/presigned"
    monkeypatch.setattr("central_api.routes.jobs._object_storage", lambda: storage)
    return fake


def _client(identity: AgentCertIdentity | None = _IDENTITY, *, real_auth: bool = False):
    app = _make_app()
    from central_api.deps import get_engine
    app.dependency_overrides[get_engine] = lambda: MagicMock()
    if not real_auth:
        app.dependency_overrides[agent_identity_if_required] = lambda: identity
    return TestClient(app, raise_server_exceptions=True)


@pytest.mark.parametrize(
    ("path", "body"),
    [("/api/v1/jobs/upload-url", _upload_body()), ("/api/v1/jobs/register", _register_body())],
)
def test_rejeita_jobs_sem_certificado_quando_mtls_obrigatorio(
    monkeypatch, repo, path, body,
):
    monkeypatch.setattr("cnes_infra.config.AGENT_MTLS_REQUIRED", True)
    with _client(real_auth=True) as client:
        resp = client.post(path, json=body, headers={"X-Tenant-Id": "354130"})
    assert resp.status_code == 401
    repo.mint_upload_url.assert_not_called()
    repo.register.assert_not_called()


def test_upload_url_aceita_corpo_igual_ao_certificado(repo):
    with _client() as client:
        resp = client.post("/api/v1/jobs/upload-url", json=_upload_body())
    assert resp.status_code == 201, resp.text


def test_upload_url_rejeita_tenant_diferente_do_certificado(repo):
    with _client() as client:
        resp = client.post("/api/v1/jobs/upload-url", json=_upload_body(tenant_id="999999"))
    assert resp.status_code == 403
    assert resp.json()["detail"] == "agent_identity_mismatch"
    repo.mint_upload_url.assert_not_called()


def test_upload_url_grava_machine_id_do_certificado_quando_corpo_diverge(repo, caplog):
    with _client() as client, caplog.at_level("WARNING", logger="central_api.routes.jobs"):
        resp = client.post("/api/v1/jobs/upload-url", json=_upload_body(machine_id="ffffffff"))
    assert resp.status_code == 201
    assert repo.mint_upload_url.call_args.kwargs["machine_id"] == "a1b2c3d4"
    assert "agent_machine_id_mismatch" in caplog.text


def test_upload_url_sem_machine_id_usa_o_do_certificado(repo):
    body = _upload_body()
    del body["machine_id"]
    with _client() as client:
        resp = client.post("/api/v1/jobs/upload-url", json=body)
    assert resp.status_code == 201
    assert repo.mint_upload_url.call_args.kwargs["machine_id"] == "a1b2c3d4"


def test_register_restringe_ao_tenant_do_certificado(repo):
    with _client() as client:
        resp = client.post("/api/v1/jobs/register", json=_register_body())
    assert resp.status_code == 200
    assert repo.register.call_args.kwargs["tenant_id"] == "354130"


def test_register_grava_machine_id_do_certificado_quando_corpo_diverge(repo):
    with _client() as client:
        resp = client.post("/api/v1/jobs/register", json=_register_body(machine_id="ffffffff"))
    assert resp.status_code == 200
    assert repo.register.call_args.kwargs["machine_id"] == "a1b2c3d4"


def test_register_sem_identidade_mantem_comportamento_legado(repo):
    with _client(identity=None) as client:
        resp = client.post("/api/v1/jobs/register", json=_register_body(machine_id="x"))
    assert resp.status_code == 200
    assert repo.register.call_args.kwargs["tenant_id"] is None
