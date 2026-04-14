"""Smoke tests — valida endpoints contra stack Docker."""

import json
import urllib.request

import pytest

pytestmark = pytest.mark.postgres


def test_health_retorna_ok(api_url):
    resp = urllib.request.urlopen(f"{api_url}/api/v1/system/health")
    body = json.loads(resp.read())
    assert body["status"] == "ok"
    assert body["db_connected"] is True


def test_openapi_schema_disponivel(api_url):
    resp = urllib.request.urlopen(f"{api_url}/openapi.json")
    schema = json.loads(resp.read())
    assert "paths" in schema
    assert "/api/v1/system/health" in schema["paths"]


def test_ingest_estabelecimento_aceita_payload(api_url):
    payload = json.dumps({
        "tenant_id": "123456",
        "competencia": "2026-01",
        "registros": [{"cod_cnes": "0000001", "nome": "UBS Teste"}],
    }).encode()
    req = urllib.request.Request(
        f"{api_url}/api/v1/ingest/cnes/estabelecimento",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    resp = urllib.request.urlopen(req)
    assert resp.status == 202
    body = json.loads(resp.read())
    assert "job_id" in body


def test_job_status_apos_ingest(api_url):
    payload = json.dumps({
        "tenant_id": "654321",
        "competencia": "2026-02",
        "registros": [{"cod_cnes": "0000002"}],
    }).encode()
    req = urllib.request.Request(
        f"{api_url}/api/v1/ingest/cnes/estabelecimento",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    resp = urllib.request.urlopen(req)
    body = json.loads(resp.read())
    job_id = body["job_id"]

    status_resp = urllib.request.urlopen(
        f"{api_url}/api/v1/jobs/{job_id}",
    )
    status_body = json.loads(status_resp.read())
    assert status_body["job_id"] == job_id
    assert status_body["status"] == "PENDING"
    assert status_body["source_system"] == "cnes_estabelecimento"


def test_reap_leases_retorna_contagem(api_url):
    req = urllib.request.Request(
        f"{api_url}/api/v1/admin/reap-leases",
        method="POST",
        data=b"",
    )
    resp = urllib.request.urlopen(req)
    body = json.loads(resp.read())
    assert "reaped" in body
    assert isinstance(body["reaped"], int)
