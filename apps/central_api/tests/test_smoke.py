"""Smoke tests — valida endpoints contra stack Docker."""

import json
import urllib.request

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.postgres]


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
