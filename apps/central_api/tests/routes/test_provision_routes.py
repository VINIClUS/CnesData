"""Tests for /provision/cert."""
import datetime as dt

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text


def _make_app(session_root_ca, monkeypatch):
    cert_path, key_path = session_root_ca
    monkeypatch.setenv("AUTH_CA_CERT_PATH", str(cert_path))
    monkeypatch.setenv("AUTH_CA_KEY_PATH", str(key_path))
    monkeypatch.setenv("AUTH_DEVICE_VERIFICATION_URI", "https://example/activate")
    monkeypatch.setenv("DB_URL", "postgresql+psycopg://u:p@localhost/x")
    monkeypatch.setenv("MINIO_ENDPOINT", "x:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "x")
    monkeypatch.setenv("MINIO_BUCKET", "x")
    from central_api.app import create_app
    return create_app()


def test_provision_cert_sem_bearer_retorna_401(session_root_ca, monkeypatch, make_csr_pem):
    app = _make_app(session_root_ca, monkeypatch)
    with TestClient(app) as client:
        r = client.post(
            "/provision/cert",
            json={"csr_pem": make_csr_pem().decode(),
                  "machine_fingerprint": "fp:abcdef12"},
        )
    assert r.status_code == 401
    assert r.json() == {"error": "invalid_token"}


def test_provision_cert_com_bearer_invalido_retorna_401(
    session_root_ca, monkeypatch, make_csr_pem,
):
    app = _make_app(session_root_ca, monkeypatch)
    with TestClient(app) as client:
        r = client.post(
            "/provision/cert",
            headers={"Authorization": "Bearer fantasma"},
            json={"csr_pem": make_csr_pem().decode(),
                  "machine_fingerprint": "fp:abcdef12"},
        )
    assert r.status_code == 401
    assert r.json() == {"error": "invalid_token"}


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_provision_cert_com_token_valido_retorna_200(
    session_root_ca, monkeypatch, make_csr_pem, pg_engine,
):
    app = _make_app(session_root_ca, monkeypatch)
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    with TestClient(app) as client:
        token = await app.state.access_token_store.issue(
            tenant_id="presidente-epitacio", ttl_seconds=300,
        )
        r = client.post(
            "/provision/cert",
            headers={"Authorization": f"Bearer {token}"},
            json={"csr_pem": make_csr_pem().decode(),
                  "machine_fingerprint": "fp:abcdef12"},
        )
    assert r.status_code == 200
    body = r.json()
    assert "BEGIN CERTIFICATE" in body["cert_pem"]
    assert "BEGIN CERTIFICATE" in body["ca_chain_pem"]
    assert len(body["refresh_token"]) > 0
    parsed = dt.datetime.fromisoformat(body["expires_at"])
    delta = parsed - dt.datetime.now(dt.UTC)
    assert 89 <= delta.days <= 91


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_provision_cert_consome_token_unica_vez(
    session_root_ca, monkeypatch, make_csr_pem, pg_engine,
):
    app = _make_app(session_root_ca, monkeypatch)
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    with TestClient(app) as client:
        token = await app.state.access_token_store.issue(
            tenant_id="presidente-epitacio", ttl_seconds=300,
        )
        first = client.post(
            "/provision/cert",
            headers={"Authorization": f"Bearer {token}"},
            json={"csr_pem": make_csr_pem().decode(),
                  "machine_fingerprint": "fp:abcdef12"},
        )
        assert first.status_code == 200

        second = client.post(
            "/provision/cert",
            headers={"Authorization": f"Bearer {token}"},
            json={"csr_pem": make_csr_pem().decode(),
                  "machine_fingerprint": "fp:abcdef12"},
        )
    assert second.status_code == 401
    assert second.json() == {"error": "invalid_token"}


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_provision_cert_grava_audit_e_refresh_token(
    session_root_ca, monkeypatch, make_csr_pem, pg_engine,
):
    app = _make_app(session_root_ca, monkeypatch)
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    with TestClient(app) as client:
        token = await app.state.access_token_store.issue(
            tenant_id="presidente-epitacio", ttl_seconds=300,
        )
        r = client.post(
            "/provision/cert",
            headers={"Authorization": f"Bearer {token}"},
            json={"csr_pem": make_csr_pem().decode(),
                  "machine_fingerprint": "fp:fingerprintunique"},
        )
    assert r.status_code == 200
    with pg_engine.connect() as conn:
        prov_count = conn.execute(
            text(
                "SELECT COUNT(*) FROM auth_provisioned_certs "
                "WHERE tenant_id = 'presidente-epitacio'"
            ),
        ).scalar()
        rt_row = conn.execute(
            text(
                "SELECT machine_fingerprint FROM auth_refresh_tokens "
                "WHERE machine_fingerprint = 'fp:fingerprintunique'"
            ),
        ).fetchone()
    assert prov_count >= 1
    assert rt_row is not None


def test_provision_cert_csr_invalido_retorna_400(session_root_ca, monkeypatch):
    """Invalid CSR bubbles through CertAuthority -> OAuthError invalid_request."""
    import asyncio
    app = _make_app(session_root_ca, monkeypatch)
    with TestClient(app) as client:
        async def _issue():
            return await app.state.access_token_store.issue(
                tenant_id="t1", ttl_seconds=300,
            )
        token = asyncio.run(_issue())
        r = client.post(
            "/provision/cert",
            headers={"Authorization": f"Bearer {token}"},
            json={"csr_pem": "not a real CSR",
                  "machine_fingerprint": "fp:abcdef12"},
        )
    assert r.status_code == 400
    assert r.json()["error"] == "invalid_request"
