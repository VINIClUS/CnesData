"""End-to-end Phase 2b: full Phase 2 flow then rotate via mTLS."""
import urllib.parse

import pytest
from cryptography import x509
from fastapi.testclient import TestClient


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_e2e_provision_seguido_de_rotate(
    session_root_ca, monkeypatch, make_csr_pem, pg_engine,
):
    cert_path, key_path = session_root_ca
    monkeypatch.setenv("AUTH_CA_CERT_PATH", str(cert_path))
    monkeypatch.setenv("AUTH_CA_KEY_PATH", str(key_path))
    monkeypatch.setenv("AUTH_DEVICE_VERIFICATION_URI", "https://example/activate")
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    monkeypatch.setenv("MINIO_ENDPOINT", "x:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "x")
    monkeypatch.setenv("MINIO_BUCKET", "x")
    from central_api.app import create_app
    app = create_app()
    with TestClient(app) as client:
        auth = client.post(
            "/oauth/device_authorization",
            json={"client_id": "agent", "scope": "agent.provision"},
        ).json()
        await app.state.device_code_store.redeem_user_code(
            auth["user_code"], tenant_id="354130",
        )
        tok = client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": auth["device_code"],
                "client_id": "agent",
            },
        ).json()
        first_cert = client.post(
            "/provision/cert",
            headers={"Authorization": f"Bearer {tok['access_token']}"},
            json={
                "csr_pem": make_csr_pem("agent-host-e2e").decode(),
                "machine_fingerprint": "fp:e2e-rot-test",
            },
        ).json()

        first_cert_pem = first_cert["cert_pem"]
        rotate_resp = client.post(
            "/provision/cert/rotate",
            headers={
                "X-SSL-Client-Verify": "SUCCESS",
                "X-SSL-Client-Cert": urllib.parse.quote(first_cert_pem),
            },
            json={"csr_pem": make_csr_pem("agent-host-e2e-rot2").decode()},
        )
    assert rotate_resp.status_code == 200
    body = rotate_resp.json()
    new_cert = x509.load_pem_x509_certificate(body["cert_pem"].encode())
    old_cert = x509.load_pem_x509_certificate(first_cert_pem.encode())
    assert new_cert.serial_number != old_cert.serial_number
    chain = x509.load_pem_x509_certificate(body["ca_chain_pem"].encode())
    assert new_cert.issuer == chain.subject
    delta = new_cert.not_valid_after_utc - new_cert.not_valid_before_utc
    assert 89 <= delta.days <= 91
