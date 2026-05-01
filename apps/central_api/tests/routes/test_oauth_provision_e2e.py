"""End-to-end Phase 2 flow: device_authorization -> token -> provision/cert."""
import pytest
from cryptography import x509
from fastapi.testclient import TestClient


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_fluxo_completo_oauth_para_cert_assinado(
    session_root_ca, monkeypatch, make_csr_pem, pg_engine,
):
    cert_path, key_path = session_root_ca
    monkeypatch.setenv("AUTH_CA_CERT_PATH", str(cert_path))
    monkeypatch.setenv("AUTH_CA_KEY_PATH", str(key_path))
    monkeypatch.setenv("AUTH_DEVICE_VERIFICATION_URI", "https://example/activate")
    monkeypatch.setenv("MINIO_ENDPOINT", "x:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("MINIO_SECRET_KEY", "x")
    monkeypatch.setenv("MINIO_BUCKET", "x")
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))

    from central_api.app import create_app
    app = create_app()
    with TestClient(app) as client:
        auth = client.post(
            "/oauth/device_authorization",
            json={"client_id": "agent", "scope": "agent.provision"},
        ).json()
        await app.state.device_code_store.redeem_user_code(
            auth["user_code"], tenant_id="presidente-epitacio",
        )
        tok = client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": auth["device_code"],
                "client_id": "agent",
            },
        ).json()
        access = tok["access_token"]
        r = client.post(
            "/provision/cert",
            headers={"Authorization": f"Bearer {access}"},
            json={"csr_pem": make_csr_pem("agent-host-001").decode(),
                  "machine_fingerprint": "fp:e2e:host001"},
        )
    assert r.status_code == 200
    body = r.json()
    cert = x509.load_pem_x509_certificate(body["cert_pem"].encode())
    chain = x509.load_pem_x509_certificate(body["ca_chain_pem"].encode())
    assert cert.issuer == chain.subject
    delta = cert.not_valid_after_utc - cert.not_valid_before_utc
    assert 89 <= delta.days <= 91
