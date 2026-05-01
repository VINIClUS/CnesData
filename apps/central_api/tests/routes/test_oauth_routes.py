"""Tests for /oauth/device_authorization + /oauth/token routes."""
import pytest
from fastapi.testclient import TestClient

from cnes_infra.auth import AccessTokenStore, DeviceCodeStore


def _make_app(session_root_ca, monkeypatch, *, fixed_now=None):
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
    app = create_app()
    if fixed_now is not None:
        app.state.device_code_store = DeviceCodeStore(now=fixed_now)
        app.state.access_token_store = AccessTokenStore(now=fixed_now)
    return app


def test_device_authorization_retorna_payload_rfc8628(session_root_ca, monkeypatch):
    app = _make_app(session_root_ca, monkeypatch)
    with TestClient(app) as client:
        r = client.post(
            "/oauth/device_authorization",
            json={"client_id": "agent", "scope": "agent.provision"},
        )
    assert r.status_code == 200
    body = r.json()
    assert "device_code" in body
    assert len(body["device_code"]) == 43
    assert "user_code" in body
    assert len(body["user_code"]) == 9
    assert body["verification_uri"] == "https://example/activate"
    assert body["verification_uri_complete"].endswith(body["user_code"])
    assert body["expires_in"] == 600
    assert body["interval"] == 5


def test_device_authorization_rejeita_client_id_invalido(session_root_ca, monkeypatch):
    app = _make_app(session_root_ca, monkeypatch)
    with TestClient(app) as client:
        r = client.post(
            "/oauth/device_authorization",
            json={"client_id": "x", "scope": "agent.provision"},
        )
    assert r.status_code == 422


def test_token_retorna_authorization_pending(session_root_ca, monkeypatch):
    app = _make_app(session_root_ca, monkeypatch)
    with TestClient(app) as client:
        auth = client.post(
            "/oauth/device_authorization",
            json={"client_id": "agent", "scope": "agent.provision"},
        ).json()
        r = client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": auth["device_code"],
                "client_id": "agent",
            },
        )
    assert r.status_code == 400
    assert r.json() == {"error": "authorization_pending"}


def test_token_rejeita_grant_type_incorreto(session_root_ca, monkeypatch):
    app = _make_app(session_root_ca, monkeypatch)
    with TestClient(app) as client:
        r = client.post(
            "/oauth/token",
            data={
                "grant_type": "password",
                "device_code": "x",
                "client_id": "agent",
            },
        )
    assert r.status_code == 400
    assert r.json()["error"] == "unsupported_grant_type"


def test_token_retorna_invalid_grant_para_device_code_desconhecido(
    session_root_ca, monkeypatch,
):
    app = _make_app(session_root_ca, monkeypatch)
    with TestClient(app) as client:
        r = client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": "fantasma",
                "client_id": "agent",
            },
        )
    assert r.status_code == 400
    assert r.json()["error"] == "expired_token"


@pytest.mark.asyncio
async def test_token_retorna_access_token_apos_autorizacao(
    session_root_ca, monkeypatch,
):
    app = _make_app(session_root_ca, monkeypatch)
    with TestClient(app) as client:
        auth = client.post(
            "/oauth/device_authorization",
            json={"client_id": "agent", "scope": "agent.provision"},
        ).json()
        await app.state.device_code_store.redeem_user_code(
            auth["user_code"], tenant_id="presidente-epitacio",
        )
        r = client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": auth["device_code"],
                "client_id": "agent",
            },
        )
    assert r.status_code == 200
    body = r.json()
    assert body["token_type"] == "Bearer"  # noqa: S105
    assert body["expires_in"] == 300
    assert body["refresh_token"] is None
    assert len(body["access_token"]) == 43


def test_token_aciona_slow_down_em_poll_rapido(session_root_ca, monkeypatch):
    fake_t = [0.0]
    app = _make_app(session_root_ca, monkeypatch, fixed_now=lambda: fake_t[0])
    with TestClient(app) as client:
        auth = client.post(
            "/oauth/device_authorization",
            json={"client_id": "agent", "scope": "agent.provision"},
        ).json()
        fake_t[0] = 0.0
        client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": auth["device_code"],
                "client_id": "agent",
            },
        )
        fake_t[0] = 2.0
        r = client.post(
            "/oauth/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": auth["device_code"],
                "client_id": "agent",
            },
        )
    assert r.status_code == 400
    body = r.json()
    assert body["error"] == "slow_down"
    assert body["interval"] == 10
