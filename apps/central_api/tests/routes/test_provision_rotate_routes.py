"""Tests for /provision/cert/rotate."""
import base64
import datetime as dt

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from fastapi.testclient import TestClient
from sqlalchemy import text

_AGENT_OID = x509.ObjectIdentifier("1.3.6.1.4.1.99999.1.2")
_TENANT_OID = x509.ObjectIdentifier("1.3.6.1.4.1.99999.1.1")

_PROXY_PEER = ("172.20.0.2", 50000)

_TEST_AGENT_IDS = (
    "agent-rot-401a", "agent-rot-401b-orphan", "agent-rot-401c", "agent-rot-401d",
    "agent-rot-400a", "agent-rot-200a", "agent-rot-200b", "agent-rot-200c",
    "agent-rot-200d", "agent-rot-200e", "agent-rot-500a",
)


@pytest.fixture(autouse=True)
def _limpar_rotate_tables(pg_engine):
    """Clean up rows for these test agent_ids before each test."""
    with pg_engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM auth_provisioned_certs WHERE agent_id = ANY(:ids)",
            ),
            {"ids": list(_TEST_AGENT_IDS)},
        )
        conn.execute(
            text(
                "DELETE FROM auth_refresh_tokens WHERE agent_id = ANY(:ids)",
            ),
            {"ids": list(_TEST_AGENT_IDS)},
        )


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
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "172.16.0.0/12")
    from central_api.app import create_app
    return create_app()


def _sign_leaf(session_root_ca, *, agent_id="agent-rot-001",
               tenant_id="354130", ca_serial_override=None,
               include_oids=True):
    """Sign a leaf cert directly with the test CA. Returns (PEM bytes, hex serial)."""
    ca_cert_path, ca_key_path = session_root_ca
    ca_cert = x509.load_pem_x509_certificate(ca_cert_path.read_bytes())
    ca_key = serialization.load_pem_private_key(
        ca_key_path.read_bytes(), password=None,
    )
    assert isinstance(ca_key, ec.EllipticCurvePrivateKey)
    leaf_key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "agent-machine-rot")])
    serial = ca_serial_override or x509.random_serial_number()
    now = dt.datetime.now(dt.UTC)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(leaf_key.public_key())
        .serial_number(serial)
        .not_valid_before(now - dt.timedelta(minutes=1))
        .not_valid_after(now + dt.timedelta(days=90))
    )
    if include_oids:
        builder = builder.add_extension(
            x509.UnrecognizedExtension(_AGENT_OID, agent_id.encode()),
            critical=False,
        ).add_extension(
            x509.UnrecognizedExtension(_TENANT_OID, tenant_id.encode()),
            critical=False,
        )
    leaf = builder.sign(ca_key, hashes.SHA256())
    return leaf.public_bytes(serialization.Encoding.PEM), format(serial, "x")


def _mtls_headers(pem_bytes):
    der = x509.load_pem_x509_certificate(pem_bytes).public_bytes(
        serialization.Encoding.DER,
    )
    return {"X-SSL-Client-Cert": base64.b64encode(der).decode()}


def test_rotate_sem_headers_de_mtls_retorna_401(session_root_ca, monkeypatch, make_csr_pem):
    app = _make_app(session_root_ca, monkeypatch)
    with TestClient(app, client=_PROXY_PEER) as client:
        r = client.post(
            "/provision/cert/rotate",
            json={"csr_pem": make_csr_pem().decode()},
        )
    assert r.status_code == 401
    assert r.json() == {"error": "invalid_token"}


def test_rotate_rejeita_header_forjado_de_peer_fora_da_allowlist(
    session_root_ca, monkeypatch, make_csr_pem,
):
    app = _make_app(session_root_ca, monkeypatch)
    pem_bytes, _ = _sign_leaf(session_root_ca)
    with TestClient(app, client=("203.0.113.7", 50000)) as client:
        r = client.post(
            "/provision/cert/rotate", headers=_mtls_headers(pem_bytes),
            json={"csr_pem": make_csr_pem().decode()},
        )
    assert r.status_code == 401
    assert r.json() == {"error": "invalid_token"}


def test_rotate_com_cert_sem_agent_id_oid_retorna_401(
    session_root_ca, monkeypatch, make_csr_pem,
):
    app = _make_app(session_root_ca, monkeypatch)
    pem_bytes, _ = _sign_leaf(session_root_ca, include_oids=False)
    with TestClient(app, client=_PROXY_PEER) as client:
        r = client.post(
            "/provision/cert/rotate", headers=_mtls_headers(pem_bytes),
            json={"csr_pem": make_csr_pem().decode()},
        )
    assert r.status_code == 401
    assert r.json() == {"error": "invalid_token"}


@pytest.mark.postgres
def test_rotate_com_serial_diferente_do_audit_retorna_cert_revoked(
    session_root_ca, monkeypatch, make_csr_pem, pg_engine,
):
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    app = _make_app(session_root_ca, monkeypatch)
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    pem_bytes, _ = _sign_leaf(session_root_ca, agent_id="agent-rot-401a")
    expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=90)
    with TestClient(app, client=_PROXY_PEER) as client:
        app.state.provisioned_certs.record(
            agent_id="agent-rot-401a", tenant_id="354130",
            subject_cn="cn", ca_serial="serial-different",
            expires_at=expires,
        )
        app.state.refresh_token_store.create(
            agent_id="agent-rot-401a", tenant_id="354130",
            machine_fingerprint="fp:401a",
        )
        r = client.post(
            "/provision/cert/rotate", headers=_mtls_headers(pem_bytes),
            json={"csr_pem": make_csr_pem().decode()},
        )
    assert r.status_code == 401
    assert r.json() == {"error": "cert_revoked"}


@pytest.mark.postgres
def test_rotate_quando_agent_sem_audit_retorna_cert_revoked(
    session_root_ca, monkeypatch, make_csr_pem, pg_engine,
):
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    app = _make_app(session_root_ca, monkeypatch)
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    pem_bytes, _ = _sign_leaf(session_root_ca, agent_id="agent-rot-401b-orphan")
    with TestClient(app, client=_PROXY_PEER) as client:
        r = client.post(
            "/provision/cert/rotate", headers=_mtls_headers(pem_bytes),
            json={"csr_pem": make_csr_pem().decode()},
        )
    assert r.status_code == 401
    assert r.json() == {"error": "cert_revoked"}


@pytest.mark.postgres
def test_rotate_quando_agent_sem_refresh_token_retorna_agent_revoked(
    session_root_ca, monkeypatch, make_csr_pem, pg_engine,
):
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    app = _make_app(session_root_ca, monkeypatch)
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    pem_bytes, hex_serial = _sign_leaf(session_root_ca, agent_id="agent-rot-401c")
    expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=90)
    with TestClient(app, client=_PROXY_PEER) as client:
        app.state.provisioned_certs.record(
            agent_id="agent-rot-401c", tenant_id="354130",
            subject_cn="cn", ca_serial=hex_serial, expires_at=expires,
        )
        r = client.post(
            "/provision/cert/rotate", headers=_mtls_headers(pem_bytes),
            json={"csr_pem": make_csr_pem().decode()},
        )
    assert r.status_code == 401
    assert r.json() == {"error": "agent_revoked"}


@pytest.mark.postgres
def test_rotate_quando_refresh_token_revogado_retorna_agent_revoked(
    session_root_ca, monkeypatch, make_csr_pem, pg_engine,
):
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    app = _make_app(session_root_ca, monkeypatch)
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    pem_bytes, hex_serial = _sign_leaf(session_root_ca, agent_id="agent-rot-401d")
    expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=90)
    with TestClient(app, client=_PROXY_PEER) as client:
        app.state.provisioned_certs.record(
            agent_id="agent-rot-401d", tenant_id="354130",
            subject_cn="cn", ca_serial=hex_serial, expires_at=expires,
        )
        token = app.state.refresh_token_store.create(
            agent_id="agent-rot-401d", tenant_id="354130",
            machine_fingerprint="fp:401d",
        )
        app.state.refresh_token_store.revoke(token)
        r = client.post(
            "/provision/cert/rotate", headers=_mtls_headers(pem_bytes),
            json={"csr_pem": make_csr_pem().decode()},
        )
    assert r.status_code == 401
    assert r.json() == {"error": "agent_revoked"}


@pytest.mark.postgres
def test_rotate_com_csr_invalido_retorna_invalid_request(
    session_root_ca, monkeypatch, pg_engine,
):
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    app = _make_app(session_root_ca, monkeypatch)
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    pem_bytes, hex_serial = _sign_leaf(session_root_ca, agent_id="agent-rot-400a")
    expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=90)
    with TestClient(app, client=_PROXY_PEER) as client:
        app.state.provisioned_certs.record(
            agent_id="agent-rot-400a", tenant_id="354130",
            subject_cn="cn", ca_serial=hex_serial, expires_at=expires,
        )
        app.state.refresh_token_store.create(
            agent_id="agent-rot-400a", tenant_id="354130",
            machine_fingerprint="fp:400a",
        )
        r = client.post(
            "/provision/cert/rotate", headers=_mtls_headers(pem_bytes),
            json={"csr_pem": "not a real CSR with enough chars"},
        )
    assert r.status_code == 400
    assert r.json()["error"] == "invalid_request"


@pytest.mark.postgres
def test_rotate_com_chain_valida_emite_novo_cert_de_90_dias(
    session_root_ca, monkeypatch, make_csr_pem, pg_engine,
):
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    app = _make_app(session_root_ca, monkeypatch)
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    pem_bytes, hex_serial = _sign_leaf(session_root_ca, agent_id="agent-rot-200a")
    expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=90)
    with TestClient(app, client=_PROXY_PEER) as client:
        app.state.provisioned_certs.record(
            agent_id="agent-rot-200a", tenant_id="354130",
            subject_cn="cn", ca_serial=hex_serial, expires_at=expires,
        )
        app.state.refresh_token_store.create(
            agent_id="agent-rot-200a", tenant_id="354130",
            machine_fingerprint="fp:200a",
        )
        r = client.post(
            "/provision/cert/rotate", headers=_mtls_headers(pem_bytes),
            json={"csr_pem": make_csr_pem().decode()},
        )
    assert r.status_code == 200
    body = r.json()
    assert "BEGIN CERTIFICATE" in body["cert_pem"]
    assert "BEGIN CERTIFICATE" in body["ca_chain_pem"]
    parsed = dt.datetime.fromisoformat(body["expires_at"])
    delta = parsed - dt.datetime.now(dt.UTC)
    assert 89 <= delta.days <= 91


@pytest.mark.postgres
def test_rotate_grava_nova_linha_em_auth_provisioned_certs(
    session_root_ca, monkeypatch, make_csr_pem, pg_engine,
):
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    app = _make_app(session_root_ca, monkeypatch)
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    pem_bytes, hex_serial = _sign_leaf(session_root_ca, agent_id="agent-rot-200b")
    expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=90)
    with TestClient(app, client=_PROXY_PEER) as client:
        app.state.provisioned_certs.record(
            agent_id="agent-rot-200b", tenant_id="354130",
            subject_cn="cn-old", ca_serial=hex_serial, expires_at=expires,
        )
        app.state.refresh_token_store.create(
            agent_id="agent-rot-200b", tenant_id="354130",
            machine_fingerprint="fp:200b",
        )
        client.post(
            "/provision/cert/rotate", headers=_mtls_headers(pem_bytes),
            json={"csr_pem": make_csr_pem().decode()},
        )
    with pg_engine.connect() as conn:
        count = conn.execute(
            text(
                "SELECT COUNT(*) FROM auth_provisioned_certs "
                "WHERE agent_id = 'agent-rot-200b'",
            ),
        ).scalar()
    assert count == 2  # Old (seeded) + new (rotation)


@pytest.mark.postgres
def test_rotate_nao_revoga_linha_anterior_overlap_periodo(
    session_root_ca, monkeypatch, make_csr_pem, pg_engine,
):
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    app = _make_app(session_root_ca, monkeypatch)
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    pem_bytes, hex_serial = _sign_leaf(session_root_ca, agent_id="agent-rot-200c")
    expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=90)
    with TestClient(app, client=_PROXY_PEER) as client:
        app.state.provisioned_certs.record(
            agent_id="agent-rot-200c", tenant_id="354130",
            subject_cn="cn-old", ca_serial=hex_serial, expires_at=expires,
        )
        app.state.refresh_token_store.create(
            agent_id="agent-rot-200c", tenant_id="354130",
            machine_fingerprint="fp:200c",
        )
        client.post(
            "/provision/cert/rotate", headers=_mtls_headers(pem_bytes),
            json={"csr_pem": make_csr_pem().decode()},
        )
    with pg_engine.connect() as conn:
        old_revoked = conn.execute(
            text(
                "SELECT revoked_at FROM auth_provisioned_certs "
                "WHERE agent_id = 'agent-rot-200c' AND ca_serial = :s",
            ),
            {"s": hex_serial},
        ).scalar()
    assert old_revoked is None


@pytest.mark.postgres
def test_rotate_preserva_refresh_token_existente(
    session_root_ca, monkeypatch, make_csr_pem, pg_engine,
):
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    app = _make_app(session_root_ca, monkeypatch)
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    pem_bytes, hex_serial = _sign_leaf(session_root_ca, agent_id="agent-rot-200d")
    expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=90)
    with TestClient(app, client=_PROXY_PEER) as client:
        app.state.provisioned_certs.record(
            agent_id="agent-rot-200d", tenant_id="354130",
            subject_cn="cn", ca_serial=hex_serial, expires_at=expires,
        )
        app.state.refresh_token_store.create(
            agent_id="agent-rot-200d", tenant_id="354130",
            machine_fingerprint="fp:200d",
        )
        with pg_engine.connect() as conn:
            before = conn.execute(
                text(
                    "SELECT COUNT(*) FROM auth_refresh_tokens "
                    "WHERE agent_id = 'agent-rot-200d' AND revoked_at IS NULL",
                ),
            ).scalar()
        client.post(
            "/provision/cert/rotate", headers=_mtls_headers(pem_bytes),
            json={"csr_pem": make_csr_pem().decode()},
        )
        with pg_engine.connect() as conn:
            after = conn.execute(
                text(
                    "SELECT COUNT(*) FROM auth_refresh_tokens "
                    "WHERE agent_id = 'agent-rot-200d' AND revoked_at IS NULL",
                ),
            ).scalar()
    assert before == 1
    assert after == 1


@pytest.mark.postgres
def test_rotate_com_ca_nao_configurada_retorna_500(
    session_root_ca, monkeypatch, make_csr_pem, pg_engine,
):
    """When app.state.cert_authority is None, route returns 500 server_error.

    Seeds full audit + refresh state to bypass earlier checks. Then nulls
    cert_authority post-construction to exercise the CA-missing branch.
    """
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    app = _make_app(session_root_ca, monkeypatch)
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    pem_bytes, hex_serial = _sign_leaf(session_root_ca, agent_id="agent-rot-500a")
    expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=90)
    with TestClient(app, client=_PROXY_PEER) as client:
        app.state.provisioned_certs.record(
            agent_id="agent-rot-500a", tenant_id="354130",
            subject_cn="cn", ca_serial=hex_serial, expires_at=expires,
        )
        app.state.refresh_token_store.create(
            agent_id="agent-rot-500a", tenant_id="354130",
            machine_fingerprint="fp:500a",
        )
        app.state.cert_authority = None
        r = client.post(
            "/provision/cert/rotate", headers=_mtls_headers(pem_bytes),
            json={"csr_pem": make_csr_pem().decode()},
        )
    assert r.status_code == 500
    assert r.json()["error"] == "server_error"


@pytest.mark.postgres
def test_rotate_aceita_cert_anterior_durante_overlap(
    session_root_ca, monkeypatch, make_csr_pem, pg_engine,
):
    """Após rotacionar, conexões keep-alive ainda apresentam o cert antigo."""
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    app = _make_app(session_root_ca, monkeypatch)
    monkeypatch.setenv("DB_URL", pg_engine.url.render_as_string(hide_password=False))
    old_pem, old_serial = _sign_leaf(session_root_ca, agent_id="agent-rot-200e")
    expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=90)
    with TestClient(app, client=_PROXY_PEER) as client:
        app.state.provisioned_certs.record(
            agent_id="agent-rot-200e", tenant_id="354130",
            subject_cn="cn", ca_serial=old_serial, expires_at=expires,
        )
        app.state.refresh_token_store.create(
            agent_id="agent-rot-200e", tenant_id="354130",
            machine_fingerprint="fp:200e",
        )
        first = client.post(
            "/provision/cert/rotate", headers=_mtls_headers(old_pem),
            json={"csr_pem": make_csr_pem().decode()},
        )
        again_with_old = client.post(
            "/provision/cert/rotate", headers=_mtls_headers(old_pem),
            json={"csr_pem": make_csr_pem().decode()},
        )
    assert first.status_code == 200
    assert again_with_old.status_code == 200
