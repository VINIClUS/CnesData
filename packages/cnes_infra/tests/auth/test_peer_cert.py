"""peer_cert: mTLS header parsing + OID extraction."""
import datetime as dt
import urllib.parse

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from cnes_infra.auth.errors import OAuthError
from cnes_infra.auth.peer_cert import (
    extract_peer_cert,
    read_agent_id,
    read_tenant_id,
)

_AGENT_OID = x509.ObjectIdentifier("1.3.6.1.4.1.99999.1.2")
_TENANT_OID = x509.ObjectIdentifier("1.3.6.1.4.1.99999.1.1")


def _make_leaf_cert(*, agent_id: str | None = "agent-001",
                    tenant_id: str | None = "354130") -> bytes:
    """Build a leaf cert with optional custom OIDs."""
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "agent-test")])
    now = dt.datetime.now(dt.UTC)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=1))
        .not_valid_after(now + dt.timedelta(days=90))
    )
    if agent_id is not None:
        builder = builder.add_extension(
            x509.UnrecognizedExtension(_AGENT_OID, agent_id.encode()),
            critical=False,
        )
    if tenant_id is not None:
        builder = builder.add_extension(
            x509.UnrecognizedExtension(_TENANT_OID, tenant_id.encode()),
            critical=False,
        )
    cert = builder.sign(key, hashes.SHA256())
    return cert.public_bytes(serialization.Encoding.PEM)


class _FakeRequest:
    def __init__(self, headers: dict[str, str]) -> None:
        self.headers = headers


def test_extract_peer_cert_rejeita_verify_ausente_retorna_401():
    req = _FakeRequest({})
    with pytest.raises(OAuthError) as exc:
        extract_peer_cert(req)
    assert exc.value.code == "invalid_token"
    assert exc.value.status_code == 401


def test_extract_peer_cert_rejeita_verify_failed_retorna_401():
    req = _FakeRequest({"X-SSL-Client-Verify": "FAILED:expired"})
    with pytest.raises(OAuthError) as exc:
        extract_peer_cert(req)
    assert exc.value.code == "invalid_token"


def test_extract_peer_cert_rejeita_verify_none_retorna_401():
    req = _FakeRequest({"X-SSL-Client-Verify": "NONE"})
    with pytest.raises(OAuthError):
        extract_peer_cert(req)


def test_extract_peer_cert_rejeita_cert_header_ausente_retorna_401():
    req = _FakeRequest({"X-SSL-Client-Verify": "SUCCESS"})
    with pytest.raises(OAuthError):
        extract_peer_cert(req)


def test_extract_peer_cert_rejeita_pem_malformado_retorna_401():
    req = _FakeRequest({
        "X-SSL-Client-Verify": "SUCCESS",
        "X-SSL-Client-Cert": "not%20a%20cert",
    })
    with pytest.raises(OAuthError):
        extract_peer_cert(req)


def test_extract_peer_cert_aceita_pem_url_escaped():
    pem_bytes = _make_leaf_cert()
    escaped = urllib.parse.quote(pem_bytes.decode())
    req = _FakeRequest({
        "X-SSL-Client-Verify": "SUCCESS",
        "X-SSL-Client-Cert": escaped,
    })
    cert = extract_peer_cert(req)
    assert isinstance(cert, x509.Certificate)


def test_extract_peer_cert_aceita_pem_raw_idempotente():
    pem_bytes = _make_leaf_cert()
    req = _FakeRequest({
        "X-SSL-Client-Verify": "SUCCESS",
        "X-SSL-Client-Cert": pem_bytes.decode(),
    })
    cert = extract_peer_cert(req)
    assert isinstance(cert, x509.Certificate)


def test_read_agent_id_extrai_oid():
    pem_bytes = _make_leaf_cert(agent_id="agent-xyz")
    cert = x509.load_pem_x509_certificate(pem_bytes)
    assert read_agent_id(cert) == "agent-xyz"


def test_read_tenant_id_extrai_oid():
    pem_bytes = _make_leaf_cert(tenant_id="354130")
    cert = x509.load_pem_x509_certificate(pem_bytes)
    assert read_tenant_id(cert) == "354130"


def test_read_oid_levanta_401_quando_oid_ausente():
    pem_bytes = _make_leaf_cert(agent_id=None)
    cert = x509.load_pem_x509_certificate(pem_bytes)
    with pytest.raises(OAuthError) as exc:
        read_agent_id(cert)
    assert exc.value.code == "invalid_token"
    assert exc.value.status_code == 401


def test_read_oid_levanta_401_quando_oid_tem_bytes_invalidos_utf8():
    """Critical regression: malformed UTF-8 in OID must NOT crash 500."""
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "agent-test")])
    now = dt.datetime.now(dt.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject).issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=1))
        .not_valid_after(now + dt.timedelta(days=90))
        .add_extension(
            x509.UnrecognizedExtension(_AGENT_OID, b"\xff\xfe\xfd"),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    pem = cert.public_bytes(serialization.Encoding.PEM)
    parsed = x509.load_pem_x509_certificate(pem)
    with pytest.raises(OAuthError) as exc:
        read_agent_id(parsed)
    assert exc.value.code == "invalid_token"
    assert exc.value.status_code == 401
