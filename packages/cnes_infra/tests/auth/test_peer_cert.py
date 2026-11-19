"""peer_cert: mTLS header parsing, chain check + identity extraction."""

import base64
import datetime as dt
from typing import cast

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from starlette.requests import Request

from cnes_infra.auth.ca import CertAuthority
from cnes_infra.auth.cert_extensions import (
    AGENT_ID_OID,
    TENANT_ID_OID,
    encode_extension_value,
)
from cnes_infra.auth.errors import OAuthError
from cnes_infra.auth.peer_cert import (
    extract_peer_cert,
    read_agent_id,
    read_machine_id,
    read_tenant_id,
    verify_peer_cert,
)

_LEGACY_AGENT_OID = x509.ObjectIdentifier("1.3.6.1.4.1.99999.1.2")
_LEGACY_TENANT_OID = x509.ObjectIdentifier("1.3.6.1.4.1.99999.1.1")
_NOW = dt.datetime.now(dt.UTC)


def _cert_with_extensions(
    extensions: list[tuple[x509.ObjectIdentifier, bytes]],
) -> x509.Certificate:
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "agent-test")])
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(_NOW - dt.timedelta(minutes=1))
        .not_valid_after(_NOW + dt.timedelta(days=90))
    )
    for oid, value in extensions:
        builder = builder.add_extension(
            x509.UnrecognizedExtension(oid, value), critical=False,
        )
    return builder.sign(key, hashes.SHA256())


def _make_leaf_cert(
    *, agent_id: str | None = "agent-001", tenant_id: str | None = "354130"
) -> x509.Certificate:
    extensions: list[tuple[x509.ObjectIdentifier, bytes]] = []
    if agent_id is not None:
        extensions.append((AGENT_ID_OID, encode_extension_value(agent_id)))
    if tenant_id is not None:
        extensions.append((TENANT_ID_OID, encode_extension_value(tenant_id)))
    return _cert_with_extensions(extensions)


def _der_b64(cert: x509.Certificate) -> str:
    return base64.b64encode(cert.public_bytes(serialization.Encoding.DER)).decode()


def _issue(root_ca: tuple[bytes, bytes], csr_pem: bytes) -> x509.Certificate:
    ca = CertAuthority(root_cert_pem=root_ca[0], root_key_pem=root_ca[1])
    leaf_pem = ca.issue_cert(csr_pem=csr_pem, agent_id="agent-1", tenant_id="354130")
    return x509.load_pem_x509_certificate(leaf_pem)


class _FakeRequest:
    def __init__(self, headers: dict[str, str]) -> None:
        self.headers = headers


def _request(headers: dict[str, str]) -> Request:
    return cast("Request", _FakeRequest(headers))


def test_extract_peer_cert_rejeita_header_ausente_retorna_401():
    with pytest.raises(OAuthError) as exc:
        extract_peer_cert(_request({}))
    assert exc.value.code == "invalid_token"
    assert exc.value.status_code == 401


def test_extract_peer_cert_rejeita_header_vazio_retorna_401():
    with pytest.raises(OAuthError):
        extract_peer_cert(_request({"X-SSL-Client-Cert": ""}))


def test_extract_peer_cert_rejeita_base64_invalido_retorna_401():
    with pytest.raises(OAuthError):
        extract_peer_cert(_request({"X-SSL-Client-Cert": "not base64!"}))


def test_extract_peer_cert_rejeita_der_malformado_retorna_401():
    garbage = base64.b64encode(b"not a certificate").decode()
    with pytest.raises(OAuthError):
        extract_peer_cert(_request({"X-SSL-Client-Cert": garbage}))


def test_extract_peer_cert_ignora_verify_header_forjado():
    headers = {"X-SSL-Client-Verify": "SUCCESS"}
    with pytest.raises(OAuthError):
        extract_peer_cert(_request(headers))


def test_extract_peer_cert_aceita_der_base64():
    leaf = _make_leaf_cert()
    cert = extract_peer_cert(_request({"X-SSL-Client-Cert": _der_b64(leaf)}))
    assert cert.serial_number == leaf.serial_number


def test_verify_peer_cert_aceita_cert_emitido_pela_ca(test_root_ca, test_csr_pem):
    leaf = _issue(test_root_ca, test_csr_pem)
    valid_at = (
        leaf.not_valid_before_utc + (leaf.not_valid_after_utc - leaf.not_valid_before_utc) / 2
    )
    verify_peer_cert(leaf, test_root_ca[0], valid_at)


def test_verify_peer_cert_rejeita_cert_de_ca_estranha(test_root_ca):
    with pytest.raises(OAuthError) as exc:
        verify_peer_cert(_make_leaf_cert(), test_root_ca[0], _NOW)
    assert exc.value.status_code == 401


def test_verify_peer_cert_rejeita_assinatura_de_outra_chave_com_mesmo_issuer(
    test_root_ca,
    test_csr_pem,
):
    root = x509.load_pem_x509_certificate(test_root_ca[0])
    impostor_key = ec.generate_private_key(ec.SECP256R1())
    impostor_key_pem = impostor_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    impostor_root = (
        x509.CertificateBuilder()
        .subject_name(root.subject)
        .issuer_name(root.subject)
        .public_key(impostor_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(_NOW - dt.timedelta(minutes=1))
        .not_valid_after(_NOW + dt.timedelta(days=1))
        .sign(impostor_key, hashes.SHA256())
        .public_bytes(serialization.Encoding.PEM)
    )
    leaf = _issue((impostor_root, impostor_key_pem), test_csr_pem)
    with pytest.raises(OAuthError):
        verify_peer_cert(leaf, test_root_ca[0], _NOW)


def test_verify_peer_cert_rejeita_cert_expirado(test_root_ca, test_csr_pem):
    leaf = _issue(test_root_ca, test_csr_pem)
    with pytest.raises(OAuthError):
        verify_peer_cert(leaf, test_root_ca[0], _NOW + dt.timedelta(days=91))


def test_verify_peer_cert_rejeita_cert_ainda_nao_valido(test_root_ca, test_csr_pem):
    leaf = _issue(test_root_ca, test_csr_pem)
    with pytest.raises(OAuthError):
        verify_peer_cert(leaf, test_root_ca[0], _NOW - dt.timedelta(days=1))


def test_read_machine_id_extrai_common_name(test_root_ca, test_csr_pem):
    leaf = _issue(test_root_ca, test_csr_pem)
    assert read_machine_id(leaf) == "agent-machine-001"


def test_read_machine_id_levanta_401_sem_common_name():
    key = ec.generate_private_key(ec.SECP256R1())
    empty = x509.Name([])
    cert = (
        x509.CertificateBuilder()
        .subject_name(empty)
        .issuer_name(empty)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(_NOW - dt.timedelta(minutes=1))
        .not_valid_after(_NOW + dt.timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    with pytest.raises(OAuthError) as exc:
        read_machine_id(cert)
    assert exc.value.status_code == 401


def test_read_agent_id_extrai_oid():
    assert read_agent_id(_make_leaf_cert(agent_id="agent-xyz")) == "agent-xyz"


def test_read_tenant_id_extrai_oid():
    assert read_tenant_id(_make_leaf_cert(tenant_id="354130")) == "354130"


def test_read_oid_levanta_401_quando_oid_ausente():
    with pytest.raises(OAuthError) as exc:
        read_agent_id(_make_leaf_cert(agent_id=None))
    assert exc.value.code == "invalid_token"
    assert exc.value.status_code == 401


def test_read_oid_levanta_401_quando_oid_tem_bytes_invalidos_utf8():
    """Critical regression: malformed UTF-8 in OID must NOT crash 500."""
    cert = _cert_with_extensions([(AGENT_ID_OID, b"\x0c\x03\xff\xfe\xfd")])
    with pytest.raises(OAuthError) as exc:
        read_agent_id(cert)
    assert exc.value.code == "invalid_token"
    assert exc.value.status_code == 401


def test_rejeita_valor_utf8_cru_sem_der_com_401():
    cert = _cert_with_extensions(
        [(AGENT_ID_OID, b"agent-001"), (TENANT_ID_OID, b"354130")],
    )
    for reader in (read_agent_id, read_tenant_id):
        with pytest.raises(OAuthError) as exc:
            reader(cert)
        assert (exc.value.code, exc.value.status_code) == ("invalid_token", 401)


@pytest.mark.parametrize("value", [b"agent-001", b"\x0c\x09agent-001"])
def test_rejeita_certificado_com_oid_placeholder_99999_com_401(value):
    cert = _cert_with_extensions(
        [(_LEGACY_AGENT_OID, value), (_LEGACY_TENANT_OID, value)],
    )
    for reader in (read_agent_id, read_tenant_id):
        with pytest.raises(OAuthError) as exc:
            reader(cert)
        assert (exc.value.code, exc.value.status_code) == ("invalid_token", 401)
