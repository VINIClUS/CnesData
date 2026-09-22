"""Testes da identidade mTLS do Edge Agent (agent_auth)."""
from __future__ import annotations

import base64
import datetime as dt
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from central_api.agent_auth import (
    AgentCertIdentity,
    agent_identity_if_required,
    require_agent_cert,
)
from cnes_domain.tenant import get_tenant_id
from cnes_infra.auth import CertAuthority
from cnes_infra.auth.errors import OAuthError

_PROXY = "172.20.0.2"


def _root_ca() -> tuple[bytes, bytes]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "agent-auth-ca")])
    now = dt.datetime.now(dt.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name).issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=1))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return cert.public_bytes(serialization.Encoding.PEM), key_pem


def _authority() -> CertAuthority:
    cert_pem, key_pem = _root_ca()
    return CertAuthority(root_cert_pem=cert_pem, root_key_pem=key_pem)


def _leaf(ca: CertAuthority, machine_id: str = "a1b2c3d4") -> x509.Certificate:
    key = ec.generate_private_key(ec.SECP256R1())
    csr = (
        x509.CertificateSigningRequestBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, machine_id)]))
        .sign(key, hashes.SHA256())
        .public_bytes(serialization.Encoding.PEM)
    )
    pem = ca.issue_cert(csr_pem=csr, agent_id="agent-1", tenant_id="354130")
    return x509.load_pem_x509_certificate(pem)


def _request(
    ca: CertAuthority | None,
    cert: x509.Certificate | None,
    *,
    peer: str = _PROXY,
    active_serial: str | None = None,
    refresh_active: bool = True,
) -> MagicMock:
    headers = {}
    if cert is not None:
        der = cert.public_bytes(serialization.Encoding.DER)
        headers["X-SSL-Client-Cert"] = base64.b64encode(der).decode()
        if active_serial is None:
            active_serial = format(cert.serial_number, "x")
    audit = MagicMock()
    audit.find_active_by_agent_id.return_value = (
        None if active_serial is None else SimpleNamespace(ca_serial=active_serial)
    )
    refresh = MagicMock()
    refresh.has_active_for_agent.return_value = refresh_active
    req = MagicMock()
    req.headers = headers
    req.client.host = peer
    req.app.state.cert_authority = ca
    req.app.state.provisioned_certs = audit
    req.app.state.refresh_token_store = refresh
    return req


@pytest.fixture(autouse=True)
def _proxy_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "172.16.0.0/12")


def _assert_oauth(req: MagicMock, code: str, status: int = 401) -> None:
    with pytest.raises(OAuthError) as exc:
        require_agent_cert(req)
    assert exc.value.code == code
    assert exc.value.status_code == status


def test_retorna_identidade_do_cert_valido_e_fixa_tenant():
    ca = _authority()
    identity = require_agent_cert(_request(ca, _leaf(ca)))
    assert identity == AgentCertIdentity(
        tenant_id="354130", agent_id="agent-1", machine_id="a1b2c3d4",
    )
    assert get_tenant_id() == "354130"


def test_rejeita_header_forjado_vindo_de_peer_fora_da_allowlist():
    ca = _authority()
    _assert_oauth(_request(ca, _leaf(ca), peer="203.0.113.7"), "invalid_token")


def test_rejeita_header_quando_allowlist_vazia(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "")
    ca = _authority()
    _assert_oauth(_request(ca, _leaf(ca)), "invalid_token")


def test_rejeita_requisicao_sem_certificado():
    _assert_oauth(_request(_authority(), None), "invalid_token")


def test_rejeita_cert_emitido_por_outra_ca():
    _assert_oauth(_request(_authority(), _leaf(_authority())), "invalid_token")


def test_retorna_500_quando_ca_nao_configurada():
    cert = _leaf(_authority())
    _assert_oauth(_request(None, cert), "server_error", status=500)


def test_rejeita_cert_sem_registro_ativo():
    ca = _authority()
    req = _request(ca, _leaf(ca))
    req.app.state.provisioned_certs.find_active_by_agent_id.return_value = None
    _assert_oauth(req, "cert_revoked")


def test_rejeita_cert_com_serial_substituido():
    ca = _authority()
    _assert_oauth(_request(ca, _leaf(ca), active_serial="abc123"), "cert_revoked")


def test_rejeita_agente_sem_refresh_token_ativo():
    ca = _authority()
    _assert_oauth(_request(ca, _leaf(ca), refresh_active=False), "agent_revoked")


def test_identidade_opcional_retorna_none_quando_mtls_desligado(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr("cnes_infra.config.AGENT_MTLS_REQUIRED", False)
    assert agent_identity_if_required(_request(None, None, peer="203.0.113.7")) is None


def test_identidade_opcional_exige_cert_quando_mtls_ligado(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr("cnes_infra.config.AGENT_MTLS_REQUIRED", True)
    with pytest.raises(OAuthError):
        agent_identity_if_required(_request(_authority(), None))
