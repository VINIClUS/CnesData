"""Parse + verify the agent cert Caddy forwards in X-SSL-Client-Cert (DER base64).

Caddy (`client_auth verify_if_given`) proves key possession during the TLS
handshake and overwrites X-SSL-Client-Cert. Callers must only trust the header
from an allowlisted proxy peer; `verify_peer_cert` re-checks chain + validity.
"""
from __future__ import annotations

import base64
import binascii
from typing import TYPE_CHECKING

from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.x509.oid import NameOID

from cnes_infra.auth.errors import OAuthError

if TYPE_CHECKING:
    import datetime as dt

    from starlette.requests import Request

_AGENT_ID_OID = x509.ObjectIdentifier("1.3.6.1.4.1.99999.1.2")
_TENANT_OID = x509.ObjectIdentifier("1.3.6.1.4.1.99999.1.1")


def _invalid_token() -> OAuthError:
    return OAuthError("invalid_token", status_code=401)


def extract_peer_cert(request: Request) -> x509.Certificate:
    """Raises: OAuthError 401 se o header estiver ausente ou malformado."""
    cert_header = request.headers.get("X-SSL-Client-Cert", "")
    if not cert_header:
        raise _invalid_token()
    try:
        der = base64.b64decode(cert_header, validate=True)
        return x509.load_der_x509_certificate(der)
    except (binascii.Error, ValueError) as exc:
        raise _invalid_token() from exc


def verify_peer_cert(
    cert: x509.Certificate, root_cert_pem: bytes, now: dt.datetime,
) -> None:
    """Raises: OAuthError 401 se não emitido pela CA ou fora da validade."""
    root = x509.load_pem_x509_certificate(root_cert_pem)
    try:
        cert.verify_directly_issued_by(root)
    except (ValueError, TypeError, InvalidSignature) as exc:
        raise _invalid_token() from exc
    if not cert.not_valid_before_utc <= now <= cert.not_valid_after_utc:
        raise _invalid_token()


def _read_oid_value(
    cert: x509.Certificate, oid: x509.ObjectIdentifier,
) -> str:
    for ext in cert.extensions:
        if ext.oid == oid and isinstance(ext.value, x509.UnrecognizedExtension):
            try:
                return ext.value.value.decode()
            except UnicodeDecodeError as exc:
                raise _invalid_token() from exc
    raise _invalid_token()


def read_agent_id(cert: x509.Certificate) -> str:
    return _read_oid_value(cert, _AGENT_ID_OID)


def read_tenant_id(cert: x509.Certificate) -> str:
    return _read_oid_value(cert, _TENANT_OID)


def read_machine_id(cert: x509.Certificate) -> str:
    """Returns: CN do subject — o agente Go gera o CSR com CN=machine_id."""
    names = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    if not names:
        raise _invalid_token()
    return str(names[0].value)
