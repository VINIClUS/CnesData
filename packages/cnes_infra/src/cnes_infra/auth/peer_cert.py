"""Extract peer cert from ingress-passed mTLS headers.

Ingress (nginx, Traefik) terminates TLS, validates client cert chain
against CA, forwards X-SSL-Client-Cert (URL-escaped PEM) +
X-SSL-Client-Verify (SUCCESS|FAILED:<reason>|NONE).

Server-side trust depends entirely on ingress config; this helper does
NOT re-validate the chain. Wrong ingress config = security hole.
"""
from __future__ import annotations

import urllib.parse
from typing import TYPE_CHECKING

from cryptography import x509

from cnes_infra.auth.errors import OAuthError

if TYPE_CHECKING:
    from starlette.requests import Request

_AGENT_ID_OID = x509.ObjectIdentifier("1.3.6.1.4.1.99999.1.2")
_TENANT_OID = x509.ObjectIdentifier("1.3.6.1.4.1.99999.1.1")


def extract_peer_cert(request: Request) -> x509.Certificate:
    verify = request.headers.get("X-SSL-Client-Verify", "")
    if verify != "SUCCESS":
        raise OAuthError("invalid_token", status_code=401)
    cert_header = request.headers.get("X-SSL-Client-Cert", "")
    if not cert_header:
        raise OAuthError("invalid_token", status_code=401)
    pem = urllib.parse.unquote(cert_header)
    try:
        return x509.load_pem_x509_certificate(pem.encode())
    except ValueError as exc:
        raise OAuthError("invalid_token", status_code=401) from exc


def _read_oid_value(
    cert: x509.Certificate, oid: x509.ObjectIdentifier,
) -> str:
    for ext in cert.extensions:
        if ext.oid == oid and isinstance(ext.value, x509.UnrecognizedExtension):
            try:
                return ext.value.value.decode()
            except UnicodeDecodeError as exc:
                raise OAuthError(
                    "invalid_token", status_code=401,
                ) from exc
    raise OAuthError("invalid_token", status_code=401)


def read_agent_id(cert: x509.Certificate) -> str:
    return _read_oid_value(cert, _AGENT_ID_OID)


def read_tenant_id(cert: x509.Certificate) -> str:
    return _read_oid_value(cert, _TENANT_OID)
