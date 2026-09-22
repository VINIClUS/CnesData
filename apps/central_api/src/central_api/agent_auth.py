"""Identidade do Edge Agent a partir do certificado mTLS repassado pelo Caddy."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from fastapi import Request  # noqa: TC002

from central_api.ratelimit import is_trusted_proxy_peer
from cnes_domain.tenant import set_tenant_id
from cnes_infra import config
from cnes_infra.auth import (
    extract_peer_cert,
    read_agent_id,
    read_machine_id,
    read_tenant_id,
    verify_peer_cert,
)
from cnes_infra.auth.errors import OAuthError


@dataclass(frozen=True, slots=True)
class AgentCertIdentity:
    tenant_id: str
    agent_id: str
    machine_id: str


def _require_active(request: Request, agent_id: str, serial: str) -> None:
    if not request.app.state.provisioned_certs.is_serial_active(agent_id, serial):
        raise OAuthError("cert_revoked", status_code=401)
    if not request.app.state.refresh_token_store.has_active_for_agent(agent_id):
        raise OAuthError("agent_revoked", status_code=401)


def require_agent_cert(request: Request) -> AgentCertIdentity:
    """Raises: OAuthError 401 (cert ausente/inválido/revogado) ou 500 (CA ausente)."""
    if not is_trusted_proxy_peer(request):
        raise OAuthError("invalid_token", status_code=401)
    cert = extract_peer_cert(request)
    ca = request.app.state.cert_authority
    if ca is None:
        raise OAuthError(
            "server_error", description="ca_not_configured", status_code=500,
        )
    verify_peer_cert(cert, ca.root_cert_pem, datetime.now(UTC))
    identity = AgentCertIdentity(
        tenant_id=read_tenant_id(cert),
        agent_id=read_agent_id(cert),
        machine_id=read_machine_id(cert),
    )
    set_tenant_id(identity.tenant_id)
    _require_active(request, identity.agent_id, format(cert.serial_number, "x"))
    return identity


def agent_identity_if_required(request: Request) -> AgentCertIdentity | None:
    """Returns: None somente quando AGENT_MTLS_REQUIRED=false (stack local)."""
    if not config.AGENT_MTLS_REQUIRED:
        return None
    return require_agent_cert(request)
