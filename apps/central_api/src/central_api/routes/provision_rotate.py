"""Cert rotation route — POST /provision/cert/rotate (mTLS-gated)."""
from __future__ import annotations

from cryptography import x509
from fastapi import APIRouter, Request

from cnes_infra.auth import (
    CertRotateRequest,
    CertRotateResponse,
    extract_peer_cert,
    read_agent_id,
    read_tenant_id,
)
from cnes_infra.auth.errors import OAuthError

router = APIRouter(tags=["provision"])


@router.post("/provision/cert/rotate", response_model=CertRotateResponse)
async def provision_cert_rotate(
    body: CertRotateRequest,
    request: Request,
) -> CertRotateResponse:
    cert = extract_peer_cert(request)
    agent_id = read_agent_id(cert)
    tenant_id = read_tenant_id(cert)

    audit = request.app.state.provisioned_certs
    active = audit.find_active_by_agent_id(agent_id)
    if active is None:
        raise OAuthError("cert_revoked", status_code=401)
    presented_serial = format(cert.serial_number, "x")
    if presented_serial != active.ca_serial:
        raise OAuthError("cert_revoked", status_code=401)

    refresh_store = request.app.state.refresh_token_store
    if not refresh_store.has_active_for_agent(agent_id):
        raise OAuthError("agent_revoked", status_code=401)

    ca = request.app.state.cert_authority
    if ca is None:
        raise OAuthError(
            "server_error", description="ca_not_configured",
            status_code=500,
        )
    ttl_days = request.app.state.cert_ttl_days
    try:
        leaf_pem = ca.issue_cert(
            csr_pem=body.csr_pem.encode(),
            agent_id=agent_id, tenant_id=tenant_id, ttl_days=ttl_days,
        )
    except ValueError as exc:
        raise OAuthError("invalid_request", description=str(exc)) from exc

    leaf = x509.load_pem_x509_certificate(leaf_pem)
    subject_cn = leaf.subject.get_attributes_for_oid(
        x509.NameOID.COMMON_NAME,
    )[0].value
    audit.record(
        agent_id=agent_id, tenant_id=tenant_id, subject_cn=subject_cn,
        ca_serial=format(leaf.serial_number, "x"),
        expires_at=leaf.not_valid_after_utc,
    )
    return CertRotateResponse(
        cert_pem=leaf_pem.decode(),
        ca_chain_pem=ca.root_cert_pem.decode(),
        expires_at=leaf.not_valid_after_utc,
    )
