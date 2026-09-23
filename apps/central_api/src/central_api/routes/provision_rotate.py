"""Cert rotation route — POST /provision/cert/rotate (mTLS-gated)."""
from __future__ import annotations

from typing import Annotated

from cryptography import x509
from fastapi import APIRouter, Depends, Request

from central_api.agent_auth import AgentCertIdentity, require_agent_cert
from cnes_domain.tenant import set_tenant_id
from cnes_infra.auth import CertRotateRequest, CertRotateResponse
from cnes_infra.auth.errors import OAuthError

router = APIRouter(tags=["provision"])


@router.post("/provision/cert/rotate", response_model=CertRotateResponse)
async def provision_cert_rotate(
    body: CertRotateRequest,
    request: Request,
    identity: Annotated[AgentCertIdentity, Depends(require_agent_cert)],
) -> CertRotateResponse:
    set_tenant_id(identity.tenant_id)
    ca = request.app.state.cert_authority
    ttl_days = request.app.state.cert_ttl_days
    try:
        leaf_pem = ca.issue_cert(
            csr_pem=body.csr_pem.encode(),
            agent_id=identity.agent_id, tenant_id=identity.tenant_id,
            ttl_days=ttl_days,
        )
    except ValueError as exc:
        raise OAuthError("invalid_request", description=str(exc)) from exc

    leaf = x509.load_pem_x509_certificate(leaf_pem)
    subject_cn = leaf.subject.get_attributes_for_oid(
        x509.NameOID.COMMON_NAME,
    )[0].value
    request.app.state.provisioned_certs.record(
        agent_id=identity.agent_id, tenant_id=identity.tenant_id,
        subject_cn=subject_cn, ca_serial=format(leaf.serial_number, "x"),
        expires_at=leaf.not_valid_after_utc,
    )
    return CertRotateResponse(
        cert_pem=leaf_pem.decode(),
        ca_chain_pem=ca.root_cert_pem.decode(),
        expires_at=leaf.not_valid_after_utc,
    )
