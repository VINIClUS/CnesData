"""Provision routes — POST /provision/cert (cert exchange)."""
from __future__ import annotations

from cryptography import x509
from cryptography.x509.oid import NameOID
from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from cnes_infra.auth import CertProvisionResponse
from cnes_infra.auth.errors import OAuthError

router = APIRouter(tags=["provision"])


class ProvisionCertRequest(BaseModel):
    csr_pem: str = Field(min_length=10)
    machine_fingerprint: str = Field(min_length=8, max_length=128)


def _extract_bearer(request: Request) -> str:
    header = request.headers.get("Authorization", "")
    if not header.lower().startswith("bearer "):
        raise OAuthError("invalid_token", status_code=401)
    return header[7:].strip()


@router.post("/provision/cert", response_model=CertProvisionResponse)
async def provision_cert(
    body: ProvisionCertRequest,
    request: Request,
) -> CertProvisionResponse:
    token_str = _extract_bearer(request)
    access_store = request.app.state.access_token_store
    access = await access_store.consume(token_str)
    if access is None:
        raise OAuthError("invalid_token", status_code=401)

    ca = request.app.state.cert_authority
    if ca is None:
        raise OAuthError(
            "server_error",
            description="ca_not_configured",
            status_code=500,
        )
    ttl_days = request.app.state.cert_ttl_days
    try:
        leaf_pem = ca.issue_cert(
            csr_pem=body.csr_pem.encode(),
            agent_id=access.agent_id,
            tenant_id=access.tenant_id,
            ttl_days=ttl_days,
        )
    except ValueError as exc:
        raise OAuthError(
            "invalid_request", description=str(exc),
        ) from exc

    leaf = x509.load_pem_x509_certificate(leaf_pem)
    subject_cn = leaf.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
    ca_serial = format(leaf.serial_number, "x")
    expires_at = leaf.not_valid_after_utc

    refresh_store = request.app.state.refresh_token_store
    refresh_token = refresh_store.create(
        agent_id=access.agent_id,
        tenant_id=access.tenant_id,
        machine_fingerprint=body.machine_fingerprint,
    )

    audit = request.app.state.provisioned_certs
    audit.record(
        agent_id=access.agent_id,
        tenant_id=access.tenant_id,
        subject_cn=subject_cn,
        ca_serial=ca_serial,
        expires_at=expires_at,
    )

    return CertProvisionResponse(
        cert_pem=leaf_pem.decode(),
        ca_chain_pem=ca.root_cert_pem.decode(),
        refresh_token=refresh_token,
        expires_at=expires_at,
    )
