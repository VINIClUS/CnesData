"""Identidade OIDC e descoberta de memberships canônicas do perfil AWS."""

import logging

from fastapi import HTTPException, Request
from pydantic import BaseModel

from cnes_infra.auth.oidc import OidcPrincipal

logger = logging.getLogger(__name__)


class AwsMeResponse(BaseModel):
    subject: str
    issuer: str
    email: str | None
    display_name: str | None


class MembershipResponse(BaseModel):
    tenant_id: str
    municipality_name: str
    role: str


def require_principal(request: Request) -> OidcPrincipal:
    principal = getattr(request.state, "principal", None)
    if not isinstance(principal, OidcPrincipal):
        raise HTTPException(status_code=401, detail="auth_required")
    return principal


def memberships(request: Request) -> list[MembershipResponse]:
    principal = require_principal(request)
    runtime = request.app.state.runtime
    try:
        grants = runtime.services.membership_authorizer.list_authorized(principal)
        result: list[MembershipResponse] = []
        for grant in sorted(grants, key=lambda grant: grant.tenant_id):
            tenant = runtime.control_plane.get_tenant(grant.tenant_id)
            if tenant is None:
                raise ValueError("canonical_tenant_missing")
            result.append(
                MembershipResponse(
                    tenant_id=grant.tenant_id,
                    municipality_name=tenant.municipality_name,
                    role=grant.role,
                )
            )
        return result
    except Exception:
        logger.exception("tenant_discovery_unavailable")
        raise HTTPException(status_code=503, detail="discovery_unavailable") from None
