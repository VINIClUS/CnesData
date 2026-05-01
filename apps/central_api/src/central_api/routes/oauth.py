"""OAuth routes — device authorization + token + activate confirmation."""
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.util import get_remote_address

from central_api.deps import require_auth
from central_api.middleware import AuthenticatedUser
from cnes_infra.auth import (
    DeviceAuthorizationRequest,
    DeviceAuthorizationResponse,
    TokenResponse,
)
from cnes_infra.auth.errors import OAuthError

router = APIRouter(tags=["oauth"])

limiter = Limiter(key_func=get_remote_address)

_DEVICE_CODE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"


class ActivateConfirmRequest(BaseModel):
    user_code: str = Field(min_length=8, max_length=10, pattern=r"^[A-Z0-9-]{8,10}$")
    tenant_id: str = Field(min_length=6, max_length=6)


class ActivateConfirmResponse(BaseModel):
    status: str
    expires_in_seconds: int


@router.post("/oauth/device_authorization", response_model=DeviceAuthorizationResponse)
async def device_authorization(
    body: DeviceAuthorizationRequest,
    request: Request,
) -> DeviceAuthorizationResponse:
    store = request.app.state.device_code_store
    ttl = request.app.state.device_code_ttl
    auth = await store.issue(
        client_id=body.client_id, scope=body.scope, ttl_seconds=ttl,
    )
    base_uri = request.app.state.verification_uri
    return DeviceAuthorizationResponse(
        device_code=auth.device_code,
        user_code=auth.user_code,
        verification_uri=base_uri,
        verification_uri_complete=f"{base_uri}?code={auth.user_code}",
        expires_in=ttl,
        interval=5,
    )


@router.post("/oauth/token", response_model=TokenResponse)
async def token(
    request: Request,
    grant_type: str = Form(...),
    device_code: str = Form(...),
    client_id: str = Form(...),
) -> TokenResponse:
    if grant_type != _DEVICE_CODE_GRANT:
        raise OAuthError("unsupported_grant_type")
    if client_id != "agent":
        raise OAuthError("invalid_client")
    store = request.app.state.device_code_store
    status = await store.poll_device_code(device_code)
    if status.kind == "slow_down":
        raise OAuthError("slow_down", extra={"interval": status.interval})
    if status.kind == "authorization_pending":
        raise OAuthError("authorization_pending")
    if status.kind == "expired_token":
        raise OAuthError("expired_token")
    if status.kind != "authorized":
        raise OAuthError("invalid_grant")
    access_store = request.app.state.access_token_store
    access_ttl = request.app.state.access_token_ttl
    access_token = await access_store.issue(
        tenant_id=status.tenant_id, ttl_seconds=access_ttl,
    )
    return TokenResponse(
        access_token=access_token,
        token_type="Bearer",  # noqa: S106
        expires_in=access_ttl,
        refresh_token=None,
    )


@router.post("/activate/confirm", response_model=ActivateConfirmResponse)
@limiter.limit("10/minute")
async def activate_confirm(
    body: ActivateConfirmRequest,
    request: Request,
    user: AuthenticatedUser = Depends(require_auth),
) -> ActivateConfirmResponse:
    if body.tenant_id not in user.tenant_ids:
        raise HTTPException(status_code=403, detail="tenant_not_allowed")
    store = request.app.state.device_code_store
    ok = await store.redeem_user_code(body.user_code, tenant_id=body.tenant_id)
    if not ok:
        raise HTTPException(status_code=400, detail="invalid_or_expired_user_code")
    repo = request.app.state.dashboard_repo
    repo.log_action(
        user_id=user.user_id, tenant_id=body.tenant_id, action="activate_agent",
        metadata={"user_code": body.user_code},
    )
    return ActivateConfirmResponse(status="approved", expires_in_seconds=300)
