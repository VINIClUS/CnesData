"""OAuth routes — /activate/confirm (Bearer JWT gated, rate limited)."""
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.util import get_remote_address

from central_api.deps import require_auth
from central_api.middleware import AuthenticatedUser

router = APIRouter(tags=["oauth"])

limiter = Limiter(key_func=get_remote_address)


class ActivateConfirmRequest(BaseModel):
    user_code: str = Field(min_length=8, max_length=10, pattern=r"^[A-Z0-9-]{8,10}$")
    tenant_id: str = Field(min_length=6, max_length=6)


class ActivateConfirmResponse(BaseModel):
    status: str
    expires_in_seconds: int


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
