"""Dashboard routes: /auth/me, /tenants. Mounted at /api/v1/dashboard in app.py."""
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field

from central_api.deps import require_auth
from central_api.middleware import AuthenticatedUser

router = APIRouter(tags=["dashboard"])


class MeResponse(BaseModel):
    user_id: UUID
    email: str
    display_name: str | None
    role: str
    tenant_ids: list[str]


class TenantResponse(BaseModel):
    ibge6: str = Field(min_length=6, max_length=6)
    ibge7: str = Field(min_length=7, max_length=7)
    nome: str
    uf: str = Field(min_length=2, max_length=2)


@router.get("/auth/me", response_model=MeResponse)
def auth_me(
    request: Request,
    user: AuthenticatedUser = Depends(require_auth),
) -> MeResponse:
    repo = request.app.state.dashboard_repo
    repo.log_action(
        user_id=user.user_id, tenant_id=None, action="login", metadata=None,
    )
    return MeResponse(
        user_id=user.user_id, email=user.email,
        display_name=user.display_name, role=user.role,
        tenant_ids=user.tenant_ids,
    )


@router.get("/tenants", response_model=list[TenantResponse])
def list_tenants(
    request: Request,
    user: AuthenticatedUser = Depends(require_auth),
) -> list[TenantResponse]:
    repo = request.app.state.dashboard_repo
    rows = repo.list_tenants(user_id=user.user_id)
    repo.log_action(
        user_id=user.user_id, tenant_id=None, action="view_tenants", metadata=None,
    )
    return [
        TenantResponse(ibge6=r.ibge6, ibge7=r.ibge7, nome=r.nome, uf=r.uf)
        for r in rows
    ]
