"""Access requests routes — mounted at /api/v1/dashboard/access-requests."""
from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError

from central_api.deps import require_auth
from central_api.middleware import AuthenticatedUser

router = APIRouter(tags=["access-requests"])


class AccessRequestOut(BaseModel):
    id: UUID
    tenant_id: str
    tenant_nome: str | None
    motivation: str
    status: str
    requested_at: datetime
    reviewed_at: datetime | None
    review_notes: str | None


class TenantOut(BaseModel):
    ibge6: str = Field(min_length=6, max_length=6)
    ibge7: str = Field(min_length=7, max_length=7)
    nome: str
    uf: str = Field(min_length=2, max_length=2)


class AccessRequestCreate(BaseModel):
    tenant_id: str = Field(min_length=6, max_length=6)
    motivation: str = Field(min_length=1, max_length=500)


class AccessRequestCreated(BaseModel):
    request_id: UUID


@router.get("/mine", response_model=list[AccessRequestOut])
def list_mine(
    request: Request,
    user: AuthenticatedUser = Depends(require_auth),
) -> list[AccessRequestOut]:
    repo = request.app.state.dashboard_repo
    rows = repo.list_access_requests(user_id=user.user_id)
    return [
        AccessRequestOut(
            id=r.id, tenant_id=r.tenant_id, tenant_nome=r.tenant_nome,
            motivation=r.motivation, status=r.status,
            requested_at=r.requested_at, reviewed_at=r.reviewed_at,
            review_notes=r.review_notes,
        )
        for r in rows
    ]


@router.post("", response_model=AccessRequestCreated, status_code=201)
def create_request(
    body: AccessRequestCreate,
    request: Request,
    user: AuthenticatedUser = Depends(require_auth),
) -> AccessRequestCreated:
    repo = request.app.state.dashboard_repo
    try:
        req_id = repo.submit_access_request(
            user_id=user.user_id, tenant_id=body.tenant_id,
            motivation=body.motivation,
        )
    except IntegrityError as e:
        raise HTTPException(status_code=409, detail="duplicate_request") from e
    repo.log_action(
        user_id=user.user_id, tenant_id=body.tenant_id,
        action="request_access",
        metadata={"request_id": str(req_id)},
    )
    return AccessRequestCreated(request_id=req_id)


@router.get("/available-tenants", response_model=list[TenantOut])
def list_available_tenants(
    request: Request,
    user: AuthenticatedUser = Depends(require_auth),
) -> list[TenantOut]:
    repo = request.app.state.dashboard_repo
    rows = repo.list_available_tenants_for_user(user_id=user.user_id)
    return [
        TenantOut(ibge6=r.ibge6, ibge7=r.ibge7, nome=r.nome, uf=r.uf)
        for r in rows
    ]
