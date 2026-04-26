"""Dashboard routes: /auth/me, /tenants. Mounted at /api/v1/dashboard in app.py."""
from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field

from central_api.deps import require_auth, require_tenant_header
from central_api.middleware import AuthenticatedUser
from cnes_infra import config

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


class SourceStatusOut(BaseModel):
    fonte_sistema: str
    last_extracao_ts: datetime | None
    last_competencia: int | None
    lag_months: int | None
    row_count: int | None
    status: str
    last_machine_id: str | None


class AgentStatusResponse(BaseModel):
    fetched_at: datetime
    sources: list[SourceStatusOut]


class RunOut(BaseModel):
    id: str
    extracao_ts: datetime
    fonte_sistema: str
    competencia: int
    row_count: int
    sha256: str
    machine_id: str | None


class AgentRunsResponse(BaseModel):
    runs: list[RunOut]


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


@router.get("/agents/status", response_model=AgentStatusResponse)
def agents_status(
    response: Response,
    request: Request,
    user: AuthenticatedUser = Depends(require_auth),
    tenant_id: str = Depends(require_tenant_header),
) -> AgentStatusResponse:
    response.headers["Cache-Control"] = "private, max-age=10"
    repo = request.app.state.dashboard_repo
    target = config.COMPETENCIA_ANO * 100 + config.COMPETENCIA_MES
    sources = repo.agent_status(tenant_id=tenant_id, current_competencia=target)
    repo.log_action(
        user_id=user.user_id, tenant_id=tenant_id,
        action="view_status", metadata=None,
    )
    return AgentStatusResponse(
        fetched_at=datetime.now(UTC),
        sources=[SourceStatusOut(**s.__dict__) for s in sources],
    )


@router.get("/agents/runs", response_model=AgentRunsResponse)
def agents_runs(
    response: Response,
    request: Request,
    user: AuthenticatedUser = Depends(require_auth),
    tenant_id: str = Depends(require_tenant_header),
    limit: int = 20,
) -> AgentRunsResponse:
    response.headers["Cache-Control"] = "private, max-age=10"
    if limit < 1 or limit > 100:
        raise HTTPException(status_code=400, detail="limit_out_of_range")
    repo = request.app.state.dashboard_repo
    rows = repo.recent_runs(tenant_id=tenant_id, limit=limit)
    repo.log_action(
        user_id=user.user_id, tenant_id=tenant_id,
        action="view_runs", metadata=None,
    )
    return AgentRunsResponse(
        runs=[
            RunOut(
                id=str(r.id),
                extracao_ts=r.extracao_ts,
                fonte_sistema=r.fonte_sistema,
                competencia=r.competencia,
                row_count=r.row_count,
                sha256=r.sha256,
                machine_id=r.machine_id,
            )
            for r in rows
        ],
    )
