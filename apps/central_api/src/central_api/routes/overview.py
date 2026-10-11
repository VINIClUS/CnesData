"""Overview routes — /overview, /faturamento/by-establishment."""
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel

from central_api.deps import require_auth, require_tenant_header
from central_api.middleware import AuthenticatedUser

router = APIRouter(tags=["overview"])


class OverviewResponse(BaseModel):
    competencia_atual: int
    faturamento_atual_cents: int
    faturamento_anterior_cents: int
    aih_atual: int
    aih_anterior: int
    profissionais_ativos: int
    profissionais_anterior: int
    estabs_sem_producao: int
    estabs_total: int
    estabs_sem_producao_anterior: int


class FaturamentoResponse(BaseModel):
    series: list[dict[str, str | int]]
    categories: list[str]


@router.get("/overview", response_model=OverviewResponse)
def get_overview(
    response: Response,
    request: Request,
    user: AuthenticatedUser = Depends(require_auth),
    tenant_id: str = Depends(require_tenant_header),
) -> OverviewResponse:
    raise HTTPException(status_code=410, detail="legacy_route_retired")


@router.get(
    "/faturamento/by-establishment", response_model=FaturamentoResponse,
)
def get_faturamento_chart(
    response: Response,
    request: Request,
    user: AuthenticatedUser = Depends(require_auth),
    tenant_id: str = Depends(require_tenant_header),
    months: int = Query(12, ge=1, le=24),
) -> FaturamentoResponse:
    raise HTTPException(status_code=410, detail="legacy_route_retired")
