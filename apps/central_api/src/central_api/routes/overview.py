"""Overview routes — /overview, /faturamento/by-establishment."""
from fastapi import APIRouter, Depends, Query, Request, Response
from pydantic import BaseModel

from central_api.deps import require_auth, require_tenant_header
from central_api.middleware import AuthenticatedUser
from cnes_infra import config

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
    response.headers["Cache-Control"] = "private, max-age=30"
    repo = request.app.state.dashboard_repo
    target = config.COMPETENCIA_ANO * 100 + config.COMPETENCIA_MES
    kpis = repo.overview_kpis(tenant_id=tenant_id, current_competencia=target)
    repo.log_action(
        user_id=user.user_id, tenant_id=tenant_id,
        action="view_overview", metadata=None,
    )
    return OverviewResponse(**kpis.__dict__)


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
    response.headers["Cache-Control"] = "private, max-age=60"
    repo = request.app.state.dashboard_repo
    target = config.COMPETENCIA_ANO * 100 + config.COMPETENCIA_MES
    chart = repo.faturamento_by_establishment(
        tenant_id=tenant_id, months=months, current_competencia=target,
    )
    repo.log_action(
        user_id=user.user_id, tenant_id=tenant_id,
        action="view_faturamento", metadata={"months": months},
    )
    return FaturamentoResponse(
        series=chart.series, categories=chart.categories,
    )
