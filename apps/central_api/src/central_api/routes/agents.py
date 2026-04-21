"""Rota de status agregado de agents por tenant."""

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.engine import Engine

from central_api.deps import get_engine
from central_api.repositories.agent_status_repo import query_agent_status

router = APIRouter(tags=["agents"])


class AgentStatusResponse(BaseModel):
    tenant_id: str
    last_seen: str | None
    agent_version: str | None
    machine_id: str | None
    jobs_completed_7d: int
    jobs_failed_7d: int


@router.get("/agents/status", response_model=AgentStatusResponse)
def get_agent_status(
    tenant_id: str = Query(..., pattern=r"^\d{6}$"),
    x_tenant_id: str = Header(..., alias="X-Tenant-Id"),
    engine: Engine = Depends(get_engine),
) -> AgentStatusResponse:
    """Retorna status agregado do agent do tenant."""
    if tenant_id != x_tenant_id:
        raise HTTPException(status_code=403, detail="tenant_mismatch")
    status = query_agent_status(engine, tenant_id=tenant_id)
    return AgentStatusResponse(
        tenant_id=status.tenant_id,
        last_seen=status.last_seen.isoformat() if status.last_seen else None,
        agent_version=status.agent_version,
        machine_id=status.machine_id,
        jobs_completed_7d=status.jobs_completed_7d,
        jobs_failed_7d=status.jobs_failed_7d,
    )
