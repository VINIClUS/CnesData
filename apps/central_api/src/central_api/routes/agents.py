"""Rotas de status agregado e identidade mTLS dos agents."""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.engine import Engine

from central_api.agent_auth import AgentCertIdentity, require_agent_cert
from central_api.deps import get_engine, require_tenant_header
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
    x_tenant_id: str = Depends(require_tenant_header),
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


class AgentWhoamiResponse(BaseModel):
    tenant_id: str
    agent_id: str
    machine_id: str


@router.get("/agents/whoami", response_model=AgentWhoamiResponse)
def get_agent_whoami(
    identity: AgentCertIdentity = Depends(require_agent_cert),
) -> AgentWhoamiResponse:
    """Retorna a identidade do cert mTLS; usado pelo smoke do `register`."""
    return AgentWhoamiResponse(
        tenant_id=identity.tenant_id,
        agent_id=identity.agent_id,
        machine_id=identity.machine_id,
    )
