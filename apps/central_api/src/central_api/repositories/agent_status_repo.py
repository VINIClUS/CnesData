"""Repositório de status agregado de agents por tenant."""

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

from cnes_domain.tenant import set_tenant_id


@dataclass
class AgentStatus:
    tenant_id: str
    last_seen: datetime | None
    agent_version: str | None
    machine_id: str | None
    jobs_completed_7d: int
    jobs_failed_7d: int


_SQL = text(
    """
    SELECT
        MAX(e.registered_at)                                         AS last_seen,
        MAX(e.agent_version)                                         AS agent_version,
        MAX(e.machine_id)                                            AS machine_id,
        SUM(CASE WHEN e.registered_at IS NOT NULL THEN 1 ELSE 0 END) AS completed,
        SUM(CASE WHEN e.status IN ('FAILED','DLQ')
                  AND e.registered_at IS NULL
                 THEN 1 ELSE 0 END)                                  AS failed
    FROM landing.extractions e
    WHERE e.tenant_id = :tenant
      AND e.created_at >= NOW() - INTERVAL '7 days'
    """
)


def query_agent_status(engine: Engine, *, tenant_id: str) -> AgentStatus:
    """Agrega métricas recentes para o tenant."""
    set_tenant_id(tenant_id)
    with engine.connect() as conn:
        row = conn.execute(_SQL, {"tenant": tenant_id}).mappings().one_or_none()
    if row is None:
        return AgentStatus(
            tenant_id=tenant_id,
            last_seen=None,
            agent_version=None,
            machine_id=None,
            jobs_completed_7d=0,
            jobs_failed_7d=0,
        )
    return AgentStatus(
        tenant_id=tenant_id,
        last_seen=row["last_seen"],
        agent_version=row["agent_version"],
        machine_id=row["machine_id"],
        jobs_completed_7d=int(row["completed"] or 0),
        jobs_failed_7d=int(row["failed"] or 0),
    )
