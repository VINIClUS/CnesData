"""Job Queue — fila de processamento baseada em PostgreSQL (FOR UPDATE SKIP LOCKED)."""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Index,
    MetaData,
    String,
    Table,
    Text,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import UUID, insert
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

queue_metadata = MetaData(schema="queue")

jobs = Table(
    "jobs",
    queue_metadata,
    Column(
        "id", UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column(
        "status", String(20), nullable=False,
        server_default=text("'PENDING'"),
    ),
    Column("source_system", String(30), nullable=False),
    Column("tenant_id", String(6), nullable=False),
    Column("payload_id", UUID(as_uuid=True), nullable=False),
    Column(
        "created_at", DateTime(timezone=True),
        server_default=text("NOW()"),
    ),
    Column("started_at", DateTime(timezone=True)),
    Column("completed_at", DateTime(timezone=True)),
    Column("error_detail", Text),
    CheckConstraint(
        "status IN ('PENDING','PROCESSING','COMPLETED','FAILED')",
        name="chk_job_status",
    ),
    ForeignKeyConstraint(
        ["payload_id"], ["landing.raw_payload.id"],
    ),
    Index("idx_jobs_pending", "status", postgresql_where=text("status = 'PENDING'")),
)


@dataclass(frozen=True)
class Job:
    id: uuid.UUID
    status: str
    source_system: str
    tenant_id: str
    payload_id: uuid.UUID
    created_at: datetime | None = None


def enqueue(
    engine: Engine,
    tenant_id: str,
    source_system: str,
    payload_id: uuid.UUID,
) -> uuid.UUID:
    """Insere novo job na fila com status PENDING.

    Returns:
        UUID do job criado.
    """
    job_id = uuid.uuid4()
    with engine.begin() as con:
        con.execute(
            insert(jobs).values(
                id=job_id,
                tenant_id=tenant_id,
                source_system=source_system,
                payload_id=payload_id,
            )
        )
    logger.info("job_enqueued job_id=%s source=%s tenant=%s", job_id, source_system, tenant_id)
    return job_id


def claim_next(engine: Engine) -> Job | None:
    """Reivindica o próximo job PENDING (FOR UPDATE SKIP LOCKED).

    Returns:
        Job reivindicado ou None se fila vazia.
    """
    with engine.begin() as con:
        row = con.execute(
            select(jobs)
            .where(jobs.c.status == "PENDING")
            .order_by(jobs.c.created_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        ).first()
        if row is None:
            return None
        con.execute(
            update(jobs)
            .where(jobs.c.id == row.id)
            .values(status="PROCESSING", started_at=text("NOW()"))
        )
    return Job(
        id=row.id,
        status="PROCESSING",
        source_system=row.source_system,
        tenant_id=row.tenant_id,
        payload_id=row.payload_id,
        created_at=row.created_at,
    )


def complete(engine: Engine, job_id: uuid.UUID) -> None:
    """Marca job como COMPLETED."""
    with engine.begin() as con:
        con.execute(
            update(jobs)
            .where(jobs.c.id == job_id)
            .values(status="COMPLETED", completed_at=text("NOW()"))
        )
    logger.info("job_completed job_id=%s", job_id)


def fail(engine: Engine, job_id: uuid.UUID, error: str) -> None:
    """Marca job como FAILED com detalhes do erro."""
    with engine.begin() as con:
        con.execute(
            update(jobs)
            .where(jobs.c.id == job_id)
            .values(
                status="FAILED",
                completed_at=text("NOW()"),
                error_detail=error,
            )
        )
    logger.error("job_failed job_id=%s error=%s", job_id, error)


def get_status(engine: Engine, job_id: uuid.UUID) -> dict | None:
    """Consulta status de um job.

    Returns:
        Dict com campos do job ou None se não encontrado.
    """
    with engine.connect() as con:
        row = con.execute(
            select(jobs).where(jobs.c.id == job_id)
        ).first()
    if row is None:
        return None
    return {
        "job_id": str(row.id),
        "status": row.status,
        "source_system": row.source_system,
        "tenant_id": row.tenant_id,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
        "error_detail": row.error_detail,
    }
