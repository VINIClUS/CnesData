"""Job Queue — fila com retry, DLQ e leases centralizados."""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID, insert
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_LEASE_MINUTES = 15

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
        "attempt_count", Integer, nullable=False,
        server_default=text("0"),
    ),
    Column(
        "max_retries", Integer, nullable=False,
        server_default=text("3"),
    ),
    Column(
        "error_history", JSONB, nullable=False,
        server_default=text("'[]'::jsonb"),
    ),
    Column("trace_context", JSONB),
    Column("machine_id", String(128)),
    Column("lease_expires_at", DateTime(timezone=True)),
    Column("heartbeat_at", DateTime(timezone=True)),
    Column(
        "created_at", DateTime(timezone=True),
        server_default=text("NOW()"),
    ),
    Column("started_at", DateTime(timezone=True)),
    Column("completed_at", DateTime(timezone=True)),
    Column("error_detail", Text),
    CheckConstraint(
        "status IN ("
        "'PENDING','ACQUIRED','STREAMING','PROCESSING',"
        "'COMPLETED','FAILED','DEAD_LETTER'"
        ")",
        name="chk_job_status",
    ),
    ForeignKeyConstraint(
        ["payload_id"], ["landing.raw_payload.id"],
    ),
    Index(
        "idx_jobs_pending", "status",
        postgresql_where=text("status = 'PENDING'"),
    ),
    Index(
        "idx_jobs_leased", "status", "lease_expires_at",
        postgresql_where=text(
            "status IN ('ACQUIRED','STREAMING')",
        ),
    ),
)

jobs_dlq = Table(
    "jobs_dlq",
    queue_metadata,
    Column(
        "id", UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column("original_job_id", UUID(as_uuid=True), nullable=False),
    Column(
        "status", String(20), nullable=False,
        server_default=text("'DEAD'"),
    ),
    Column("source_system", String(30), nullable=False),
    Column("tenant_id", String(6), nullable=False),
    Column("payload_id", UUID(as_uuid=True), nullable=False),
    Column("attempt_count", Integer, nullable=False),
    Column("max_retries", Integer, nullable=False),
    Column("last_error", Text),
    Column(
        "error_history", JSONB, nullable=False,
        server_default=text("'[]'::jsonb"),
    ),
    Column(
        "created_at", DateTime(timezone=True),
        server_default=text("NOW()"),
    ),
    Column(
        "died_at", DateTime(timezone=True),
        server_default=text("NOW()"),
    ),
    ForeignKeyConstraint(
        ["payload_id"], ["landing.raw_payload.id"],
    ),
    Index("idx_dlq_tenant", "tenant_id"),
    Index("idx_dlq_source", "source_system"),
)


@dataclass(frozen=True)
class Job:
    id: uuid.UUID
    status: str
    source_system: str
    tenant_id: str
    payload_id: uuid.UUID
    created_at: datetime | None = None
    attempt_count: int = 0
    max_retries: int = 3
    machine_id: str | None = None
    lease_expires_at: datetime | None = None
    heartbeat_at: datetime | None = None


def enqueue(
    engine: Engine,
    tenant_id: str,
    source_system: str,
    payload_id: uuid.UUID,
) -> uuid.UUID:
    """Insere novo job na fila com status PENDING."""
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
    logger.info(
        "job_enqueued job_id=%s source=%s tenant=%s",
        job_id, source_system, tenant_id,
    )
    return job_id


def acquire_for_agent(
    engine: Engine,
    machine_id: str,
    source_system: str | None = None,
) -> Job | None:
    """Concede lease de job PENDING ao agent identificado."""
    stmt = (
        select(jobs)
        .where(jobs.c.status == "PENDING")
        .order_by(jobs.c.created_at)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    if source_system:
        stmt = stmt.where(jobs.c.source_system == source_system)
    with engine.begin() as con:
        row = con.execute(stmt).first()
        if row is None:
            return None
        lease_sql = (
            f"NOW() + INTERVAL '{_LEASE_MINUTES} minutes'"
        )
        con.execute(
            update(jobs).where(jobs.c.id == row.id).values(
                status="ACQUIRED",
                machine_id=machine_id,
                lease_expires_at=text(lease_sql),
                heartbeat_at=text("NOW()"),
                started_at=text("NOW()"),
            )
        )
    logger.info(
        "job_acquired job_id=%s machine=%s", row.id, machine_id,
    )
    return Job(
        id=row.id,
        status="ACQUIRED",
        source_system=row.source_system,
        tenant_id=row.tenant_id,
        payload_id=row.payload_id,
        created_at=row.created_at,
        attempt_count=row.attempt_count,
        max_retries=row.max_retries,
        machine_id=machine_id,
    )


def renew_heartbeat(
    engine: Engine, job_id: uuid.UUID, machine_id: str,
) -> bool:
    """Renova lease do job se machine_id confere."""
    lease_sql = f"NOW() + INTERVAL '{_LEASE_MINUTES} minutes'"
    with engine.begin() as con:
        result = con.execute(
            update(jobs)
            .where(jobs.c.id == job_id)
            .where(jobs.c.machine_id == machine_id)
            .where(
                jobs.c.status.in_(["ACQUIRED", "STREAMING"]),
            )
            .values(
                heartbeat_at=text("NOW()"),
                lease_expires_at=text(lease_sql),
            )
        )
    renewed = result.rowcount > 0
    if renewed:
        logger.debug("heartbeat_renewed job_id=%s", job_id)
    return renewed


def transition_to_streaming(
    engine: Engine, job_id: uuid.UUID, machine_id: str,
) -> bool:
    """ACQUIRED → STREAMING quando agent inicia upload."""
    with engine.begin() as con:
        result = con.execute(
            update(jobs)
            .where(jobs.c.id == job_id)
            .where(jobs.c.machine_id == machine_id)
            .where(jobs.c.status == "ACQUIRED")
            .values(status="STREAMING")
        )
    return result.rowcount > 0


def complete_upload(
    engine: Engine,
    job_id: uuid.UUID,
    machine_id: str,
    object_key: str,
) -> bool:
    """STREAMING → COMPLETED; grava object_key no landing."""
    from cnes_infra.storage.landing import raw_payload

    with engine.begin() as con:
        row = con.execute(
            select(jobs.c.payload_id, jobs.c.status, jobs.c.machine_id)
            .where(jobs.c.id == job_id)
        ).first()
        if row is None:
            return False
        if row.machine_id != machine_id:
            return False
        if row.status not in ("ACQUIRED", "STREAMING"):
            return False
        con.execute(
            update(raw_payload)
            .where(raw_payload.c.id == row.payload_id)
            .values(object_key=object_key)
        )
        con.execute(
            update(jobs).where(jobs.c.id == job_id).values(
                status="COMPLETED",
                completed_at=text("NOW()"),
            )
        )
    logger.info(
        "upload_completed job_id=%s key=%s", job_id, object_key,
    )
    return True


def reap_expired_leases(engine: Engine) -> int:
    """Recupera jobs com lease expirado — reseta ou move para DLQ."""
    reaped = 0
    with engine.begin() as con:
        rows = con.execute(
            select(jobs)
            .where(
                jobs.c.status.in_(["ACQUIRED", "STREAMING"]),
            )
            .where(jobs.c.lease_expires_at < text("NOW()"))
            .with_for_update(skip_locked=True)
        ).fetchall()

        for row in rows:
            new_count = row.attempt_count + 1
            now_iso = datetime.now(timezone.utc).isoformat()
            entry = {
                "attempt": new_count,
                "error": "lease_expired",
                "at": now_iso,
            }
            history = (row.error_history or []) + [entry]
            if new_count >= row.max_retries:
                _move_to_dlq(con, row, new_count, history)
            else:
                _reset_to_pending(con, row.id, new_count, history)
            reaped += 1
    if reaped:
        logger.info("leases_reaped count=%d", reaped)
    return reaped


def _move_to_dlq(con, row, count: int, history: list) -> None:
    con.execute(insert(jobs_dlq).values(
        original_job_id=row.id,
        source_system=row.source_system,
        tenant_id=row.tenant_id,
        payload_id=row.payload_id,
        attempt_count=count,
        max_retries=row.max_retries,
        last_error="lease_expired",
        error_history=history,
    ))
    con.execute(
        update(jobs).where(jobs.c.id == row.id).values(
            status="DEAD_LETTER",
            attempt_count=count,
            error_history=history,
            error_detail="lease_expired",
            completed_at=text("NOW()"),
            machine_id=None,
        )
    )


def _reset_to_pending(
    con, job_id: uuid.UUID, count: int, history: list,
) -> None:
    con.execute(
        update(jobs).where(jobs.c.id == job_id).values(
            status="PENDING",
            attempt_count=count,
            error_history=history,
            error_detail="lease_expired",
            machine_id=None,
            lease_expires_at=None,
            heartbeat_at=None,
            started_at=None,
        )
    )


def claim_next(engine: Engine) -> Job | None:
    """Reivindica o próximo job PENDING (legacy worker)."""
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
        attempt_count=row.attempt_count,
        max_retries=row.max_retries,
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


def fail(engine: Engine, job_id: uuid.UUID, error: str) -> bool:
    """Incrementa attempt_count; move para DLQ se excedeu max_retries."""
    with engine.begin() as con:
        row = con.execute(
            select(jobs).where(jobs.c.id == job_id).with_for_update()
        ).one()

        new_count = row.attempt_count + 1
        now_iso = datetime.now(timezone.utc).isoformat()
        entry = {
            "attempt": new_count, "error": error[:500],
            "at": now_iso,
        }
        history = (row.error_history or []) + [entry]

        if new_count >= row.max_retries:
            _move_to_dlq(con, row, new_count, history)
            con.execute(
                update(jobs).where(jobs.c.id == job_id).values(
                    status="DEAD_LETTER",
                    attempt_count=new_count,
                    error_history=history,
                    error_detail=error[:2000],
                    completed_at=text("NOW()"),
                )
            )
            logger.warning(
                "job_dlq job_id=%s attempts=%d",
                job_id, new_count,
            )
            return True

        con.execute(
            update(jobs).where(jobs.c.id == job_id).values(
                status="PENDING",
                attempt_count=new_count,
                error_history=history,
                error_detail=error[:2000],
                started_at=None,
            )
        )
        logger.warning(
            "job_retry job_id=%s attempt=%d/%d",
            job_id, new_count, row.max_retries,
        )
        return False


def get_status(engine: Engine, job_id: uuid.UUID) -> dict | None:
    """Consulta status de um job."""
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
        "created_at": _iso(row.created_at),
        "started_at": _iso(row.started_at),
        "completed_at": _iso(row.completed_at),
        "error_detail": row.error_detail,
        "attempt_count": row.attempt_count,
        "machine_id": row.machine_id,
    }


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None
