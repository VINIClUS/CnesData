"""landing.extractions repository — orchestration + artifact metadata (Gold v2)."""
from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import text

from cnes_contracts.jobs import JobStatus
from cnes_contracts.landing import Extraction, ExtractionRegisterPayload

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


def register(
    conn: Connection,
    payload: ExtractionRegisterPayload,
    bucket: str,
) -> tuple[UUID, str]:
    """Insert PENDING row + return (extraction_id, object_key)."""
    extraction_id = uuid4()
    object_key = (
        f"{payload.tenant_id}/{payload.fonte_sistema}/"
        f"{payload.competencia}/{extraction_id}.parquet.gz"
    )
    conn.execute(
        text("""
            INSERT INTO landing.extractions (
                id, job_id, tenant_id, fonte_sistema, tipo_extracao,
                competencia, object_key, agent_version, machine_id, status
            )
            VALUES (:id, :job_id, :tenant_id, :fonte, :tipo, :comp, :obj, :av, :mid, 'PENDING')
        """),
        {
            "id": extraction_id,
            "job_id": payload.job_id,
            "tenant_id": payload.tenant_id,
            "fonte": payload.fonte_sistema,
            "tipo": payload.tipo_extracao,
            "comp": payload.competencia,
            "obj": object_key,
            "av": payload.agent_version,
            "mid": payload.machine_id,
        },
    )
    return extraction_id, object_key


def mark_uploaded(
    conn: Connection, extraction_id: UUID, sha256: str, row_count: int,
) -> None:
    conn.execute(
        text("""
            UPDATE landing.extractions SET
                status = 'UPLOADED',
                sha256 = :sha,
                row_count = :rc,
                uploaded_at = NOW()
            WHERE id = :i
        """),
        {"i": extraction_id, "sha": sha256, "rc": row_count},
    )


def claim_next(
    conn: Connection, processor_id: str, lease_secs: int,
) -> Extraction | None:
    row = conn.execute(
        text("""
            WITH claimed AS (
                SELECT id FROM landing.extractions
                WHERE status = 'UPLOADED'
                ORDER BY uploaded_at ASC NULLS LAST
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE landing.extractions e SET
                status = 'PROCESSING',
                lease_owner = :pid,
                lease_until = NOW() + (:secs || ' seconds')::INTERVAL,
                started_at = NOW()
            FROM claimed
            WHERE e.id = claimed.id
            RETURNING e.*
        """),
        {"pid": processor_id, "secs": str(lease_secs)},
    ).first()
    if row is None:
        return None
    payload = dict(row._mapping)  # noqa: SLF001 - sqlalchemy Row._mapping is the public dict view
    payload["status"] = JobStatus(payload["status"])
    return Extraction.model_validate(payload)


def complete(conn: Connection, extraction_id: UUID) -> None:
    conn.execute(
        text("""
            UPDATE landing.extractions SET
                status = 'INGESTED',
                completed_at = NOW()
            WHERE id = :i
        """),
        {"i": extraction_id},
    )


def fail(
    conn: Connection, extraction_id: UUID, error: str, max_retries: int = 3,
) -> None:
    conn.execute(
        text("""
            UPDATE landing.extractions SET
                status = CASE WHEN retry_count + 1 >= :max THEN 'FAILED' ELSE 'UPLOADED' END,
                retry_count = retry_count + 1,
                lease_owner = NULL,
                lease_until = NULL,
                error_detail = :err
            WHERE id = :i
        """),
        {"i": extraction_id, "err": error, "max": max_retries},
    )


def heartbeat(
    conn: Connection, extraction_id: UUID, processor_id: str, lease_secs: int = 300,
) -> None:
    conn.execute(
        text("""
            UPDATE landing.extractions SET
                lease_until = NOW() + (:secs || ' seconds')::INTERVAL
            WHERE id = :i
              AND lease_owner = :pid
              AND status = 'PROCESSING'
        """),
        {"i": extraction_id, "pid": processor_id, "secs": str(lease_secs)},
    )


def reap_expired(conn: Connection) -> int:
    result = conn.execute(
        text("""
            UPDATE landing.extractions SET
                status = 'UPLOADED',
                lease_owner = NULL,
                lease_until = NULL
            WHERE status = 'PROCESSING'
              AND lease_until < NOW()
        """),
    )
    return result.rowcount
