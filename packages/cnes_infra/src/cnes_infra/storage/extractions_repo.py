"""extractions_repo — N-file manifest enqueue/claim for landing.extractions."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from sqlalchemy import text

if TYPE_CHECKING:
    from datetime import date

    from sqlalchemy.engine import Engine


@dataclass(frozen=True, slots=True)
class ClaimedExtraction:
    job_id: UUID
    tenant_id: str
    source_type: str
    competencia: date
    files: list[dict]
    depends_on: list[UUID]


def enqueue(
    engine: Engine,
    *,
    tenant_id: str,
    source_type: str,
    competencia: date,
    files: list[dict[str, Any]],
    depends_on: list[UUID] | None = None,
) -> UUID:
    job_id = uuid4()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO landing.extractions (
                    job_id, tenant_id, source_type, competencia,
                    files, depends_on, status, created_at
                ) VALUES (
                    :j, :t, :s, :c, CAST(:f AS jsonb),
                    CAST(:d AS uuid[]), 'PENDING', NOW()
                )
            """),
            {
                "j": str(job_id),
                "t": tenant_id,
                "s": source_type,
                "c": competencia,
                "f": json.dumps(files),
                "d": _uuid_array([str(u) for u in (depends_on or [])]),
            },
        )
    return job_id


def _uuid_array(ids: list[str]) -> str:
    if not ids:
        return "{}"
    return "{" + ",".join(ids) + "}"


def claim_next(
    engine: Engine,
    *,
    tenant_id: str,
    lease_seconds: int = 300,
) -> ClaimedExtraction | None:
    sql = text("""
        UPDATE landing.extractions
        SET status = 'CLAIMED',
            lease_until = NOW() + make_interval(secs => :lease)
        WHERE job_id = (
            SELECT e.job_id FROM landing.extractions e
            WHERE e.tenant_id = :t
              AND e.status = 'PENDING'
              AND NOT EXISTS (
                  SELECT 1 FROM unnest(e.depends_on) AS d(dep)
                  JOIN landing.extractions pe ON pe.job_id = d.dep
                  WHERE pe.status <> 'COMPLETED'
              )
            ORDER BY e.created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING job_id, tenant_id, source_type, competencia,
                  files, depends_on
    """)
    with engine.begin() as conn:
        row = conn.execute(
            sql, {"t": tenant_id, "lease": lease_seconds},
        ).one_or_none()
    if row is None:
        return None
    return ClaimedExtraction(
        job_id=row.job_id,
        tenant_id=row.tenant_id,
        source_type=row.source_type,
        competencia=row.competencia,
        files=list(row.files) if row.files else [],
        depends_on=list(row.depends_on or []),
    )


def mark_completed(engine: Engine, *, job_id: UUID) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE landing.extractions
                SET status = 'COMPLETED', registered_at = NOW()
                WHERE job_id = :j
            """),
            {"j": str(job_id)},
        )


def mark_failed(engine: Engine, *, job_id: UUID, reason: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE landing.extractions
                SET status = 'FAILED', lease_until = NULL
                WHERE job_id = :j
            """),
            {"j": str(job_id)},
        )


def register(*args: object, **kwargs: object) -> None:
    raise NotImplementedError(
        "extractions_repo.register: pending Task 7 /jobs/register route",
    )


def mark_uploaded(*args: object, **kwargs: object) -> None:
    raise NotImplementedError(
        "extractions_repo.mark_uploaded: pending Task 7",
    )


def complete(*args: object, **kwargs: object) -> None:
    raise NotImplementedError("extractions_repo.complete: pending Task 7")


def fail(*args: object, **kwargs: object) -> None:
    raise NotImplementedError("extractions_repo.fail: pending Task 7")


def heartbeat(*args: object, **kwargs: object) -> None:
    raise NotImplementedError("extractions_repo.heartbeat: pending Task 7")


def reap_expired(*args: object, **kwargs: object) -> None:
    raise NotImplementedError("extractions_repo.reap_expired: pending Task 7")
