"""Batch trigger — flag de controle para drenagem do data_processor."""

import logging
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    MetaData,
    String,
    Table,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_batch_trigger_metadata = MetaData(schema="queue")

batch_trigger = Table(
    "batch_trigger",
    _batch_trigger_metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("tenant_id", String(6)),
    Column("status", String(10), nullable=False, server_default=text("'CLOSED'")),
    Column("opened_at", DateTime(timezone=True)),
    Column("closed_at", DateTime(timezone=True)),
    Column("reason", String(40)),
    Column("pending_bytes", BigInteger),
    Column("oldest_completed_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True), server_default=text("NOW()")),
    CheckConstraint("status IN ('OPEN','CLOSED')", name="chk_batch_trigger_status"),
)

_SQL_METRICS = (
    "SELECT "
    "  COALESCE(SUM(r.size_bytes), 0) AS pending_bytes, "
    "  MIN(j.completed_at) AS oldest_completed_at "
    "FROM queue.jobs j "
    "JOIN landing.raw_payload r ON r.id = j.payload_id "
    "WHERE j.status = 'COMPLETED'"
)

_SQL_EVALUATE = (
    "UPDATE queue.batch_trigger SET "
    "  pending_bytes = :pb, "
    "  oldest_completed_at = :oc, "
    "  updated_at = NOW(), "
    "  status = CASE "
    "    WHEN :pb >= :st OR (:oc IS NOT NULL AND :oc < NOW() - (:ad || ' days')::INTERVAL) "
    "    THEN 'OPEN' ELSE status END, "
    "  opened_at = CASE "
    "    WHEN status = 'CLOSED' AND ("
    "      :pb >= :st OR (:oc IS NOT NULL AND :oc < NOW() - (:ad || ' days')::INTERVAL)"
    "    ) THEN NOW() ELSE opened_at END, "
    "  reason = CASE "
    "    WHEN status = 'CLOSED' AND :pb >= :st THEN 'size_threshold' "
    "    WHEN status = 'CLOSED' AND :oc IS NOT NULL "
    "         AND :oc < NOW() - (:ad || ' days')::INTERVAL THEN 'age_threshold' "
    "    ELSE reason END "
    "WHERE tenant_id IS NULL"
)

_SQL_CLOSE_IF_DRAINED_GLOBAL = (
    "UPDATE queue.batch_trigger SET "
    "  status = 'CLOSED', closed_at = NOW(), reason = 'queue_empty', "
    "  pending_bytes = 0, updated_at = NOW() "
    "WHERE tenant_id IS NULL AND status = 'OPEN' "
    "  AND NOT EXISTS (SELECT 1 FROM queue.jobs WHERE status = 'COMPLETED')"
)

_SQL_CLOSE_IF_DRAINED_TENANT = (
    "UPDATE queue.batch_trigger SET "
    "  status = 'CLOSED', closed_at = NOW(), reason = 'queue_empty', "
    "  pending_bytes = 0, updated_at = NOW() "
    "WHERE tenant_id = :tenant_id AND status = 'OPEN' "
    "  AND NOT EXISTS ("
    "    SELECT 1 FROM queue.jobs "
    "    WHERE status = 'COMPLETED' AND tenant_id = :tenant_id"
    "  )"
)


@dataclass(frozen=True)
class TriggerState:
    status: str
    opened_at: datetime | None
    pending_bytes: int | None
    oldest_completed_at: datetime | None
    reason: str | None


@dataclass(frozen=True)
class Thresholds:
    size_bytes: int
    age_days: int


def read_state(engine: Engine, tenant_id: str | None = None) -> TriggerState | None:
    """Lê row atual do flag. tenant_id=None → global."""
    stmt = select(batch_trigger).where(
        batch_trigger.c.tenant_id.is_(None)
        if tenant_id is None
        else batch_trigger.c.tenant_id == tenant_id
    )
    with engine.connect() as con:
        row = con.execute(stmt).first()
    if row is None:
        return None
    return TriggerState(
        status=row.status,
        opened_at=row.opened_at,
        pending_bytes=row.pending_bytes,
        oldest_completed_at=row.oldest_completed_at,
        reason=row.reason,
    )


def evaluate_and_open(engine: Engine, thresholds: Thresholds) -> TriggerState:
    """Avalia pending bytes + oldest age, UPDATE flag.

    Sempre atualiza pending_bytes/oldest_completed_at/updated_at.
    Transiciona CLOSED→OPEN se size OR age threshold batem.
    Prioridade de reason: size > age em tie.
    """
    with engine.begin() as con:
        metrics = con.execute(text(_SQL_METRICS)).first()
        con.execute(
            text(_SQL_EVALUATE),
            {"pb": metrics.pending_bytes, "oc": metrics.oldest_completed_at,
             "st": thresholds.size_bytes, "ad": thresholds.age_days},
        )
    state = read_state(engine)
    logger.info(
        "trigger_evaluated status=%s pending_bytes=%s reason=%s",
        state.status if state else "none",
        metrics.pending_bytes,
        state.reason if state else "none",
    )
    return state  # type: ignore[return-value]


def close_if_drained(engine: Engine, tenant_id: str | None = None) -> bool:
    """Fecha flag se não há jobs COMPLETED. Retorna True se fechou."""
    with engine.begin() as con:
        if tenant_id is None:
            result = con.execute(text(_SQL_CLOSE_IF_DRAINED_GLOBAL))
        else:
            result = con.execute(
                text(_SQL_CLOSE_IF_DRAINED_TENANT),
                {"tenant_id": tenant_id},
            )
    fechou = result.rowcount > 0
    if fechou:
        logger.info("trigger_closed reason=queue_empty tenant_id=%s", tenant_id)
    return fechou
