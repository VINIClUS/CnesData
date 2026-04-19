"""SQLAlchemy Table definitions for the job queue schema."""

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
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

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
        "'COMPLETED','DONE','FAILED','DEAD_LETTER'"
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
    Index(
        "idx_jobs_completed", "status", "completed_at",
        postgresql_where=text("status = 'COMPLETED'"),
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
