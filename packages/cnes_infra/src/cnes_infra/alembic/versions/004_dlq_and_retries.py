"""DLQ, retry columns, and trace context.

Revision ID: 004
Revises: 003
Create Date: 2026-04-13
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column(
            "attempt_count", sa.Integer,
            server_default=sa.text("0"), nullable=False,
        ),
        schema="queue",
    )
    op.add_column(
        "jobs",
        sa.Column(
            "max_retries", sa.Integer,
            server_default=sa.text("3"), nullable=False,
        ),
        schema="queue",
    )
    op.add_column(
        "jobs",
        sa.Column(
            "error_history", postgresql.JSONB,
            server_default=sa.text("'[]'::jsonb"), nullable=False,
        ),
        schema="queue",
    )
    op.add_column(
        "jobs",
        sa.Column("trace_context", postgresql.JSONB),
        schema="queue",
    )

    op.drop_constraint("chk_job_status", "jobs", schema="queue")
    op.create_check_constraint(
        "chk_job_status", "jobs",
        "status IN ("
        "'PENDING','PROCESSING','COMPLETED','FAILED','DEAD_LETTER'"
        ")",
        schema="queue",
    )

    op.create_table(
        "jobs_dlq",
        sa.Column(
            "id", postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            primary_key=True,
        ),
        sa.Column(
            "original_job_id",
            postgresql.UUID(as_uuid=True), nullable=False,
        ),
        sa.Column(
            "status", sa.String(20), nullable=False,
            server_default=sa.text("'DEAD'"),
        ),
        sa.Column("source_system", sa.String(30), nullable=False),
        sa.Column("tenant_id", sa.String(6), nullable=False),
        sa.Column(
            "payload_id",
            postgresql.UUID(as_uuid=True), nullable=False,
        ),
        sa.Column("attempt_count", sa.Integer, nullable=False),
        sa.Column("max_retries", sa.Integer, nullable=False),
        sa.Column("last_error", sa.Text),
        sa.Column(
            "error_history", postgresql.JSONB,
            server_default=sa.text("'[]'::jsonb"), nullable=False,
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "died_at", sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
        ),
        sa.ForeignKeyConstraint(
            ["payload_id"], ["landing.raw_payload.id"],
        ),
        schema="queue",
    )
    op.create_index(
        "idx_dlq_tenant", "jobs_dlq", ["tenant_id"], schema="queue",
    )
    op.create_index(
        "idx_dlq_source", "jobs_dlq", ["source_system"], schema="queue",
    )


def downgrade() -> None:
    op.drop_index("idx_dlq_source", "jobs_dlq", schema="queue")
    op.drop_index("idx_dlq_tenant", "jobs_dlq", schema="queue")
    op.drop_table("jobs_dlq", schema="queue")

    op.drop_constraint("chk_job_status", "jobs", schema="queue")
    op.create_check_constraint(
        "chk_job_status", "jobs",
        "status IN ('PENDING','PROCESSING','COMPLETED','FAILED')",
        schema="queue",
    )

    op.drop_column("jobs", "trace_context", schema="queue")
    op.drop_column("jobs", "error_history", schema="queue")
    op.drop_column("jobs", "max_retries", schema="queue")
    op.drop_column("jobs", "attempt_count", schema="queue")
