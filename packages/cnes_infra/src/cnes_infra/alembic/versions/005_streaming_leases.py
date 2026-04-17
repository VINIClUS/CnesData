"""Streaming leases: machine_id, heartbeat, lease_expires_at, object_key.

Revision ID: 005
Revises: 004
Create Date: 2026-04-13
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:  # pragma: no cover - alembic migration
    op.add_column(
        "jobs",
        sa.Column("machine_id", sa.String(128)),
        schema="queue",
    )
    op.add_column(
        "jobs",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        schema="queue",
    )
    op.add_column(
        "jobs",
        sa.Column("heartbeat_at", sa.DateTime(timezone=True)),
        schema="queue",
    )

    op.drop_constraint("chk_job_status", "jobs", schema="queue")
    op.create_check_constraint(
        "chk_job_status", "jobs",
        "status IN ("
        "'PENDING','ACQUIRED','STREAMING','PROCESSING',"
        "'COMPLETED','FAILED','DEAD_LETTER'"
        ")",
        schema="queue",
    )

    op.create_index(
        "idx_jobs_leased", "jobs",
        ["status", "lease_expires_at"],
        schema="queue",
        postgresql_where=sa.text(
            "status IN ('ACQUIRED','STREAMING')",
        ),
    )

    op.add_column(
        "raw_payload",
        sa.Column("object_key", sa.String(512)),
        schema="landing",
    )
    op.alter_column(
        "raw_payload", "payload",
        existing_type=postgresql.JSONB,
        nullable=True,
        schema="landing",
    )


def downgrade() -> None:  # pragma: no cover - alembic migration
    op.alter_column(
        "raw_payload", "payload",
        existing_type=postgresql.JSONB,
        nullable=False,
        schema="landing",
    )
    op.drop_column("raw_payload", "object_key", schema="landing")

    op.drop_index(
        "idx_jobs_leased", "jobs", schema="queue",
    )

    op.drop_constraint("chk_job_status", "jobs", schema="queue")
    op.create_check_constraint(
        "chk_job_status", "jobs",
        "status IN ("
        "'PENDING','PROCESSING','COMPLETED','FAILED','DEAD_LETTER'"
        ")",
        schema="queue",
    )

    op.drop_column("jobs", "heartbeat_at", schema="queue")
    op.drop_column("jobs", "lease_expires_at", schema="queue")
    op.drop_column("jobs", "machine_id", schema="queue")
