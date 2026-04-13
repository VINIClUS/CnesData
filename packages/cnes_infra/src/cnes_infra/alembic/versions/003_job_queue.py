"""Job queue — fila de processamento assíncrono.

Revision ID: 003
Revises: 002
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "003"
down_revision: Union[str, Sequence[str], None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS queue")
    op.create_table(
        "jobs",
        sa.Column(
            "id", postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column(
            "status", sa.String(20), nullable=False,
            server_default=sa.text("'PENDING'"),
        ),
        sa.Column("source_system", sa.String(30), nullable=False),
        sa.Column("tenant_id", sa.String(6), nullable=False),
        sa.Column("payload_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("error_detail", sa.Text),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["payload_id"], ["landing.raw_payload.id"],
        ),
        sa.CheckConstraint(
            "status IN ('PENDING','PROCESSING','COMPLETED','FAILED')",
            name="chk_job_status",
        ),
        schema="queue",
    )
    op.create_index(
        "idx_jobs_pending",
        "jobs",
        ["status"],
        schema="queue",
        postgresql_where=sa.text("status = 'PENDING'"),
    )


def downgrade() -> None:
    op.drop_index("idx_jobs_pending", table_name="jobs", schema="queue")
    op.drop_table("jobs", schema="queue")
    op.execute("DROP SCHEMA IF EXISTS queue")
