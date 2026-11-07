"""batch_trigger table + raw_payload.size_bytes column.

Revision ID: 008
Revises: 007
Create Date: 2026-04-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "008"
down_revision: str = "007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:  # pragma: no cover - alembic migration
    op.add_column(
        "raw_payload",
        sa.Column(
            "size_bytes", sa.BigInteger(), nullable=False,
            server_default=sa.text("0"),
        ),
        schema="landing",
    )
    op.create_table(
        "batch_trigger",
        sa.Column(
            "id", postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("tenant_id", sa.String(6)),
        sa.Column(
            "status", sa.String(10), nullable=False,
            server_default=sa.text("'CLOSED'"),
        ),
        sa.Column("opened_at", sa.DateTime(timezone=True)),
        sa.Column("closed_at", sa.DateTime(timezone=True)),
        sa.Column("reason", sa.String(40)),
        sa.Column("pending_bytes", sa.BigInteger()),
        sa.Column(
            "oldest_completed_at", sa.DateTime(timezone=True),
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "status IN ('OPEN','CLOSED')",
            name="chk_batch_trigger_status",
        ),
        sa.CheckConstraint(
            "tenant_id IS NULL OR tenant_id ~ '^[0-9]{6}$'",
            name="chk_batch_trigger_tenant",
        ),
        schema="queue",
    )
    op.execute(
        "CREATE UNIQUE INDEX batch_trigger_scope_uniq "
        "ON queue.batch_trigger (COALESCE(tenant_id, '__GLOBAL__'))"
    )
    op.execute(
        "ALTER TABLE queue.batch_trigger SET ("
        "autovacuum_vacuum_scale_factor = 0.01,"
        "autovacuum_vacuum_threshold = 10"
        ")"
    )
    op.execute(
        "INSERT INTO queue.batch_trigger (tenant_id, status) "
        "VALUES (NULL, 'CLOSED')"
    )


def downgrade() -> None:  # pragma: no cover - alembic migration
    op.drop_table("batch_trigger", schema="queue")
    op.drop_column("raw_payload", "size_bytes", schema="landing")
