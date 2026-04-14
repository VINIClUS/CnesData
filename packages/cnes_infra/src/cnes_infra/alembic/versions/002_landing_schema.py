"""Landing schema — camada bronze para payloads brutos.

Revision ID: 002
Revises: 001
Create Date: 2026-04-12

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "002"
down_revision: str | Sequence[str] | None = "001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS landing")
    op.create_table(
        "raw_payload",
        sa.Column(
            "id", postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", sa.String(6), nullable=False),
        sa.Column("source_system", sa.String(30), nullable=False),
        sa.Column("competencia", sa.String(7), nullable=False),
        sa.Column("payload", postgresql.JSONB, nullable=False),
        sa.Column(
            "received_at", sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        schema="landing",
    )


def downgrade() -> None:
    op.drop_table("raw_payload", schema="landing")
    op.execute("DROP SCHEMA IF EXISTS landing")
