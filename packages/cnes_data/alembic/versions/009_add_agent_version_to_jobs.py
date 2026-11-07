"""add agent_version to queue.jobs.

Revision ID: 009
Revises: 008
Create Date: 2026-04-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "009"
down_revision: str = "008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:  # pragma: no cover - alembic migration
    op.add_column(
        "jobs",
        sa.Column("agent_version", sa.String(length=50), nullable=True),
        schema="queue",
    )


def downgrade() -> None:  # pragma: no cover - alembic migration
    op.drop_column("jobs", "agent_version", schema="queue")
