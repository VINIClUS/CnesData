"""Add error_detail column to landing.extractions so mark_failed persists a reason.

extractions_repo.mark_failed accepted a `reason` param and discarded it —
landing.extractions had no column to hold a failure cause, so every worker
failure diagnosis was dropped on the floor (only ever visible in logs).

Revision ID: 020_error_detail_landing
Revises: 019_marketing_leads
Create Date: 2026-09-22
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "020_error_detail_landing"
down_revision = "019_marketing_leads"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "extractions",
        sa.Column("error_detail", sa.Text(), nullable=True),
        schema="landing",
    )


def downgrade() -> None:
    op.drop_column("extractions", "error_detail", schema="landing")
