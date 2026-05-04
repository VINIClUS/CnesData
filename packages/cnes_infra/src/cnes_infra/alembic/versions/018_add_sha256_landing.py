"""Add sha256 column to landing.extractions for P2 integrity verification.

Revision ID: 018_sha256_landing
Revises: 017_agent_metadata
Create Date: 2026-05-04
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "018_sha256_landing"
down_revision = "017_agent_metadata"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "extractions",
        sa.Column("sha256", sa.CHAR(64), nullable=True),
        schema="landing",
    )


def downgrade() -> None:
    op.drop_column("extractions", "sha256", schema="landing")
