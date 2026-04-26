"""Add agent_version + machine_id columns to landing.extractions.

Revision ID: 017_agent_metadata
Revises: 016_access_requests
"""
import sqlalchemy as sa
from alembic import op

revision = "017_agent_metadata"
down_revision = "016_access_requests"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "extractions",
        sa.Column("agent_version", sa.Text(), nullable=True),
        schema="landing",
    )
    op.add_column(
        "extractions",
        sa.Column("machine_id", sa.Text(), nullable=True),
        schema="landing",
    )
    op.create_index(
        "ix_extractions_machine_id",
        "extractions",
        ["machine_id"],
        schema="landing",
        postgresql_where=sa.text("machine_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_extractions_machine_id",
        table_name="extractions",
        schema="landing",
    )
    op.drop_column("extractions", "machine_id", schema="landing")
    op.drop_column("extractions", "agent_version", schema="landing")
