"""Autovacuum storage params em tabelas de fila.

Revision ID: 007
Revises: 006
Create Date: 2026-04-19
"""

from collections.abc import Sequence

from alembic import op

revision: str = "007"
down_revision: str = "006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:  # pragma: no cover - alembic migration
    op.execute(
        "ALTER TABLE queue.jobs SET ("
        "autovacuum_vacuum_scale_factor = 0.05,"
        "autovacuum_vacuum_threshold = 50,"
        "autovacuum_analyze_scale_factor = 0.02,"
        "autovacuum_analyze_threshold = 50,"
        "autovacuum_vacuum_cost_delay = 2,"
        "autovacuum_vacuum_cost_limit = 1000"
        ")"
    )
    op.execute(
        "ALTER TABLE queue.jobs_dlq SET ("
        "autovacuum_vacuum_scale_factor = 0.1,"
        "autovacuum_vacuum_threshold = 100"
        ")"
    )
    op.execute(
        "ALTER TABLE landing.raw_payload SET ("
        "autovacuum_vacuum_scale_factor = 0.1,"
        "autovacuum_vacuum_threshold = 100"
        ")"
    )


def downgrade() -> None:  # pragma: no cover - alembic migration
    op.execute(
        "ALTER TABLE queue.jobs RESET ("
        "autovacuum_vacuum_scale_factor,"
        "autovacuum_vacuum_threshold,"
        "autovacuum_analyze_scale_factor,"
        "autovacuum_analyze_threshold,"
        "autovacuum_vacuum_cost_delay,"
        "autovacuum_vacuum_cost_limit"
        ")"
    )
    op.execute(
        "ALTER TABLE queue.jobs_dlq RESET ("
        "autovacuum_vacuum_scale_factor,"
        "autovacuum_vacuum_threshold"
        ")"
    )
    op.execute(
        "ALTER TABLE landing.raw_payload RESET ("
        "autovacuum_vacuum_scale_factor,"
        "autovacuum_vacuum_threshold"
        ")"
    )
