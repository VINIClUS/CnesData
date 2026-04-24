"""012_fato_producao_unique: add UNIQUE index to fato_producao_ambulatorial.

Revision ID: 012_fato_producao_unique
Revises: 011_bpa_sia_sources
Create Date: 2026-04-23

Enables ON CONFLICT DO UPDATE upserts in producao_ambulatorial_repo.gravar
with idempotency + fontes_reportadas JSONB merge. The unique index on
(sk_competencia, sk_profissional, sk_estabelecimento, sk_procedimento,
job_id) includes the partition key (sk_competencia) as required by
Postgres for partitioned-table unique indexes.
"""
from collections.abc import Sequence

from alembic import op

revision: str = "012_fato_producao_unique"
down_revision: str | Sequence[str] | None = "011_bpa_sia_sources"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:  # pragma: no cover - alembic migration
    op.execute("""
        CREATE UNIQUE INDEX ix_fpa_natural_key
        ON gold.fato_producao_ambulatorial (
            sk_competencia, sk_profissional, sk_estabelecimento,
            sk_procedimento, job_id
        )
    """)


def downgrade() -> None:  # pragma: no cover - alembic migration
    op.execute("DROP INDEX IF EXISTS gold.ix_fpa_natural_key")
