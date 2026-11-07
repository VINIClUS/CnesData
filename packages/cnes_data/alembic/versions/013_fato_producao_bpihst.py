"""013_fato_producao_bpihst: extend chk_fonte_amb with SIA_BPIHST.

Revision ID: 013_fato_producao_bpihst
Revises: 012_fato_producao_unique
Create Date: 2026-04-23

Adds SIA_BPIHST to the allowed fonte_sistema values for
fato_producao_ambulatorial. Keeps parity with the Python Literal
union ProducaoAmbulatorial.fonte_sistema.
"""
from collections.abc import Sequence

from alembic import op

revision: str = "013_fato_producao_bpihst"
down_revision: str | Sequence[str] | None = "012_fato_producao_unique"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:  # pragma: no cover - alembic migration
    op.execute(
        "ALTER TABLE gold.fato_producao_ambulatorial "
        "DROP CONSTRAINT IF EXISTS chk_fonte_amb",
    )
    op.execute(
        "ALTER TABLE gold.fato_producao_ambulatorial "
        "ADD CONSTRAINT chk_fonte_amb CHECK ("
        "fonte_sistema IN ('SIA_APA','SIA_BPI','SIA_BPIHST','BPA_C','BPA_I')"
        ")",
    )


def downgrade() -> None:  # pragma: no cover - alembic migration
    op.execute(
        "ALTER TABLE gold.fato_producao_ambulatorial "
        "DROP CONSTRAINT IF EXISTS chk_fonte_amb",
    )
    op.execute(
        "ALTER TABLE gold.fato_producao_ambulatorial "
        "ADD CONSTRAINT chk_fonte_amb CHECK ("
        "fonte_sistema IN ('SIA_APA','SIA_BPI','BPA_C','BPA_I')"
        ")",
    )
