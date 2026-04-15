"""GIN index on fato_vinculo.fontes for snapshot replace DELETE performance.

Revision ID: 006
Revises: 005
Create Date: 2026-04-15
"""

from collections.abc import Sequence

from alembic import op

revision: str = "006"
down_revision: str = "005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "idx_fato_vinculo_fontes",
        "fato_vinculo",
        ["fontes"],
        schema="gold",
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index(
        "idx_fato_vinculo_fontes",
        "fato_vinculo",
        schema="gold",
    )
