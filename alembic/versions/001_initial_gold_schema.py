"""Initial gold schema.

Revision ID: 001
Revises:
Create Date: 2026-04-11

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Cria schema gold e tabelas dimensão/fato."""
    op.execute("CREATE SCHEMA IF NOT EXISTS gold")

    op.create_table(
        "dim_estabelecimento",
        sa.Column("tenant_id", sa.String(6), nullable=False),
        sa.Column("cnes", sa.String(7), nullable=False),
        sa.Column("nome_fantasia", sa.String(120)),
        sa.Column("tipo_unidade", sa.String(2)),
        sa.Column("cnpj_mantenedora", sa.String(14)),
        sa.Column("natureza_juridica", sa.String(4)),
        sa.Column("vinculo_sus", sa.Boolean(), server_default=sa.text("FALSE")),
        sa.Column("fontes", postgresql.JSONB(), server_default=sa.text("'{}'")),
        sa.Column(
            "criado_em",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "atualizado_em",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("tenant_id", "cnes"),
        sa.CheckConstraint(r"cnes ~ '^\d{7}$'", name="chk_cnes_format"),
        sa.CheckConstraint(
            r"tenant_id ~ '^\d{6}$'", name="chk_tenant_format_estab"
        ),
        schema="gold",
    )

    op.create_table(
        "dim_profissional",
        sa.Column("tenant_id", sa.String(6), nullable=False),
        sa.Column("cpf", sa.String(11), nullable=False),
        sa.Column("cns", sa.String(15)),
        sa.Column("nome_profissional", sa.String(120)),
        sa.Column("sexo", sa.String(1)),
        sa.Column("fontes", postgresql.JSONB(), server_default=sa.text("'{}'")),
        sa.Column(
            "criado_em",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "atualizado_em",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("tenant_id", "cpf"),
        sa.CheckConstraint(r"cpf ~ '^\d{11}$'", name="chk_cpf_format"),
        sa.CheckConstraint(
            r"cns IS NULL OR cns ~ '^\d{15}$'", name="chk_cns_format"
        ),
        sa.CheckConstraint(
            r"tenant_id ~ '^\d{6}$'", name="chk_tenant_format_prof"
        ),
        schema="gold",
    )

    op.create_table(
        "fato_vinculo",
        sa.Column("tenant_id", sa.String(6), nullable=False),
        sa.Column("competencia", sa.String(7), nullable=False),
        sa.Column("cpf", sa.String(11), nullable=False),
        sa.Column("cnes", sa.String(7), nullable=False),
        sa.Column("cbo", sa.String(6), nullable=False),
        sa.Column("tipo_vinculo", sa.String(6)),
        sa.Column("sus", sa.Boolean()),
        sa.Column("ch_total", sa.Integer()),
        sa.Column("ch_ambulatorial", sa.Integer()),
        sa.Column("ch_outras", sa.Integer()),
        sa.Column("ch_hospitalar", sa.Integer()),
        sa.Column("fontes", postgresql.JSONB(), server_default=sa.text("'{}'")),
        sa.Column(
            "criado_em",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "atualizado_em",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("tenant_id", "competencia", "cpf", "cnes", "cbo"),
        sa.ForeignKeyConstraint(
            ["tenant_id", "cpf"],
            ["gold.dim_profissional.tenant_id", "gold.dim_profissional.cpf"],
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "cnes"],
            ["gold.dim_estabelecimento.tenant_id", "gold.dim_estabelecimento.cnes"],
        ),
        schema="gold",
    )

    op.create_index(
        "idx_fato_vinculo_cpf",
        "fato_vinculo",
        ["tenant_id", "cpf"],
        schema="gold",
    )
    op.create_index(
        "idx_fato_vinculo_cnes",
        "fato_vinculo",
        ["tenant_id", "cnes"],
        schema="gold",
    )


def downgrade() -> None:
    """Remove schema gold e todo seu conteúdo."""
    op.execute("DROP SCHEMA IF EXISTS gold CASCADE")
