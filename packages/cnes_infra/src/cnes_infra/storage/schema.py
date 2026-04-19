"""SQLAlchemy Core — tabelas do schema gold (estrela CNES)."""
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

gold_metadata = MetaData(schema="gold")

dim_estabelecimento = Table(
    "dim_estabelecimento",
    gold_metadata,
    Column("tenant_id", String(6), nullable=False),
    Column("cnes", String(7), nullable=False),
    Column("nome_fantasia", String(120)),
    Column("tipo_unidade", String(2)),
    Column("cnpj_mantenedora", String(14)),
    Column("natureza_juridica", String(4)),
    Column("vinculo_sus", Boolean, server_default=text("FALSE")),
    # Contrato: fontes = dict[str, bool], ex: {"WEB": True, "LOCAL": True}.
    # Upsert usa `||` (merge raso por chave) — idempotente SOMENTE enquanto
    # o tipo for JSONB-object. Mudança para array quebra test_fontes_idempotency.
    Column("fontes", JSONB, server_default=text("'{}'::jsonb")),
    Column("criado_em", DateTime(timezone=True), server_default=text("NOW()")),
    Column("atualizado_em", DateTime(timezone=True), server_default=text("NOW()")),
    CheckConstraint(r"cnes ~ '^\d{7}$'", name="chk_cnes_format"),
    CheckConstraint(r"tenant_id ~ '^\d{6}$'", name="chk_tenant_format"),
)

dim_profissional = Table(
    "dim_profissional",
    gold_metadata,
    Column("tenant_id", String(6), nullable=False),
    Column("cpf", String(11), nullable=False),
    Column("cns", String(15)),
    Column("nome_profissional", String(120)),
    Column("sexo", String(1)),
    # Contrato: fontes = dict[str, bool], ex: {"WEB": True, "LOCAL": True}.
    # Upsert usa `||` (merge raso por chave) — idempotente SOMENTE enquanto
    # o tipo for JSONB-object. Mudança para array quebra test_fontes_idempotency.
    Column("fontes", JSONB, server_default=text("'{}'::jsonb")),
    Column("criado_em", DateTime(timezone=True), server_default=text("NOW()")),
    Column("atualizado_em", DateTime(timezone=True), server_default=text("NOW()")),
    CheckConstraint(r"cpf ~ '^\d{11}$'", name="chk_cpf_format"),
    CheckConstraint(r"cns IS NULL OR cns ~ '^\d{15}$'", name="chk_cns_format"),
    CheckConstraint(r"tenant_id ~ '^\d{6}$'", name="chk_tenant_format"),
)

fato_vinculo = Table(
    "fato_vinculo",
    gold_metadata,
    Column("tenant_id", String(6), nullable=False),
    Column("competencia", String(7), nullable=False),
    Column("cpf", String(11), nullable=False),
    Column("cnes", String(7), nullable=False),
    Column("cbo", String(6), nullable=False),
    Column("tipo_vinculo", String(6)),
    Column("sus", Boolean),
    Column("ch_total", Integer),
    Column("ch_ambulatorial", Integer),
    Column("ch_outras", Integer),
    Column("ch_hospitalar", Integer),
    # Contrato: fontes = dict[str, bool], ex: {"WEB": True, "LOCAL": True}.
    # Upsert usa `||` (merge raso por chave) — idempotente SOMENTE enquanto
    # o tipo for JSONB-object. Mudança para array quebra test_fontes_idempotency.
    Column("fontes", JSONB, server_default=text("'{}'::jsonb")),
    Column("criado_em", DateTime(timezone=True), server_default=text("NOW()")),
    Column("atualizado_em", DateTime(timezone=True), server_default=text("NOW()")),
    ForeignKeyConstraint(
        ["tenant_id", "cpf"],
        ["gold.dim_profissional.tenant_id", "gold.dim_profissional.cpf"],
    ),
    ForeignKeyConstraint(
        ["tenant_id", "cnes"],
        ["gold.dim_estabelecimento.tenant_id", "gold.dim_estabelecimento.cnes"],
    ),
    Index("idx_fato_vinculo_cpf", "tenant_id", "cpf"),
    Index("idx_fato_vinculo_cnes", "tenant_id", "cnes"),
)
