"""SQLAlchemy Core table definitions for Gold v2 + landing v2."""
from __future__ import annotations

from sqlalchemy import (
    CHAR,
    BigInteger,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Identity,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    SmallInteger,
    Table,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = MetaData()

extractions = Table(
    "extractions",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()),
    Column("job_id", UUID(as_uuid=True), nullable=False),
    Column("tenant_id", CHAR(6), nullable=False),
    Column("fonte_sistema", Text, nullable=False),
    Column("tipo_extracao", Text, nullable=False),
    Column("competencia", Integer, nullable=False),
    Column("object_key", Text),
    Column("row_count", Integer),
    Column("sha256", CHAR(64)),
    Column("schema_version", SmallInteger, nullable=False, server_default="1"),
    Column("status", Text, nullable=False, server_default="PENDING"),
    Column("agent_version", Text),
    Column("machine_id", Text),
    Column("lease_owner", Text),
    Column("lease_until", DateTime(timezone=True)),
    Column("retry_count", Integer, nullable=False, server_default="0"),
    Column("error_detail", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("uploaded_at", DateTime(timezone=True)),
    Column("started_at", DateTime(timezone=True)),
    Column("completed_at", DateTime(timezone=True)),
    UniqueConstraint(
        "fonte_sistema",
        "tenant_id",
        "competencia",
        "tipo_extracao",
        name="uniq_source_comp",
    ),
    schema="landing",
)

dim_profissional = Table(
    "dim_profissional",
    metadata,
    Column("sk_profissional", Integer, Identity(always=True), primary_key=True),
    Column("cpf_hash", CHAR(11), nullable=False, unique=True),
    Column("nome", Text, nullable=False),
    Column("cns", CHAR(15)),
    Column("sk_cbo_principal", Integer),
    Column("fontes", JSONB, nullable=False, server_default="{}"),
    Column("criado_em", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("atualizado_em", DateTime(timezone=True), nullable=False, server_default=func.now()),
    schema="gold",
)

dim_cbo = Table(
    "dim_cbo",
    metadata,
    Column("sk_cbo", Integer, Identity(always=True), primary_key=True),
    Column("cod_cbo", CHAR(6), nullable=False, unique=True),
    Column("descricao", Text, nullable=False),
    schema="gold",
)

dim_cid10 = Table(
    "dim_cid10",
    metadata,
    Column("sk_cid", Integer, Identity(always=True), primary_key=True),
    Column("cod_cid", CHAR(4), nullable=False, unique=True),
    Column("descricao", Text, nullable=False),
    Column("capitulo", SmallInteger, nullable=False),
    schema="gold",
)

dim_municipio = Table(
    "dim_municipio",
    metadata,
    Column("sk_municipio", Integer, Identity(always=True), primary_key=True),
    Column("ibge6", CHAR(6), nullable=False, unique=True),
    Column("ibge7", CHAR(7), nullable=False, unique=True),
    Column("nome", Text, nullable=False),
    Column("uf", CHAR(2), nullable=False),
    Column("populacao_estimada", Integer),
    Column("teto_pab_cents", BigInteger),
    schema="gold",
)

dim_estabelecimento = Table(
    "dim_estabelecimento",
    metadata,
    Column("sk_estabelecimento", Integer, Identity(always=True), primary_key=True),
    Column("cnes", CHAR(7), nullable=False, unique=True),
    Column("nome", Text, nullable=False),
    Column("cnpj_mantenedora", CHAR(14)),
    Column("tp_unid", SmallInteger, nullable=False),
    Column(
        "sk_municipio",
        Integer,
        ForeignKey("gold.dim_municipio.sk_municipio"),
        nullable=False,
    ),
    Column("fontes", JSONB, nullable=False, server_default="{}"),
    Column("criado_em", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("atualizado_em", DateTime(timezone=True), nullable=False, server_default=func.now()),
    schema="gold",
)

dim_procedimento_sus = Table(
    "dim_procedimento_sus",
    metadata,
    Column("sk_procedimento", Integer, Identity(always=True), primary_key=True),
    Column("cod_sigtap", CHAR(10), nullable=False, unique=True),
    Column("descricao", Text, nullable=False),
    Column("complexidade", SmallInteger),
    Column("financiamento", CHAR(3)),
    Column("modalidade", CHAR(3)),
    Column("competencia_vigencia_ini", Integer),
    Column("competencia_vigencia_fim", Integer),
    schema="gold",
)

dim_competencia = Table(
    "dim_competencia",
    metadata,
    Column("sk_competencia", Integer, Identity(always=True), primary_key=True),
    Column("competencia", Integer, nullable=False, unique=True),
    Column("ano", SmallInteger, nullable=False),
    Column("mes", SmallInteger, nullable=False),
    Column("qtd_dias_uteis", SmallInteger),
    Column("inicio_coleta", Date),
    Column("fim_coleta", Date),
    schema="gold",
)

fato_vinculo_cnes = Table(
    "fato_vinculo_cnes",
    metadata,
    Column("sk_vinculo", BigInteger, Identity(always=True)),
    Column("sk_profissional", Integer, nullable=False),
    Column("sk_estabelecimento", Integer, nullable=False),
    Column("sk_cbo", Integer, nullable=False),
    Column("sk_competencia", Integer, nullable=False),
    Column("carga_horaria_sem", SmallInteger),
    Column("ind_vinc", CHAR(6)),
    Column("sk_equipe", Integer),
    Column("job_id", UUID(as_uuid=True), nullable=False),
    Column("fonte_sistema", Text, nullable=False),
    Column("extracao_ts", DateTime(timezone=True), nullable=False),
    PrimaryKeyConstraint("sk_competencia", "sk_vinculo", name="pk_fato_vinculo_cnes"),
    schema="gold",
)

fato_producao_ambulatorial = Table(
    "fato_producao_ambulatorial",
    metadata,
    Column("sk_producao", BigInteger, Identity(always=True)),
    Column("sk_profissional", Integer, nullable=False),
    Column("sk_estabelecimento", Integer, nullable=False),
    Column("sk_procedimento", Integer, nullable=False),
    Column("sk_competencia", Integer, nullable=False),
    Column("sk_cid_principal", Integer),
    Column("qtd", Integer, nullable=False),
    Column("valor_aprov_cents", BigInteger, nullable=False, server_default="0"),
    Column("dt_atendimento", Date),
    Column("job_id", UUID(as_uuid=True), nullable=False),
    Column("fonte_sistema", Text, nullable=False),
    Column("extracao_ts", DateTime(timezone=True), nullable=False),
    Column("fontes_reportadas", JSONB),
    PrimaryKeyConstraint("sk_competencia", "sk_producao", name="pk_fato_producao_ambulatorial"),
    schema="gold",
)

fato_internacao = Table(
    "fato_internacao",
    metadata,
    Column("sk_aih", BigInteger, Identity(always=True)),
    Column("num_aih", CHAR(13), nullable=False),
    Column("sk_profissional_solicit", Integer),
    Column("sk_estabelecimento", Integer, nullable=False),
    Column("sk_competencia", Integer, nullable=False),
    Column("sk_cid_principal", Integer),
    Column("dt_internacao", Date, nullable=False),
    Column("dt_saida", Date),
    Column("valor_total_cents", BigInteger),
    Column("job_id", UUID(as_uuid=True), nullable=False),
    Column("fonte_sistema", Text, nullable=False, server_default="SIHD"),
    Column("extracao_ts", DateTime(timezone=True), nullable=False),
    PrimaryKeyConstraint("sk_competencia", "sk_aih", name="pk_fato_internacao"),
    schema="gold",
)

fato_procedimento_aih = Table(
    "fato_procedimento_aih",
    metadata,
    Column("sk_proc_aih", BigInteger, Identity(always=True)),
    Column("sk_aih", BigInteger, nullable=False),
    Column("sk_procedimento", Integer, nullable=False),
    Column("sk_profissional_exec", Integer),
    Column("sk_competencia", Integer, nullable=False),
    Column("qtd", Integer, nullable=False, server_default="1"),
    Column("valor_cents", BigInteger),
    Column("job_id", UUID(as_uuid=True), nullable=False),
    Column("extracao_ts", DateTime(timezone=True), nullable=False),
    PrimaryKeyConstraint("sk_competencia", "sk_proc_aih", name="pk_fato_procedimento_aih"),
    schema="gold",
)
