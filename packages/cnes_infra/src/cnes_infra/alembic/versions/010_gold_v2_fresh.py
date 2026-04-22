"""Gold v2 fresh: drop v1 + queue, create gold+landing v2.

Revision ID: 010
Revises: 009
Create Date: 2026-04-22

DESTRUCTIVE: drops all v1 Gold + queue tables. Safe because user
confirmed no production data at time of migration.

Downgrade does NOT restore v1 — only cleans v2 artifacts.
"""
from collections.abc import Sequence

from alembic import op

revision: str = "010"
down_revision: str | Sequence[str] | None = "009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _drop_v1_and_queue() -> None:
    op.execute("DROP TABLE IF EXISTS gold.fato_vinculo CASCADE")
    op.execute("DROP TABLE IF EXISTS gold.dim_profissional CASCADE")
    op.execute("DROP TABLE IF EXISTS gold.dim_estabelecimento CASCADE")
    op.execute("DROP TABLE IF EXISTS queue.jobs_dlq CASCADE")
    op.execute("DROP TABLE IF EXISTS queue.jobs CASCADE")
    op.execute("DROP TABLE IF EXISTS queue.batch_trigger CASCADE")
    op.execute("DROP SCHEMA IF EXISTS queue CASCADE")
    op.execute("DROP TABLE IF EXISTS landing.raw_payload CASCADE")
    op.execute("DROP TABLE IF EXISTS landing.raw_extractions CASCADE")


def _create_landing_extractions() -> None:
    op.execute(r"""
        CREATE TABLE landing.extractions (
            id                UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
            job_id            UUID          NOT NULL,
            tenant_id         CHAR(6)       NOT NULL,
            fonte_sistema     TEXT          NOT NULL,
            tipo_extracao     TEXT          NOT NULL,
            competencia       INT4          NOT NULL,
            object_key        TEXT,
            row_count         INT4,
            sha256            CHAR(64),
            schema_version    SMALLINT      NOT NULL DEFAULT 1,
            status            TEXT          NOT NULL DEFAULT 'PENDING',
            agent_version     TEXT,
            machine_id        TEXT,
            lease_owner       TEXT,
            lease_until       TIMESTAMPTZ,
            retry_count       INT4          NOT NULL DEFAULT 0,
            error_detail      TEXT,
            created_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
            uploaded_at       TIMESTAMPTZ,
            started_at        TIMESTAMPTZ,
            completed_at      TIMESTAMPTZ,
            CONSTRAINT uniq_source_comp UNIQUE (fonte_sistema, tenant_id, competencia, tipo_extracao),
            CONSTRAINT chk_fonte_sistema CHECK (fonte_sistema IN (
                'CNES_LOCAL','CNES_NACIONAL','SIHD','SIA_APA','SIA_BPI','BPA_C','BPA_I'
            )),
            CONSTRAINT chk_status CHECK (status IN (
                'PENDING','UPLOADED','PROCESSING','INGESTED','FAILED','DLQ'
            )),
            CONSTRAINT chk_competencia_yyyymm CHECK (
                competencia BETWEEN 200001 AND 209912
                AND (competencia % 100) BETWEEN 1 AND 12
            ),
            CONSTRAINT chk_tenant_format CHECK (tenant_id ~ '^\d{6}$')
        );
        CREATE INDEX ix_extractions_tenant_comp ON landing.extractions (tenant_id, competencia);
        CREATE INDEX ix_extractions_job ON landing.extractions (job_id);
        CREATE INDEX ix_extractions_status_ready ON landing.extractions (status) WHERE status = 'UPLOADED';
        CREATE INDEX ix_extractions_lease_expired ON landing.extractions (lease_until)
            WHERE status = 'PROCESSING';
        CREATE INDEX ix_extractions_pending ON landing.extractions (status, created_at) WHERE status = 'PENDING';
        ALTER TABLE landing.extractions SET (autovacuum_vacuum_scale_factor = 0.05);
        ALTER TABLE landing.extractions ENABLE ROW LEVEL SECURITY;
        CREATE POLICY tenant_isolation ON landing.extractions
            USING (tenant_id = current_setting('app.tenant_id', true));
    """)


def _create_dims() -> None:
    op.execute("""
        CREATE TABLE gold.dim_profissional (
            sk_profissional INT4 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            cpf_hash CHAR(11) NOT NULL UNIQUE,
            nome TEXT NOT NULL,
            cns CHAR(15),
            sk_cbo_principal INT4,
            fontes JSONB NOT NULL DEFAULT '{}'::JSONB,
            criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE TABLE gold.dim_cbo (
            sk_cbo INT4 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            cod_cbo CHAR(6) NOT NULL UNIQUE,
            descricao TEXT NOT NULL
        );
        CREATE TABLE gold.dim_cid10 (
            sk_cid INT4 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            cod_cid CHAR(4) NOT NULL UNIQUE,
            descricao TEXT NOT NULL,
            capitulo SMALLINT NOT NULL,
            CONSTRAINT chk_capitulo CHECK (capitulo BETWEEN 1 AND 22)
        );
        CREATE INDEX ix_cid_capitulo ON gold.dim_cid10 (capitulo);
        CREATE TABLE gold.dim_municipio (
            sk_municipio INT4 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            ibge6 CHAR(6) NOT NULL UNIQUE,
            ibge7 CHAR(7) NOT NULL UNIQUE,
            nome TEXT NOT NULL,
            uf CHAR(2) NOT NULL,
            populacao_estimada INT4,
            teto_pab_cents BIGINT
        );
        CREATE INDEX ix_municipio_uf ON gold.dim_municipio (uf);
        CREATE TABLE gold.dim_estabelecimento (
            sk_estabelecimento INT4 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            cnes CHAR(7) NOT NULL UNIQUE,
            nome TEXT NOT NULL,
            cnpj_mantenedora CHAR(14),
            tp_unid SMALLINT NOT NULL,
            sk_municipio INT4 NOT NULL REFERENCES gold.dim_municipio (sk_municipio),
            fontes JSONB NOT NULL DEFAULT '{}'::JSONB,
            criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX ix_estab_municipio ON gold.dim_estabelecimento (sk_municipio);
        CREATE INDEX ix_estab_tp_unid ON gold.dim_estabelecimento (tp_unid);
        CREATE TABLE gold.dim_procedimento_sus (
            sk_procedimento INT4 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            cod_sigtap CHAR(10) NOT NULL UNIQUE,
            descricao TEXT NOT NULL,
            complexidade SMALLINT,
            financiamento CHAR(3),
            modalidade CHAR(3),
            competencia_vigencia_ini INT4,
            competencia_vigencia_fim INT4,
            CONSTRAINT chk_complexidade CHECK (complexidade IN (1,2,3)),
            CONSTRAINT chk_financiamento CHECK (financiamento IN ('MAC','FAE','PAB','VISA')),
            CONSTRAINT chk_modalidade CHECK (modalidade IN ('AMB','HOSP','APAC'))
        );
        CREATE TABLE gold.dim_competencia (
            sk_competencia INT4 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            competencia INT4 NOT NULL UNIQUE,
            ano SMALLINT NOT NULL,
            mes SMALLINT NOT NULL,
            qtd_dias_uteis SMALLINT,
            inicio_coleta DATE,
            fim_coleta DATE,
            CONSTRAINT chk_mes_valido CHECK (mes BETWEEN 1 AND 12)
        );
        CREATE INDEX ix_competencia_ano_mes ON gold.dim_competencia (ano, mes);
    """)


def _populate_dim_competencia() -> None:
    values = []
    for ano in range(2020, 2041):
        for mes in range(1, 13):
            c = ano * 100 + mes
            values.append(f"({c}, {ano}, {mes})")
    stmt = "INSERT INTO gold.dim_competencia (competencia, ano, mes) VALUES "
    stmt += ", ".join(values)
    op.execute(stmt)


def _create_fatos_and_partitions() -> None:
    op.execute("""
        CREATE TABLE gold.fato_vinculo_cnes (
            sk_vinculo BIGINT GENERATED ALWAYS AS IDENTITY,
            sk_profissional INT4 NOT NULL,
            sk_estabelecimento INT4 NOT NULL,
            sk_cbo INT4 NOT NULL,
            sk_competencia INT4 NOT NULL,
            carga_horaria_sem SMALLINT,
            ind_vinc CHAR(6),
            sk_equipe INT4,
            job_id UUID NOT NULL,
            fonte_sistema TEXT NOT NULL,
            extracao_ts TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (sk_competencia, sk_vinculo),
            CONSTRAINT chk_fonte_cnes CHECK (fonte_sistema IN ('CNES_LOCAL','CNES_NACIONAL'))
        ) PARTITION BY RANGE (sk_competencia);
        CREATE TABLE gold.fato_producao_ambulatorial (
            sk_producao BIGINT GENERATED ALWAYS AS IDENTITY,
            sk_profissional INT4 NOT NULL,
            sk_estabelecimento INT4 NOT NULL,
            sk_procedimento INT4 NOT NULL,
            sk_competencia INT4 NOT NULL,
            sk_cid_principal INT4,
            qtd INT4 NOT NULL CHECK (qtd > 0),
            valor_aprov_cents BIGINT NOT NULL DEFAULT 0 CHECK (valor_aprov_cents >= 0),
            dt_atendimento DATE,
            job_id UUID NOT NULL,
            fonte_sistema TEXT NOT NULL,
            extracao_ts TIMESTAMPTZ NOT NULL,
            fontes_reportadas JSONB,
            PRIMARY KEY (sk_competencia, sk_producao),
            CONSTRAINT chk_fonte_amb CHECK (fonte_sistema IN ('SIA_APA','SIA_BPI','BPA_C','BPA_I'))
        ) PARTITION BY RANGE (sk_competencia);
        CREATE TABLE gold.fato_internacao (
            sk_aih BIGINT GENERATED ALWAYS AS IDENTITY,
            num_aih CHAR(13) NOT NULL,
            sk_profissional_solicit INT4,
            sk_estabelecimento INT4 NOT NULL,
            sk_competencia INT4 NOT NULL,
            sk_cid_principal INT4,
            dt_internacao DATE NOT NULL,
            dt_saida DATE,
            valor_total_cents BIGINT,
            job_id UUID NOT NULL,
            fonte_sistema TEXT NOT NULL DEFAULT 'SIHD',
            extracao_ts TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (sk_competencia, sk_aih),
            CONSTRAINT chk_fonte_sihd CHECK (fonte_sistema = 'SIHD'),
            CONSTRAINT chk_datas_aih CHECK (dt_saida IS NULL OR dt_saida >= dt_internacao)
        ) PARTITION BY RANGE (sk_competencia);
        CREATE TABLE gold.fato_procedimento_aih (
            sk_proc_aih BIGINT GENERATED ALWAYS AS IDENTITY,
            sk_aih BIGINT NOT NULL,
            sk_procedimento INT4 NOT NULL,
            sk_profissional_exec INT4,
            sk_competencia INT4 NOT NULL,
            qtd INT4 NOT NULL DEFAULT 1 CHECK (qtd > 0),
            valor_cents BIGINT CHECK (valor_cents IS NULL OR valor_cents >= 0),
            job_id UUID NOT NULL,
            extracao_ts TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (sk_competencia, sk_proc_aih)
        ) PARTITION BY RANGE (sk_competencia);
    """)
    op.execute("""
        CREATE TABLE gold.fato_vinculo_cnes_2026 PARTITION OF gold.fato_vinculo_cnes FOR VALUES FROM (73) TO (85);
        CREATE TABLE gold.fato_producao_ambulatorial_2026 PARTITION OF gold.fato_producao_ambulatorial FOR VALUES FROM (73) TO (85);
        CREATE TABLE gold.fato_internacao_2026 PARTITION OF gold.fato_internacao FOR VALUES FROM (73) TO (85);
        CREATE TABLE gold.fato_procedimento_aih_2026 PARTITION OF gold.fato_procedimento_aih FOR VALUES FROM (73) TO (85);
    """)


def _create_mv() -> None:
    op.execute("""
        CREATE MATERIALIZED VIEW gold.view_auditoria_producao AS
        SELECT
            COALESCE(fv.sk_profissional, fpa.sk_profissional, fi.sk_profissional_solicit) AS sk_profissional,
            COALESCE(fv.sk_estabelecimento, fpa.sk_estabelecimento, fi.sk_estabelecimento) AS sk_estabelecimento,
            COALESCE(fv.sk_competencia, fpa.sk_competencia, fi.sk_competencia) AS sk_competencia,
            COUNT(DISTINCT fv.sk_vinculo) AS qtd_vinculos,
            COUNT(DISTINCT fpa.sk_producao) AS qtd_producao_amb,
            COUNT(DISTINCT fi.sk_aih) AS qtd_aih,
            COALESCE(SUM(fpa.valor_aprov_cents), 0) AS valor_producao_cents,
            COALESCE(SUM(fi.valor_total_cents), 0) AS valor_aih_cents
        FROM gold.fato_vinculo_cnes fv
        FULL OUTER JOIN gold.fato_producao_ambulatorial fpa
            USING (sk_profissional, sk_estabelecimento, sk_competencia)
        FULL OUTER JOIN gold.fato_internacao fi
            ON fi.sk_profissional_solicit = COALESCE(fv.sk_profissional, fpa.sk_profissional)
            AND fi.sk_estabelecimento = COALESCE(fv.sk_estabelecimento, fpa.sk_estabelecimento)
            AND fi.sk_competencia = COALESCE(fv.sk_competencia, fpa.sk_competencia)
        GROUP BY 1, 2, 3;
        CREATE UNIQUE INDEX ix_vap_key ON gold.view_auditoria_producao (sk_profissional, sk_estabelecimento, sk_competencia);
        CREATE INDEX ix_vap_comp ON gold.view_auditoria_producao (sk_competencia);
    """)


def upgrade() -> None:  # pragma: no cover - alembic migration
    _drop_v1_and_queue()
    op.execute("CREATE SCHEMA IF NOT EXISTS landing")
    op.execute("CREATE SCHEMA IF NOT EXISTS gold")
    _create_landing_extractions()
    _create_dims()
    _populate_dim_competencia()
    _create_fatos_and_partitions()
    _create_mv()


def downgrade() -> None:  # pragma: no cover - alembic migration
    op.execute("DROP MATERIALIZED VIEW IF EXISTS gold.view_auditoria_producao")
    op.execute("DROP TABLE IF EXISTS gold.fato_procedimento_aih CASCADE")
    op.execute("DROP TABLE IF EXISTS gold.fato_internacao CASCADE")
    op.execute("DROP TABLE IF EXISTS gold.fato_producao_ambulatorial CASCADE")
    op.execute("DROP TABLE IF EXISTS gold.fato_vinculo_cnes CASCADE")
    for dim in (
        "dim_competencia", "dim_municipio", "dim_cid10", "dim_cbo",
        "dim_procedimento_sus", "dim_estabelecimento", "dim_profissional",
    ):
        op.execute(f"DROP TABLE IF EXISTS gold.{dim} CASCADE")
    op.execute("DROP TABLE IF EXISTS landing.extractions CASCADE")
