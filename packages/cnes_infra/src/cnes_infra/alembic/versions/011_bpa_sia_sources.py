"""011_bpa_sia_sources: N-file landing + dim_misses (BPA/SIA pipeline).

Revision ID: 011_bpa_sia_sources
Revises: 010
Create Date: 2026-04-23

Replaces landing.extractions with the N-file manifest shape: source_type
(BPA_MAG/SIA_LOCAL added), files JSONB, depends_on UUID[], competencia
DATE. Adds landing.dim_misses for replay of missing dimension codes.

DESTRUCTIVE: drops the v1 landing.extractions shape. Safe because no
production data at time of migration (010 was also destructive).
"""
from collections.abc import Sequence

from alembic import op

revision: str = "011_bpa_sia_sources"
down_revision: str | Sequence[str] | None = "010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _drop_v1_extractions() -> None:
    op.execute("DROP TABLE IF EXISTS landing.extractions CASCADE")


def _create_v2_extractions() -> None:
    op.execute("""
        CREATE TABLE landing.extractions (
            job_id         UUID          PRIMARY KEY,
            tenant_id      TEXT          NOT NULL,
            source_type    TEXT          NOT NULL,
            competencia    DATE          NOT NULL,
            files          JSONB         NOT NULL DEFAULT '[]'::jsonb,
            depends_on     UUID[]        NOT NULL DEFAULT '{}'::uuid[],
            status         TEXT          NOT NULL DEFAULT 'PENDING',
            lease_until    TIMESTAMPTZ,
            created_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
            registered_at  TIMESTAMPTZ,
            CONSTRAINT extractions_source_type_check CHECK (source_type IN (
                'CNES_LOCAL', 'CNES_NACIONAL', 'SIHD',
                'BPA_MAG', 'SIA_LOCAL'
            ))
        );
        CREATE INDEX ix_extractions_tenant_comp
            ON landing.extractions (tenant_id, competencia);
        CREATE INDEX ix_extractions_pending
            ON landing.extractions (status, created_at)
            WHERE status = 'PENDING';
        CREATE INDEX ix_extractions_lease_expired
            ON landing.extractions (lease_until)
            WHERE status = 'PROCESSING';
        CREATE INDEX ix_extractions_depends_on
            ON landing.extractions USING GIN (depends_on);
        ALTER TABLE landing.extractions ENABLE ROW LEVEL SECURITY;
        CREATE POLICY tenant_isolation ON landing.extractions
            USING (tenant_id = current_setting('app.tenant_id', true));
    """)


def _create_dim_misses() -> None:
    op.execute("""
        CREATE TABLE landing.dim_misses (
            id            BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            tenant_id     TEXT         NOT NULL,
            job_id        UUID         NOT NULL,
            dim_name      TEXT         NOT NULL,
            missing_code  TEXT         NOT NULL,
            row_payload   JSONB        NOT NULL,
            detected_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        );
        CREATE INDEX ix_dim_misses_job ON landing.dim_misses (job_id);
    """)


def _recreate_v1_extractions_for_downgrade() -> None:
    op.execute(r"""
        CREATE TABLE landing.extractions (
            id                UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
            job_id            UUID          NOT NULL,
            tenant_id         CHAR(6)       NOT NULL,
            fonte_sistema     TEXT          NOT NULL,
            tipo_extracao     TEXT          NOT NULL,
            competencia       INT4          NOT NULL,
            minio_key         TEXT,
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
            CONSTRAINT uniq_source_comp UNIQUE (
                fonte_sistema, tenant_id, competencia, tipo_extracao
            ),
            CONSTRAINT extractions_source_type_check CHECK (fonte_sistema IN (
                'CNES_LOCAL', 'CNES_NACIONAL', 'SIHD'
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
        CREATE INDEX ix_extractions_tenant_comp
            ON landing.extractions (tenant_id, competencia);
        CREATE INDEX ix_extractions_job ON landing.extractions (job_id);
        CREATE INDEX ix_extractions_status_ready
            ON landing.extractions (status) WHERE status = 'UPLOADED';
        CREATE INDEX ix_extractions_lease_expired
            ON landing.extractions (lease_until)
            WHERE status = 'PROCESSING';
        CREATE INDEX ix_extractions_pending
            ON landing.extractions (status, created_at)
            WHERE status = 'PENDING';
        ALTER TABLE landing.extractions ENABLE ROW LEVEL SECURITY;
        CREATE POLICY tenant_isolation ON landing.extractions
            USING (tenant_id = current_setting('app.tenant_id', true));
    """)


def upgrade() -> None:  # pragma: no cover - alembic migration
    _drop_v1_extractions()
    _create_v2_extractions()
    _create_dim_misses()


def downgrade() -> None:  # pragma: no cover - alembic migration
    op.execute("DROP TABLE IF EXISTS landing.dim_misses CASCADE")
    op.execute("DROP TABLE IF EXISTS landing.extractions CASCADE")
    _recreate_v1_extractions_for_downgrade()
