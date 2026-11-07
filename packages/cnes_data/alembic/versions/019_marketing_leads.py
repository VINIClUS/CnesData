"""Create marketing.leads for the public contact form (POST /api/v1/public/leads).

Revision ID: 019_marketing_leads
Revises: 018_sha256_landing
Create Date: 2026-09-18
"""
from __future__ import annotations

from alembic import op

revision = "019_marketing_leads"
down_revision = "018_sha256_landing"
branch_labels = None
depends_on = None


def upgrade() -> None:  # pragma: no cover - alembic migration
    op.execute("CREATE SCHEMA IF NOT EXISTS marketing")
    op.execute(
        """
        CREATE TABLE marketing.leads (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            name VARCHAR(120) NOT NULL,
            email VARCHAR(254) NOT NULL,
            interest VARCHAR(20) NOT NULL,
            organization VARCHAR(200) NOT NULL DEFAULT '',
            municipality VARCHAR(120) NOT NULL DEFAULT '',
            role VARCHAR(120) NOT NULL DEFAULT '',
            message VARCHAR(2000) NOT NULL DEFAULT '',
            source_path VARCHAR(200) NOT NULL DEFAULT '',
            source_cta VARCHAR(50) NOT NULL DEFAULT '',
            privacy_notice_version VARCHAR(10) NOT NULL,
            newsletter_opt_in BOOLEAN NOT NULL DEFAULT FALSE,
            CONSTRAINT chk_leads_interest
                CHECK (interest IN ('early_access', 'pilot', 'contact')),
            CONSTRAINT chk_leads_name_len CHECK (char_length(name) BETWEEN 1 AND 120),
            CONSTRAINT chk_leads_email_len CHECK (char_length(email) BETWEEN 3 AND 254)
        )
        """
    )
    op.execute(
        "CREATE INDEX ix_marketing_leads_email_created "
        "ON marketing.leads (lower(email), created_at DESC)"
    )
    op.execute("CREATE INDEX ix_marketing_leads_created ON marketing.leads (created_at DESC)")


def downgrade() -> None:  # pragma: no cover - alembic migration
    op.execute("DROP TABLE IF EXISTS marketing.leads CASCADE")
    op.execute("DROP SCHEMA IF EXISTS marketing")
