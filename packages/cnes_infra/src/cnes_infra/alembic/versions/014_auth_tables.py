"""auth: refresh tokens + provisioned certs

Revision ID: 014_auth_tables
Revises: 013_fato_producao_bpihst
Create Date: 2026-04-25 00:00:00
"""
from alembic import op
import sqlalchemy as sa


revision = "014_auth_tables"
down_revision = "013_fato_producao_bpihst"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "auth_refresh_tokens",
        sa.Column("token_sha256", sa.String(64), primary_key=True),
        sa.Column("agent_id", sa.String(64), nullable=False, index=True),
        sa.Column("tenant_id", sa.String(36), nullable=False, index=True),
        sa.Column("machine_fingerprint", sa.String(128), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_table(
        "auth_provisioned_certs",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("agent_id", sa.String(64), nullable=False, index=True),
        sa.Column("tenant_id", sa.String(36), nullable=False, index=True),
        sa.Column("subject_cn", sa.String(128), nullable=False),
        sa.Column("ca_serial", sa.String(64), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute("ALTER TABLE auth_refresh_tokens ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY auth_refresh_tokens_tenant_isolation "
        "ON auth_refresh_tokens FOR ALL "
        "USING (tenant_id = current_setting('app.tenant_id', true))"
    )
    op.execute("ALTER TABLE auth_provisioned_certs ENABLE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY auth_provisioned_certs_tenant_isolation "
        "ON auth_provisioned_certs FOR ALL "
        "USING (tenant_id = current_setting('app.tenant_id', true))"
    )


def downgrade() -> None:
    op.execute("DROP POLICY IF EXISTS auth_provisioned_certs_tenant_isolation ON auth_provisioned_certs")
    op.execute("DROP POLICY IF EXISTS auth_refresh_tokens_tenant_isolation ON auth_refresh_tokens")
    op.drop_table("auth_provisioned_certs")
    op.drop_table("auth_refresh_tokens")
