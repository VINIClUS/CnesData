"""dashboard schema: users + user_tenants + audit_log + RLS

Revision ID: 015_dashboard_users
Revises: 014_auth_tables
Create Date: 2026-04-25 00:00:00
"""
from alembic import op

revision = "015_dashboard_users"
down_revision = "014_auth_tables"
branch_labels = None
depends_on = None


def _create_users() -> None:  # pragma: no cover - alembic migration
    op.execute("""
        CREATE TABLE dashboard.users (
            id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
            oidc_subject    TEXT         NOT NULL,
            oidc_issuer     TEXT         NOT NULL,
            email           TEXT         NOT NULL,
            display_name    TEXT,
            role            TEXT         NOT NULL DEFAULT 'gestor',
            created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            last_login_at   TIMESTAMPTZ,
            revoked_at      TIMESTAMPTZ,
            UNIQUE (oidc_issuer, oidc_subject),
            CONSTRAINT chk_role CHECK (role IN ('gestor', 'admin'))
        )
    """)
    op.execute("CREATE INDEX ix_users_email ON dashboard.users (email)")


def _create_user_tenants() -> None:  # pragma: no cover - alembic migration
    op.execute("""
        CREATE TABLE dashboard.user_tenants (
            user_id     UUID    NOT NULL REFERENCES dashboard.users(id) ON DELETE CASCADE,
            tenant_id   CHAR(6) NOT NULL,
            PRIMARY KEY (user_id, tenant_id)
        )
    """)
    op.execute(
        "CREATE INDEX ix_user_tenants_tenant ON dashboard.user_tenants (tenant_id)"
    )


def _create_audit_log() -> None:  # pragma: no cover - alembic migration
    op.execute("""
        CREATE TABLE dashboard.audit_log (
            id           BIGSERIAL    PRIMARY KEY,
            user_id      UUID         REFERENCES dashboard.users(id),
            tenant_id    CHAR(6),
            action       TEXT         NOT NULL,
            metadata     JSONB,
            request_id   UUID,
            timestamp    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            CONSTRAINT chk_action CHECK (action IN (
                'login', 'logout', 'activate_agent',
                'view_status', 'view_runs', 'view_tenants'
            ))
        )
    """)
    op.execute(
        "CREATE INDEX ix_audit_user_ts "
        "ON dashboard.audit_log (user_id, timestamp DESC)"
    )
    op.execute(
        "CREATE INDEX ix_audit_tenant_action "
        "ON dashboard.audit_log (tenant_id, action, timestamp DESC)"
    )


def _apply_rls_policies() -> None:  # pragma: no cover - alembic migration
    op.execute("ALTER TABLE dashboard.user_tenants ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE dashboard.user_tenants FORCE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE dashboard.audit_log ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE dashboard.audit_log FORCE ROW LEVEL SECURITY")
    op.execute("""
        CREATE POLICY user_tenants_isolation ON dashboard.user_tenants
            USING (tenant_id = current_setting('app.tenant_id', true))
    """)
    op.execute("""
        CREATE POLICY audit_log_isolation ON dashboard.audit_log
            USING (tenant_id IS NULL
                   OR tenant_id = current_setting('app.tenant_id', true))
    """)


def upgrade() -> None:  # pragma: no cover - alembic migration
    op.execute("CREATE SCHEMA IF NOT EXISTS dashboard")
    _create_users()
    _create_user_tenants()
    _create_audit_log()
    _apply_rls_policies()


def downgrade() -> None:  # pragma: no cover - alembic migration
    op.execute(
        "DROP POLICY IF EXISTS audit_log_isolation ON dashboard.audit_log"
    )
    op.execute(
        "DROP POLICY IF EXISTS user_tenants_isolation ON dashboard.user_tenants"
    )
    op.execute("DROP TABLE IF EXISTS dashboard.audit_log CASCADE")
    op.execute("DROP TABLE IF EXISTS dashboard.user_tenants CASCADE")
    op.execute("DROP TABLE IF EXISTS dashboard.users CASCADE")
    op.execute("DROP SCHEMA IF EXISTS dashboard CASCADE")
