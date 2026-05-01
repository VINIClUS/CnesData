"""Migration 016 — dashboard.access_requests + chk_action extend."""
from __future__ import annotations

from alembic import op

revision = "016_access_requests"
down_revision = "015_dashboard_users"
branch_labels = None
depends_on = None


def upgrade() -> None:  # pragma: no cover - alembic migration
    _create_access_requests()
    _extend_audit_log_actions()


def _create_access_requests() -> None:  # pragma: no cover - alembic migration
    op.execute("""
        CREATE TABLE dashboard.access_requests (
            id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id         UUID         NOT NULL REFERENCES dashboard.users(id) ON DELETE CASCADE,
            tenant_id       CHAR(6)      NOT NULL,
            motivation      TEXT         NOT NULL,
            status          TEXT         NOT NULL DEFAULT 'pending',
            requested_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            reviewed_at     TIMESTAMPTZ,
            reviewed_by     UUID         REFERENCES dashboard.users(id),
            review_notes    TEXT,
            UNIQUE (user_id, tenant_id),
            CONSTRAINT chk_status CHECK (status IN ('pending', 'approved', 'rejected')),
            CONSTRAINT chk_motivation_len CHECK (length(motivation) BETWEEN 1 AND 500)
        )
    """)
    op.execute(
        "CREATE INDEX ix_access_requests_user "
        "ON dashboard.access_requests (user_id, requested_at DESC)"
    )
    op.execute(
        "CREATE INDEX ix_access_requests_pending "
        "ON dashboard.access_requests (status, requested_at DESC) "
        "WHERE status = 'pending'"
    )


def _extend_audit_log_actions() -> None:  # pragma: no cover - alembic migration
    op.execute("ALTER TABLE dashboard.audit_log DROP CONSTRAINT chk_action")
    op.execute("""
        ALTER TABLE dashboard.audit_log ADD CONSTRAINT chk_action CHECK (action IN (
            'login', 'logout', 'activate_agent',
            'view_status', 'view_runs', 'view_tenants',
            'request_access', 'approve_access', 'reject_access',
            'view_overview', 'view_faturamento'
        ))
    """)


def downgrade() -> None:  # pragma: no cover - alembic migration
    op.execute("ALTER TABLE dashboard.audit_log DROP CONSTRAINT chk_action")
    op.execute("""
        ALTER TABLE dashboard.audit_log ADD CONSTRAINT chk_action CHECK (action IN (
            'login', 'logout', 'activate_agent',
            'view_status', 'view_runs', 'view_tenants'
        ))
    """)
    op.execute("DROP TABLE IF EXISTS dashboard.access_requests CASCADE")
