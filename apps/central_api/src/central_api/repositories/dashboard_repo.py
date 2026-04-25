"""DashboardRepo — user upsert, tenant listing, audit logging."""

import json
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class DashboardUserRow:
    user_id: UUID
    email: str
    display_name: str | None
    role: str
    tenant_ids: list[str]
    last_login_at: datetime | None


@dataclass
class TenantRow:
    ibge6: str
    ibge7: str
    nome: str
    uf: str


_UPSERT_USER = text(
    """
    INSERT INTO dashboard.users (
        oidc_subject, oidc_issuer, email, display_name, last_login_at
    )
    VALUES (:sub, :iss, :email, :name, NOW())
    ON CONFLICT (oidc_issuer, oidc_subject)
    DO UPDATE SET email = EXCLUDED.email,
                  display_name = EXCLUDED.display_name,
                  last_login_at = NOW()
    RETURNING id, email, display_name, role, last_login_at
    """
)

_LOAD_TENANT_IDS = text(
    "SELECT tenant_id FROM dashboard.user_tenants WHERE user_id = :u "
    "ORDER BY tenant_id"
)

_LIST_TENANT_DETAIL = text(
    """
    SELECT m.ibge6, m.ibge7, m.nome, m.uf
    FROM dashboard.user_tenants ut
    JOIN gold.dim_municipio m ON m.ibge6 = ut.tenant_id
    WHERE ut.user_id = :u
    ORDER BY m.nome
    """
)

_INSERT_AUDIT = text(
    """
    INSERT INTO dashboard.audit_log (user_id, tenant_id, action, metadata)
    VALUES (:u, :t, :a, CAST(:m AS JSONB))
    """
)


class DashboardRepo:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def upsert_user(
        self,
        *,
        oidc_subject: str,
        oidc_issuer: str,
        email: str,
        display_name: str | None,
    ) -> DashboardUserRow:
        with self._engine.begin() as conn:
            row = conn.execute(
                _UPSERT_USER,
                {
                    "sub": oidc_subject,
                    "iss": oidc_issuer,
                    "email": email,
                    "name": display_name,
                },
            ).mappings().one()
            tenants = conn.execute(
                _LOAD_TENANT_IDS, {"u": row["id"]},
            ).scalars().all()
        return DashboardUserRow(
            user_id=row["id"],
            email=row["email"],
            display_name=row["display_name"],
            role=row["role"],
            tenant_ids=[t.rstrip() for t in tenants],
            last_login_at=row["last_login_at"],
        )

    def list_tenants(self, *, user_id: UUID) -> list[TenantRow]:
        with self._engine.connect() as conn:
            rows = conn.execute(
                _LIST_TENANT_DETAIL, {"u": user_id},
            ).mappings().all()
        return [
            TenantRow(
                ibge6=r["ibge6"].rstrip(),
                ibge7=r["ibge7"].rstrip(),
                nome=r["nome"],
                uf=r["uf"].rstrip(),
            )
            for r in rows
        ]

    def log_action(
        self,
        *,
        user_id: UUID,
        tenant_id: str | None,
        action: str,
        metadata: dict | None,
    ) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                _INSERT_AUDIT,
                {
                    "u": user_id,
                    "t": tenant_id,
                    "a": action,
                    "m": json.dumps(metadata) if metadata is not None else None,
                },
            )
