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


@dataclass
class SourceStatus:
    fonte_sistema: str
    last_extracao_ts: datetime | None
    last_competencia: int | None
    lag_months: int | None
    row_count: int | None
    status: str
    last_machine_id: str | None


@dataclass
class RunRow:
    id: UUID
    extracao_ts: datetime
    fonte_sistema: str
    competencia: int
    row_count: int
    sha256: str
    machine_id: str | None


_ALL_SOURCES = (
    "CNES_LOCAL", "CNES_NACIONAL", "SIHD", "BPA_MAG", "SIA_LOCAL",
)


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

_AGENT_STATUS_SQL = text("""
    SELECT source_type AS fonte_sistema,
           MAX(COALESCE(registered_at, created_at)) AS last_ts,
           MAX(EXTRACT(YEAR FROM competencia)::INT4 * 100
               + EXTRACT(MONTH FROM competencia)::INT4) AS last_comp,
           SUM(
               (SELECT COALESCE(SUM((f->>'row_count')::BIGINT), 0)
                FROM jsonb_array_elements(files) AS f)
           )::BIGINT AS rows
    FROM landing.extractions
    WHERE tenant_id = :t
    GROUP BY source_type
""")

_RECENT_RUNS_SQL = text("""
    SELECT job_id AS id,
           COALESCE(registered_at, created_at) AS extracao_ts,
           source_type AS fonte_sistema,
           EXTRACT(YEAR FROM competencia)::INT4 * 100
               + EXTRACT(MONTH FROM competencia)::INT4 AS competencia,
           (SELECT COALESCE(SUM((f->>'row_count')::BIGINT), 0)
            FROM jsonb_array_elements(files) AS f)::BIGINT AS row_count,
           COALESCE(files->0->>'sha256', '') AS sha256
    FROM landing.extractions
    WHERE tenant_id = :t
    ORDER BY COALESCE(registered_at, created_at) DESC
    LIMIT :n
""")


def _competencia_lag(target: int, latest: int | None) -> int | None:
    if latest is None:
        return None
    ty, tm = divmod(target, 100)
    ly, lm = divmod(latest, 100)
    return (ty - ly) * 12 + (tm - lm)


def _classify_status(lag: int | None) -> str:
    if lag is None:
        return "no_data"
    if lag <= 0:
        return "ok"
    if lag <= 1:
        return "warning"
    return "error"


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

    def agent_status(
        self, *, tenant_id: str, current_competencia: int,
    ) -> list[SourceStatus]:
        with self._engine.connect() as conn:
            rows = conn.execute(
                _AGENT_STATUS_SQL, {"t": tenant_id},
            ).mappings().all()
        seen = {r["fonte_sistema"]: r for r in rows}
        return [
            self._build_source_status(src, seen.get(src), current_competencia)
            for src in _ALL_SOURCES
        ]

    def _build_source_status(
        self, src: str, row: dict | None, current_competencia: int,
    ) -> SourceStatus:
        if row is None:
            return SourceStatus(
                fonte_sistema=src,
                last_extracao_ts=None,
                last_competencia=None,
                lag_months=None,
                row_count=None,
                status="no_data",
                last_machine_id=None,
            )
        lag = _competencia_lag(current_competencia, row["last_comp"])
        return SourceStatus(
            fonte_sistema=src,
            last_extracao_ts=row["last_ts"],
            last_competencia=row["last_comp"],
            lag_months=lag,
            row_count=int(row["rows"]) if row["rows"] is not None else None,
            status=_classify_status(lag),
            last_machine_id=None,
        )

    def recent_runs(
        self, *, tenant_id: str, limit: int,
    ) -> list[RunRow]:
        with self._engine.connect() as conn:
            rows = conn.execute(
                _RECENT_RUNS_SQL, {"t": tenant_id, "n": limit},
            ).mappings().all()
        return [
            RunRow(
                id=r["id"],
                extracao_ts=r["extracao_ts"],
                fonte_sistema=r["fonte_sistema"],
                competencia=r["competencia"],
                row_count=int(r["row_count"]),
                sha256=r["sha256"],
                machine_id=None,
            )
            for r in rows
        ]
