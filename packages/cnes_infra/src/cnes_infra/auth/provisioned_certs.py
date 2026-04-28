"""ProvisionedCertsRepo: append-only audit log + active-cert lookup.

Writes one row per /provision/cert call. RLS-isolated by tenant_id.
Append-only on write; SELECT helper for /provision/cert/rotate.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    import datetime as dt

    from sqlalchemy import Engine

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProvisionedCertRow:
    agent_id: str
    tenant_id: str
    subject_cn: str
    ca_serial: str
    issued_at: dt.datetime
    expires_at: dt.datetime
    revoked_at: dt.datetime | None


class ProvisionedCertsRepo:
    """Postgres-backed audit log of issued leaf certs."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def record(
        self,
        *,
        agent_id: str,
        tenant_id: str,
        subject_cn: str,
        ca_serial: str,
        expires_at: dt.datetime,
    ) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO auth_provisioned_certs "
                    "(agent_id, tenant_id, subject_cn, ca_serial, expires_at) "
                    "VALUES (:a, :t, :cn, :s, :e)",
                ),
                {
                    "a": agent_id, "t": tenant_id,
                    "cn": subject_cn, "s": ca_serial, "e": expires_at,
                },
            )
        logger.info(
            "provisioned_cert_recorded agent_id=%s tenant_id=%s ca_serial=%s",
            agent_id, tenant_id, ca_serial,
        )

    def find_active_by_agent_id(
        self, agent_id: str,
    ) -> ProvisionedCertRow | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT agent_id, tenant_id, subject_cn, ca_serial, "
                    "issued_at, expires_at, revoked_at "
                    "FROM auth_provisioned_certs "
                    "WHERE agent_id = :a AND revoked_at IS NULL "
                    "ORDER BY issued_at DESC LIMIT 1",
                ),
                {"a": agent_id},
            ).one_or_none()
        if row is None:
            return None
        return ProvisionedCertRow(
            agent_id=row.agent_id, tenant_id=row.tenant_id,
            subject_cn=row.subject_cn, ca_serial=row.ca_serial,
            issued_at=row.issued_at, expires_at=row.expires_at,
            revoked_at=row.revoked_at,
        )
