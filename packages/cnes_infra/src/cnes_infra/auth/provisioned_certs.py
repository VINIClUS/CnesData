"""ProvisionedCertsRepo: append-only audit log + active-cert lookup.

Writes one row per /provision/cert call. RLS-isolated by tenant_id.
Append-only on write; serial lookup gates agent mTLS (rotation overlap allowed).
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    import datetime as dt

    from sqlalchemy import Engine

logger = logging.getLogger(__name__)


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

    def is_serial_active(self, agent_id: str, ca_serial: str) -> bool:
        """Returns: True se o serial do agente não foi revogado nem expirou."""
        with self._engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT 1 FROM auth_provisioned_certs "
                    "WHERE agent_id = :a AND ca_serial = :s "
                    "AND revoked_at IS NULL AND expires_at > now() "
                    "LIMIT 1",
                ),
                {"a": agent_id, "s": ca_serial},
            ).one_or_none()
        return row is not None
