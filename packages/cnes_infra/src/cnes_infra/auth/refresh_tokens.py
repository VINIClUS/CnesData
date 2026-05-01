"""RefreshTokenStore: Postgres-backed long-lived token store.

Tokens are random 32-byte strings; only their SHA-256 hash is stored in the
DB (defense in depth — DB dump leak does not yield usable tokens).
"""
from __future__ import annotations

import hashlib
import logging
import secrets
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    import datetime as dt

    from sqlalchemy import Engine

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RefreshTokenRow:
    agent_id: str
    tenant_id: str
    machine_fingerprint: str
    issued_at: dt.datetime
    last_used_at: dt.datetime | None
    revoked_at: dt.datetime | None


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class RefreshTokenStore:
    """Postgres-backed refresh token store with RLS by tenant_id."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def create(
        self, *, agent_id: str, tenant_id: str, machine_fingerprint: str,
    ) -> str:
        token = secrets.token_urlsafe(32)
        token_hash = _hash(token)
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO auth_refresh_tokens "
                    "(token_sha256, agent_id, tenant_id, machine_fingerprint) "
                    "VALUES (:h, :a, :t, :f)",
                ),
                {"h": token_hash, "a": agent_id, "t": tenant_id, "f": machine_fingerprint},
            )
        logger.info(
            "refresh_token_created agent_id=%s tenant_id=%s",
            agent_id, tenant_id,
        )
        return token

    def validate(self, token: str, *, tenant_id: str) -> RefreshTokenRow | None:
        token_hash = _hash(token)
        with self._engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT agent_id, tenant_id, machine_fingerprint, "
                    "issued_at, last_used_at, revoked_at "
                    "FROM auth_refresh_tokens "
                    "WHERE token_sha256 = :h AND tenant_id = :t",
                ),
                {"h": token_hash, "t": tenant_id},
            ).one_or_none()
        if row is None:
            return None
        if row.revoked_at is not None:
            return None
        return RefreshTokenRow(
            agent_id=row.agent_id,
            tenant_id=row.tenant_id,
            machine_fingerprint=row.machine_fingerprint,
            issued_at=row.issued_at,
            last_used_at=row.last_used_at,
            revoked_at=row.revoked_at,
        )

    def mark_used(self, token: str) -> None:
        token_hash = _hash(token)
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE auth_refresh_tokens "
                    "SET last_used_at = now() "
                    "WHERE token_sha256 = :h",
                ),
                {"h": token_hash},
            )

    def revoke(self, token: str) -> None:
        token_hash = _hash(token)
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE auth_refresh_tokens "
                    "SET revoked_at = now() "
                    "WHERE token_sha256 = :h AND revoked_at IS NULL",
                ),
                {"h": token_hash},
            )
        logger.info("refresh_token_revoked")

    def has_active_for_agent(self, agent_id: str) -> bool:
        with self._engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT 1 FROM auth_refresh_tokens "
                    "WHERE agent_id = :a AND revoked_at IS NULL "
                    "LIMIT 1",
                ),
                {"a": agent_id},
            ).one_or_none()
        return row is not None
