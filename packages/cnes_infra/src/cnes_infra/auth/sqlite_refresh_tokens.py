"""Criação e importação transacional de refresh no SQLite."""

from collections.abc import Callable
from contextlib import closing
from datetime import UTC, datetime
from hashlib import sha256
from sqlite3 import Connection

from cnes_domain.refresh_tokens import (
    RefreshTokenConflict,
    RefreshTokenInput,
    RefreshTokenRecord,
    RefreshTokenUnavailable,
    validate_refresh_token,
)

_INSERT = (
    "INSERT INTO auth_refresh_tokens "
    "(token_sha256, agent_id, tenant_id, machine_fingerprint, issued_at, expires_at, "
    "last_used_at, revoked_at, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
    "ON CONFLICT (tenant_id, token_sha256) DO NOTHING"
)


class SQLiteRefreshTokenStore:
    def __init__(
        self, connection_factory: Callable[[], Connection], token_factory: Callable[[], str],
    ) -> None:
        self._connection_factory = connection_factory
        self._token_factory = token_factory

    def create(self, token_input: RefreshTokenInput) -> str:
        """Returns: segredo criado. Raises: Invalid, Conflict, Unavailable."""
        validate_refresh_token(token_input)
        token, digest = self._create_secret()
        record = RefreshTokenRecord(
            token_sha256=digest,
            agent_id=token_input.agent_id,
            tenant_id=token_input.tenant_id,
            machine_fingerprint=token_input.machine_fingerprint,
            issued_at=token_input.issued_at,
            expires_at=token_input.expires_at,
            last_used_at=None,
            revoked_at=None,
            version=0,
        )
        self._insert(record)
        return token

    def import_hashed(self, record: RefreshTokenRecord) -> None:
        """Raises: Invalid, Conflict, Unavailable para importação recusada."""
        validate_refresh_token(record)
        self._insert(record)

    def _create_secret(self) -> tuple[str, str]:
        try:
            token = self._token_factory()
            return token, sha256(token.encode()).hexdigest()
        except Exception:
            raise RefreshTokenUnavailable("operation=create_secret") from None

    def _insert(self, record: RefreshTokenRecord) -> None:
        try:
            with closing(self._connection_factory()) as connection, connection:
                connection.autocommit = False
                inserted = connection.execute(_INSERT, _parameters(record)).rowcount
                if inserted == 0:
                    connection.rollback()
        except Exception:
            raise RefreshTokenUnavailable("operation=insert_refresh") from None
        if inserted == 0:
            raise RefreshTokenConflict("operation=insert_refresh") from None


def _timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _parameters(record: RefreshTokenRecord) -> tuple[object, ...]:
    return (
        record.token_sha256,
        record.agent_id,
        record.tenant_id,
        record.machine_fingerprint,
        _timestamp(record.issued_at),
        _timestamp(record.expires_at),
        _timestamp(record.last_used_at),
        _timestamp(record.revoked_at),
        record.version,
    )
