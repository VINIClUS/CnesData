"""Persistência e validação de refresh no SQLite."""

from collections.abc import Callable
from contextlib import closing
from datetime import UTC, datetime
from hashlib import sha256
from sqlite3 import Connection

from cnes_domain.refresh_tokens import (
    RefreshTokenConflict,
    RefreshTokenContext,
    RefreshTokenInput,
    RefreshTokenRecord,
    RefreshTokenUnavailable,
    validate_refresh_token,
    validate_refresh_token_context,
)

_INSERT = (
    "INSERT INTO auth_refresh_tokens "
    "(token_sha256, agent_id, tenant_id, machine_fingerprint, issued_at, expires_at, "
    "last_used_at, revoked_at, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
    "ON CONFLICT (tenant_id, token_sha256) DO NOTHING"
)
_IDENTITY = "SELECT 1 FROM auth_refresh_tokens WHERE tenant_id = ? AND token_sha256 = ?"
_SELECT = (
    "SELECT token_sha256, agent_id, tenant_id, machine_fingerprint, issued_at, expires_at, "
    "last_used_at, revoked_at, version FROM auth_refresh_tokens "
    "WHERE tenant_id = ? AND token_sha256 = ?"
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

    def validate(self, token: str, context: RefreshTokenContext) -> RefreshTokenRecord | None:
        """Returns: registro ativo ou None. Raises: Invalid, Unavailable."""
        validate_refresh_token_context(token, context)
        try:
            digest = sha256(token.encode()).hexdigest()
            with closing(self._connection_factory()) as connection:
                row = connection.execute(_SELECT, (context.tenant_id, digest)).fetchone()
                if row is None:
                    return None
                record = _decode(row)
                validate_refresh_token(record)
        except Exception:
            raise RefreshTokenUnavailable("operation=validate_refresh") from None
        if record.revoked_at is not None or not record.issued_at <= context.now < record.expires_at:
            return None
        return record

    def _create_secret(self) -> tuple[str, str]:
        try:
            token = self._token_factory()
            return token, sha256(token.encode()).hexdigest()
        except Exception:
            raise RefreshTokenUnavailable("operation=create_secret") from None

    def _insert(self, record: RefreshTokenRecord) -> None:
        occupied = False
        try:
            with closing(self._connection_factory()) as connection, connection:
                connection.autocommit = False
                inserted = connection.execute(_INSERT, _parameters(record)).rowcount
                if inserted == 0:
                    occupied = connection.execute(
                        _IDENTITY, (record.tenant_id, record.token_sha256),
                    ).fetchone() is not None
                    connection.rollback()
        except Exception:
            raise RefreshTokenUnavailable("operation=insert_refresh") from None
        if inserted == 0 and occupied:
            raise RefreshTokenConflict("operation=insert_refresh") from None
        if inserted == 0:
            raise RefreshTokenUnavailable("operation=insert_refresh") from None


def _timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _decode(
    row: tuple[str, str, str, str, str, str, str | None, str | None, int],
) -> RefreshTokenRecord:
    return RefreshTokenRecord(
        token_sha256=row[0], agent_id=row[1], tenant_id=row[2], machine_fingerprint=row[3],
        issued_at=datetime.fromisoformat(row[4]), expires_at=datetime.fromisoformat(row[5]),
        last_used_at=_parse_optional_timestamp(row[6]),
        revoked_at=_parse_optional_timestamp(row[7]), version=row[8],
    )


def _parse_optional_timestamp(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value)


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
