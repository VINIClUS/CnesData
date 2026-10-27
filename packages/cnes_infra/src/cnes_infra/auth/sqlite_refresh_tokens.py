"""Persistência e validação de refresh no SQLite."""

from collections.abc import Callable
from contextlib import closing
from datetime import UTC, datetime
from hashlib import sha256
from sqlite3 import LEGACY_TRANSACTION_CONTROL, Connection

from cnes_domain.refresh_tokens import (
    RefreshTokenConflict,
    RefreshTokenContext,
    RefreshTokenInput,
    RefreshTokenMutation,
    RefreshTokenRecord,
    RefreshTokenUnavailable,
    validate_refresh_token,
    validate_refresh_token_context,
    validate_refresh_token_mutation,
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
_MARK_USED = (
    "UPDATE auth_refresh_tokens SET last_used_at = ?, version = version + 1 "
    "WHERE tenant_id = ? AND token_sha256 = ? AND version = ? AND revoked_at IS NULL"
)
_REVOKE = (
    "UPDATE auth_refresh_tokens SET revoked_at = ?, version = version + 1 "
    "WHERE tenant_id = ? AND token_sha256 = ? AND version = ? AND revoked_at IS NULL"
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

    def mark_used(self, token: str, mutation: RefreshTokenMutation) -> bool:
        """Returns: uso confirmado por CAS. Raises: Invalid, Unavailable."""
        return self._mutate(token, mutation, False)

    def revoke(self, token: str, mutation: RefreshTokenMutation) -> bool:
        """Returns: revogação confirmada por CAS. Raises: Invalid, Unavailable."""
        return self._mutate(token, mutation, True)

    def _mutate(self, token: str, mutation: RefreshTokenMutation, revoking: bool) -> bool:
        validate_refresh_token_mutation(token, mutation)
        try:
            digest = sha256(token.encode()).hexdigest()
            with closing(self._connection_factory()) as connection:
                return _apply_mutation(connection, mutation, digest, revoking)
        except Exception:
            raise RefreshTokenUnavailable("operation=mutate_refresh") from None

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


def _apply_mutation(
    connection: Connection, mutation: RefreshTokenMutation, digest: str, revoking: bool,
) -> bool:
    if connection.autocommit != LEGACY_TRANSACTION_CONTROL or connection.in_transaction:
        raise RefreshTokenUnavailable("operation=refresh_connection")
    try:
        connection.execute("BEGIN IMMEDIATE")
        changed = _write_mutation(connection, mutation, digest, revoking)
        if changed:
            connection.commit()
        else:
            connection.rollback()
        return changed
    except Exception:
        connection.rollback()
        raise


def _write_mutation(
    connection: Connection, mutation: RefreshTokenMutation, digest: str, revoking: bool,
) -> bool:
    row = connection.execute(_SELECT, (mutation.tenant_id, digest)).fetchone()
    if row is None:
        return False
    record = _decode(row)
    validate_refresh_token(record)
    if not _eligible_mutation(record, mutation, revoking):
        return False
    if record.version == 9223372036854775807:
        raise RefreshTokenUnavailable("operation=refresh_version")
    statement = _REVOKE if revoking else _MARK_USED
    parameters = (_timestamp(mutation.now), mutation.tenant_id, digest, mutation.expected_version)
    if connection.execute(statement, parameters).rowcount != 1:
        raise RefreshTokenUnavailable("operation=refresh_update")
    return True


def _eligible_mutation(
    record: RefreshTokenRecord, mutation: RefreshTokenMutation, revoking: bool,
) -> bool:
    if record.version != mutation.expected_version or record.revoked_at is not None:
        return False
    if not record.issued_at <= mutation.now < record.expires_at:
        return False
    return revoking or record.last_used_at is None or mutation.now >= record.last_used_at
