"""Persistência SQLite de credenciais locais e sessões opacas."""

from __future__ import annotations

import hashlib
import secrets
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

SCRYPT_N = 2**14
SCRYPT_R = 8
SCRYPT_P = 1
SCRYPT_DKLEN = 32
SALT_BYTES = 16
SESSION_TOKEN_BYTES = 32
MIN_PASSWORD_LENGTH = 12
MAX_PASSWORD_LENGTH = 128

_SCHEMA = """
CREATE TABLE IF NOT EXISTS local_users (
    user_id TEXT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash BLOB NOT NULL,
    salt BLOB NOT NULL,
    created_at TEXT NOT NULL,
    disabled_at TEXT
);
CREATE TABLE IF NOT EXISTS local_sessions (
    session_hash TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_local_sessions_expires_at ON local_sessions (expires_at);
"""


class CredentialRejected(Exception):
    """Levantada quando um valor de entrada de credencial é inválido."""


def normalize_email(email: str) -> str:
    return email.strip().casefold()


def generate_salt() -> bytes:
    return secrets.token_bytes(SALT_BYTES)


def hash_password(password: str, salt: bytes) -> bytes:
    return hashlib.scrypt(
        password.encode(),
        salt=salt,
        n=SCRYPT_N,
        r=SCRYPT_R,
        p=SCRYPT_P,
        dklen=SCRYPT_DKLEN,
    )


def generate_session_token() -> str:
    return secrets.token_urlsafe(SESSION_TOKEN_BYTES)


def hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class LocalUserRecord:
    user_id: str
    email: str
    password_hash: bytes
    salt: bytes
    created_at: datetime
    disabled_at: datetime | None


@dataclass(frozen=True, slots=True)
class SessionRecord:
    session_hash: str
    user_id: str
    tenant_id: str
    expires_at: datetime


def build_user(user_id: str, email: str, password: str, created_at: datetime) -> LocalUserRecord:
    """Args: user_id, email, password em texto puro, created_at.
    Returns: Registro pronto para persistir, com senha já derivada.
    Raises: CredentialRejected: Quando a senha está fora de 12-128 caracteres.
    """
    if not (MIN_PASSWORD_LENGTH <= len(password) <= MAX_PASSWORD_LENGTH):
        raise CredentialRejected("password_length")
    salt = generate_salt()
    return LocalUserRecord(
        user_id=user_id,
        email=normalize_email(email),
        password_hash=hash_password(password, salt),
        salt=salt,
        created_at=created_at,
        disabled_at=None,
    )


def _parse_optional(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value)


def _row_to_user(row: tuple) -> LocalUserRecord:
    user_id, email, password_hash, salt, created_at, disabled_at = row
    return LocalUserRecord(
        user_id=user_id,
        email=email,
        password_hash=password_hash,
        salt=salt,
        created_at=datetime.fromisoformat(created_at),
        disabled_at=_parse_optional(disabled_at),
    )


class LocalCredentialStore:
    """Guarda usuários locais e sessões opacas no mesmo SQLite do control plane."""

    def __init__(self, database_path: Path) -> None:
        self._database_path = database_path

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._database_path, timeout=5.0, isolation_level=None)
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def initialize(self) -> None:
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._open() as connection:
            connection.executescript(_SCHEMA)
            columns = {
                row[1] for row in connection.execute("PRAGMA table_info(local_sessions)")
            }
            if "tenant_id" not in columns:
                connection.execute(
                    "ALTER TABLE local_sessions ADD COLUMN tenant_id TEXT NOT NULL DEFAULT ''"
                )

    @contextmanager
    def _open(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    def put_user(self, record: LocalUserRecord) -> None:
        with self._open() as connection:
            connection.execute(
                "INSERT INTO local_users "
                "(user_id, email, password_hash, salt, created_at, disabled_at) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (user_id) DO UPDATE SET "
                "email = excluded.email, password_hash = excluded.password_hash, "
                "salt = excluded.salt, disabled_at = excluded.disabled_at",
                (
                    record.user_id,
                    record.email,
                    record.password_hash,
                    record.salt,
                    record.created_at.isoformat(),
                    None if record.disabled_at is None else record.disabled_at.isoformat(),
                ),
            )

    def find_user_by_email(self, email: str) -> LocalUserRecord | None:
        with self._open() as connection:
            row = connection.execute(
                "SELECT user_id, email, password_hash, salt, created_at, disabled_at "
                "FROM local_users WHERE email = ?",
                (normalize_email(email),),
            ).fetchone()
        return None if row is None else _row_to_user(row)

    def find_user_by_id(self, user_id: str) -> LocalUserRecord | None:
        with self._open() as connection:
            row = connection.execute(
                "SELECT user_id, email, password_hash, salt, created_at, disabled_at "
                "FROM local_users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
        return None if row is None else _row_to_user(row)

    def put_session(self, record: SessionRecord, now: datetime) -> None:
        with self._open() as connection:
            connection.execute(
                "DELETE FROM local_sessions WHERE expires_at <= ?", (now.isoformat(),)
            )
            connection.execute(
                "INSERT INTO local_sessions "
                "(session_hash, user_id, tenant_id, expires_at) VALUES (?, ?, ?, ?)",
                (
                    record.session_hash,
                    record.user_id,
                    record.tenant_id,
                    record.expires_at.isoformat(),
                ),
            )

    def find_session(self, session_hash: str) -> SessionRecord | None:
        with self._open() as connection:
            row = connection.execute(
                "SELECT session_hash, user_id, tenant_id, expires_at FROM local_sessions "
                "WHERE session_hash = ?",
                (session_hash,),
            ).fetchone()
        if row is None:
            return None
        session_hash_value, user_id, tenant_id, expires_at = row
        return SessionRecord(
            session_hash=session_hash_value,
            user_id=user_id,
            tenant_id=tenant_id,
            expires_at=datetime.fromisoformat(expires_at),
        )

    def delete_session(self, session_hash: str) -> None:
        with self._open() as connection:
            connection.execute("DELETE FROM local_sessions WHERE session_hash = ?", (session_hash,))


__all__ = [
    "MAX_PASSWORD_LENGTH",
    "MIN_PASSWORD_LENGTH",
    "SALT_BYTES",
    "SCRYPT_DKLEN",
    "SCRYPT_N",
    "SCRYPT_P",
    "SCRYPT_R",
    "SESSION_TOKEN_BYTES",
    "CredentialRejected",
    "LocalCredentialStore",
    "LocalUserRecord",
    "SessionRecord",
    "build_user",
    "generate_salt",
    "generate_session_token",
    "hash_password",
    "hash_session_token",
    "normalize_email",
]
