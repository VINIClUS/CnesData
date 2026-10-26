"""Valores e erros públicos para persistência de refresh."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from re import DOTALL, fullmatch
from typing import cast


@dataclass(frozen=True, slots=True)
class RefreshTokenInput:
    agent_id: str
    tenant_id: str
    machine_fingerprint: str
    issued_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class RefreshTokenContext:
    tenant_id: str
    now: datetime


@dataclass(frozen=True, slots=True)
class RefreshTokenRecord:
    token_sha256: str
    agent_id: str
    tenant_id: str
    machine_fingerprint: str
    issued_at: datetime
    expires_at: datetime
    last_used_at: datetime | None
    revoked_at: datetime | None
    version: int


class RefreshTokenInvalid(ValueError):
    pass


class RefreshTokenConflict(Exception):
    pass


class RefreshTokenUnavailable(Exception):
    pass


def validate_refresh_token(value: RefreshTokenInput | RefreshTokenRecord) -> None:
    """Raises: RefreshTokenInvalid para valores fora do contrato."""
    try:
        _validate_common(value)
        if isinstance(value, RefreshTokenRecord):
            _validate_record(value)
    except (AttributeError, TypeError, ValueError):
        raise RefreshTokenInvalid("field=refresh_token") from None


def validate_refresh_token_context(token: str, context: RefreshTokenContext) -> None:
    """Raises: RefreshTokenInvalid para segredo ou contexto inválido."""
    try:
        _require(isinstance(cast("object", token), str) and bool(token))
        _require(bool(fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", context.tenant_id)))
        _require(isinstance(cast("object", context.now), datetime))
        _require(context.now.utcoffset() == timedelta(0))
    except (AttributeError, TypeError, ValueError):
        raise RefreshTokenInvalid("field=refresh_context") from None


def _require(valid: bool) -> None:
    if not valid:
        raise ValueError("field=refresh_token")


def _validate_common(value: RefreshTokenInput | RefreshTokenRecord) -> None:
    for identifier in (value.agent_id, value.tenant_id):
        _require(bool(fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", identifier)))
    _require(bool(fullmatch(r".+", value.machine_fingerprint, flags=DOTALL)))
    _require(value.issued_at.utcoffset() == timedelta(0))
    _require(value.expires_at.utcoffset() == timedelta(0))
    _require(value.expires_at > value.issued_at)


def _validate_record(record: RefreshTokenRecord) -> None:
    _require(bool(fullmatch(r"[0-9a-f]{64}", record.token_sha256)))
    _require(type(record.version) is int and record.version >= 0)
    for timestamp in (record.last_used_at, record.revoked_at):
        if timestamp is not None:
            _require(timestamp.utcoffset() == timedelta(0))
            _require(timestamp >= record.issued_at)
    if record.last_used_at is not None:
        _require(record.last_used_at <= record.expires_at)
