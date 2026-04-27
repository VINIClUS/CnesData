"""DeviceCodeStore: in-memory device code store with TTL.

For PROD, swap with Redis-backed implementation. v1 is in-memory; suitable
for single central_api instance during pilot.
"""
from __future__ import annotations

import asyncio
import secrets
import string
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Callable

_USER_CODE_ALPHABET = string.ascii_uppercase + string.digits
_USER_CODE_LEN = 8


@dataclass(frozen=True)
class DeviceAuthorization:
    device_code: str
    user_code: str
    expires_at: float


@dataclass(frozen=True)
class DeviceCodeStatus:
    kind: Literal[
        "authorization_pending", "authorized",
        "expired_token", "denied", "slow_down",
    ]
    tenant_id: str | None = None
    interval: int | None = None


@dataclass
class _Entry:
    device_code: str
    user_code: str
    client_id: str
    scope: str
    expires_at: float
    tenant_id: str | None = None
    consumed: bool = False
    last_polled_at: float | None = None
    current_interval: int = 5


def _generate_user_code() -> str:
    raw = "".join(secrets.choice(_USER_CODE_ALPHABET) for _ in range(_USER_CODE_LEN))
    return f"{raw[:4]}-{raw[4:]}"


class DeviceCodeStore:
    """In-memory device code store with TTL + single-use semantics."""

    def __init__(self, now: Callable[[], float] | None = None) -> None:
        self._lock = asyncio.Lock()
        self._by_device: dict[str, _Entry] = {}
        self._by_user: dict[str, str] = {}
        self._now: Callable[[], float] = now or time.monotonic

    async def issue(
        self, *, client_id: str, scope: str, ttl_seconds: int = 600,
    ) -> DeviceAuthorization:
        async with self._lock:
            for _ in range(8):
                user_code = _generate_user_code()
                if user_code not in self._by_user:
                    break
            else:
                raise RuntimeError("user_code_collision")
            device_code = secrets.token_urlsafe(32)
            expires_at = self._now() + ttl_seconds
            entry = _Entry(
                device_code=device_code, user_code=user_code,
                client_id=client_id, scope=scope, expires_at=expires_at,
            )
            self._by_device[device_code] = entry
            self._by_user[user_code] = device_code
            return DeviceAuthorization(
                device_code=device_code, user_code=user_code,
                expires_at=expires_at,
            )

    async def redeem_user_code(self, user_code: str, *, tenant_id: str) -> bool:
        async with self._lock:
            device_code = self._by_user.get(user_code)
            if device_code is None:
                return False
            entry = self._by_device.get(device_code)
            if entry is None or entry.consumed or entry.tenant_id is not None:
                return False
            if entry.expires_at <= self._now():
                return False
            entry.tenant_id = tenant_id
            return True

    async def poll_device_code(self, device_code: str) -> DeviceCodeStatus:
        async with self._lock:
            entry = self._by_device.get(device_code)
            if entry is None or entry.consumed:
                return DeviceCodeStatus(kind="expired_token")
            if entry.expires_at <= self._now():
                self._evict(entry)
                return DeviceCodeStatus(kind="expired_token")
            now = self._now()
            if entry.last_polled_at is not None and \
                    now - entry.last_polled_at < entry.current_interval:
                entry.current_interval = min(60, entry.current_interval * 2)
                return DeviceCodeStatus(
                    kind="slow_down", interval=entry.current_interval,
                )
            entry.last_polled_at = now
            if entry.tenant_id is None:
                return DeviceCodeStatus(kind="authorization_pending")
            entry.consumed = True
            self._evict(entry)
            return DeviceCodeStatus(kind="authorized", tenant_id=entry.tenant_id)

    def _evict(self, entry: _Entry) -> None:
        self._by_user.pop(entry.user_code, None)
