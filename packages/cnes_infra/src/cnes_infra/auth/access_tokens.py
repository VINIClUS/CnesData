"""AccessTokenStore: opaque single-use access tokens for /provision/cert.

In-memory; suitable for single-replica central_api during pilot. PROD swap
to Redis is a v2 concern.
"""
from __future__ import annotations

import asyncio
import secrets
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class AccessToken:
    token: str
    agent_id: str
    tenant_id: str


@dataclass
class _Entry:
    token: str
    agent_id: str
    tenant_id: str
    expires_at: float
    consumed: bool = False


class AccessTokenStore:
    """In-memory access-token store. Each token is single-use."""

    def __init__(self, now: Callable[[], float] | None = None) -> None:
        self._lock = asyncio.Lock()
        self._by_token: dict[str, _Entry] = {}
        self._now: Callable[[], float] = now or time.monotonic

    async def issue(self, *, tenant_id: str, ttl_seconds: int = 300) -> str:
        async with self._lock:
            token = secrets.token_urlsafe(32)
            agent_id = secrets.token_hex(16)
            self._by_token[token] = _Entry(
                token=token, agent_id=agent_id, tenant_id=tenant_id,
                expires_at=self._now() + ttl_seconds,
            )
            return token

    async def consume(self, token: str) -> AccessToken | None:
        async with self._lock:
            entry = self._by_token.get(token)
            if entry is None or entry.consumed:
                return None
            if entry.expires_at <= self._now():
                self._by_token.pop(token, None)
                return None
            entry.consumed = True
            self._by_token.pop(token, None)
            return AccessToken(
                token=entry.token, agent_id=entry.agent_id,
                tenant_id=entry.tenant_id,
            )
