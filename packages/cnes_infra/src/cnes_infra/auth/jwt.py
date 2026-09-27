"""JWKS-backed JWT validator. Fetches signing keys from issuer, caches by TTL."""
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

import httpx
from jose import jwt as jose_jwt
from jose.exceptions import (
    ExpiredSignatureError,
    JWKError,
    JWTClaimsError,
    JWTError,
)

type FetchStage = Literal["discovery", "jwks"]


class TokenInvalid(Exception):
    pass


@dataclass
class _JwksCache:
    fetch: Callable[[str], httpx.Response]
    discovery_url: str
    ttl_seconds: int
    resolve_jwks_uri: Callable[[dict[str, Any]], str]
    error: Callable[[FetchStage, httpx.HTTPError], TokenInvalid]
    _keys: list[dict[str, Any]] = field(default_factory=list, init=False, repr=False)
    _fetched_at: float = field(default=0.0, init=False, repr=False)
    _jwks_uri: str = field(default="", init=False, repr=False)

    def key_for_kid(self, kid: str) -> dict[str, Any]:
        for jwk in self._fresh_keys():
            if jwk.get("kid") == kid:
                return jwk
        self._fetched_at = 0.0
        for jwk in self._fresh_keys():
            if jwk.get("kid") == kid:
                return jwk
        raise TokenInvalid("unknown_kid")

    def _fresh_keys(self) -> list[dict[str, Any]]:
        now = time.time()
        if self._keys and now - self._fetched_at < self.ttl_seconds:
            return self._keys
        url = self._jwks_uri or self._discover()
        try:
            resp = self.fetch(url)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            if self._keys:
                return self._keys
            raise self.error("jwks", e) from e
        self._keys = resp.json().get("keys", [])
        self._fetched_at = now
        return self._keys

    def _discover(self) -> str:
        try:
            resp = self.fetch(self.discovery_url)
            resp.raise_for_status()
            document = resp.json()
        except httpx.HTTPError as e:
            raise self.error("discovery", e) from e
        self._jwks_uri = self.resolve_jwks_uri(document)
        return self._jwks_uri


def discovery_url(issuer: str) -> str:
    """Returns: URL de discovery OIDC do issuer."""
    return f"{issuer.rstrip('/')}/.well-known/openid-configuration"


def _legacy_jwks_uri(document: dict[str, Any]) -> str:
    jwks_uri = document.get("jwks_uri")
    if not jwks_uri:
        raise TokenInvalid("oidc_discovery_no_jwks_uri")
    return str(jwks_uri)


def _legacy_error(stage: FetchStage, error: httpx.HTTPError) -> TokenInvalid:
    code = "oidc_discovery_failed" if stage == "discovery" else "jwks_unreachable"
    return TokenInvalid(f"{code}: {error}")


@dataclass
class JWKSValidator:
    issuer: str
    audience: str
    jwks_ttl_seconds: int = 600
    http_timeout: float = 5.0
    _cache: _JwksCache = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._cache = _JwksCache(
            fetch=lambda url: httpx.get(url, timeout=self.http_timeout),
            discovery_url=discovery_url(self.issuer),
            ttl_seconds=self.jwks_ttl_seconds,
            resolve_jwks_uri=_legacy_jwks_uri,
            error=_legacy_error,
        )

    def verify(self, token: str) -> dict[str, Any]:
        try:
            header = jose_jwt.get_unverified_header(token)
        except JWTError as e:
            raise TokenInvalid(f"malformed_header: {e}") from e
        kid = header.get("kid")
        if kid is None:
            raise TokenInvalid("missing_kid")
        key = self._cache.key_for_kid(kid)
        try:
            return jose_jwt.decode(
                token, key, algorithms=["RS256"],
                audience=self.audience, issuer=self.issuer,
            )
        except ExpiredSignatureError as e:
            raise TokenInvalid("expired") from e
        except JWTClaimsError as e:
            msg = str(e).lower()
            if "audience" in msg:
                raise TokenInvalid("audience") from e
            if "issuer" in msg:
                raise TokenInvalid("issuer") from e
            raise TokenInvalid(f"claims: {e}") from e
        except (JWTError, JWKError) as e:
            raise TokenInvalid(f"signature: {e}") from e
