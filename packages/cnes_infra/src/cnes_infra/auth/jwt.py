"""JWKS-backed JWT validator. Fetches signing keys from issuer, caches by TTL."""
import time
from dataclasses import dataclass, field
from typing import Any

import httpx
from jose import jwt as jose_jwt
from jose.exceptions import (
    ExpiredSignatureError,
    JWKError,
    JWTClaimsError,
    JWTError,
)


class TokenInvalid(Exception):
    pass


@dataclass
class JWKSValidator:
    issuer: str
    audience: str
    jwks_ttl_seconds: int = 600
    http_timeout: float = 5.0
    _jwks: list[dict[str, Any]] = field(default_factory=list, init=False, repr=False)
    _fetched_at: float = field(default=0.0, init=False, repr=False)

    def verify(self, token: str) -> dict[str, Any]:
        try:
            header = jose_jwt.get_unverified_header(token)
        except JWTError as e:
            raise TokenInvalid(f"malformed_header: {e}") from e
        kid = header.get("kid")
        key = self._key_for_kid(kid)
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

    def _key_for_kid(self, kid: str | None) -> dict[str, Any]:
        if kid is None:
            raise TokenInvalid("missing_kid")
        for jwk in self._fresh_jwks():
            if jwk.get("kid") == kid:
                return jwk
        self._fetched_at = 0.0
        for jwk in self._fresh_jwks():
            if jwk.get("kid") == kid:
                return jwk
        raise TokenInvalid("unknown_kid")

    def _fresh_jwks(self) -> list[dict[str, Any]]:
        now = time.time()
        if self._jwks and now - self._fetched_at < self.jwks_ttl_seconds:
            return self._jwks
        url = f"{self.issuer}/.well-known/jwks.json"
        try:
            resp = httpx.get(url, timeout=self.http_timeout)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            if self._jwks:
                return self._jwks
            raise TokenInvalid(f"jwks_unreachable: {e}") from e
        self._jwks = resp.json().get("keys", [])
        self._fetched_at = now
        return self._jwks
