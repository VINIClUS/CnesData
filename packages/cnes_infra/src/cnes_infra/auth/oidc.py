"""Verificação OIDC genérica: discovery, JWKS RS256 e identidade sem tenant."""
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx
from jose import jwt as jose_jwt
from jose.exceptions import ExpiredSignatureError, JWKError, JWTClaimsError, JWTError

from cnes_infra.auth.jwt import FetchStage, TokenInvalid, _JwksCache, discovery_url

_REQUIRED_CLAIMS = ("exp", "iat")


@dataclass(frozen=True, slots=True)
class OidcPrincipal:
    issuer: str
    subject: str
    email: str | None
    display_name: str | None


class OidcVerifier:
    def __init__(
        self, issuer: str, audience: str, client: httpx.Client, cache_ttl_seconds: int = 600,
    ) -> None:
        self._issuer = issuer
        self._audience = audience
        self._keys = _JwksCache(
            fetch=client.get,
            discovery_url=discovery_url(issuer),
            ttl_seconds=cache_ttl_seconds,
            resolve_jwks_uri=self._jwks_uri,
            error=_unreachable,
        )

    def verify(self, token: str) -> OidcPrincipal:
        """Valida o token e retorna a identidade do provedor.

        Raises:
            TokenInvalid: código sanitizado, sem token, URL ou corpo de resposta.
        """
        header = _header(token)
        if header.get("alg") != "RS256":
            raise TokenInvalid("unsupported_algorithm")
        kid = header.get("kid")
        if not kid:
            raise TokenInvalid("missing_kid")
        try:
            key = self._keys.key_for_kid(kid)
        except TokenInvalid as error:
            raise TokenInvalid(str(error)) from None
        claims = self._decode(token, key)
        if "aud" not in claims:
            raise TokenInvalid("audience")
        if any(name not in claims for name in _REQUIRED_CLAIMS):
            raise TokenInvalid("claims")
        subject = claims.get("sub")
        if not isinstance(subject, str) or not subject.strip():
            raise TokenInvalid("subject_required")
        return OidcPrincipal(
            issuer=self._issuer,
            subject=subject,
            email=_optional_text(claims.get("email")),
            display_name=_optional_text(claims.get("name")),
        )

    def _decode(self, token: str, key: dict[str, Any]) -> dict[str, Any]:
        try:
            return jose_jwt.decode(
                token, key, algorithms=["RS256"], audience=self._audience, issuer=self._issuer,
            )
        except ExpiredSignatureError:
            raise TokenInvalid("expired") from None
        except JWTClaimsError as error:
            raise TokenInvalid(_claims_code(error)) from None
        except (JWTError, JWKError):
            raise TokenInvalid("signature") from None

    def _jwks_uri(self, document: dict[str, Any]) -> str:
        if document.get("issuer") != self._issuer:
            raise TokenInvalid("discovery_issuer_mismatch")
        jwks_uri = document.get("jwks_uri")
        if not isinstance(jwks_uri, str) or not jwks_uri:
            raise TokenInvalid("discovery_no_jwks_uri")
        if urlsplit(jwks_uri).scheme != "https":
            raise TokenInvalid("jwks_uri_not_https")
        return jwks_uri


def _header(token: str) -> dict[str, Any]:
    try:
        return jose_jwt.get_unverified_header(token)
    except JWTError:
        raise TokenInvalid("malformed_header") from None


def _claims_code(error: JWTClaimsError) -> str:
    message = str(error).lower()
    if "audience" in message:
        return "audience"
    if "issuer" in message:
        return "issuer"
    return "claims"


def _unreachable(stage: FetchStage, _error: Exception) -> TokenInvalid:
    return TokenInvalid(f"{stage}_unreachable")


def _optional_text(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None
