"""Claims sem assinatura e veredito do OidcVerifier como códigos curtos."""

import re
from collections.abc import Mapping

import httpx
from jose import jwt as jose_jwt
from jose.exceptions import JOSEError

from cnes_infra.auth.jwt import TokenInvalid
from cnes_infra.auth.oidc import OidcVerifier
from tests.integration.aws_capability._login_ports import API_ORIGIN


def make_verifier(issuer: str, client: httpx.Client) -> OidcVerifier:
    return OidcVerifier(issuer, API_ORIGIN, client)


def claims_of(token: str | None) -> Mapping[str, object]:
    if token is None:
        return {}
    try:
        return jose_jwt.get_unverified_claims(token)
    except JOSEError:
        return {}


def verdict(verifier: OidcVerifier, token: str | None) -> str:
    if token is None:
        return "no_token"
    try:
        verifier.verify(token)
    except TokenInvalid as error:
        text = str(error)
        return text if re.fullmatch(r"[a-z_]{1,40}", text) else "other"
    return "ok"


def aud_label(claims: Mapping[str, object]) -> str:
    if "aud" not in claims:
        return "absent"
    return API_ORIGIN if claims["aud"] == API_ORIGIN else "other"


def scope_has_api(claims: Mapping[str, object]) -> bool:
    scope = claims.get("scope")
    return isinstance(scope, str) and f"{API_ORIGIN}/api.access" in scope.split()


def claim_names(claims: Mapping[str, object]) -> list[str]:
    return sorted(name for name in claims if re.fullmatch(r"[a-z_:]{1,40}", name))
