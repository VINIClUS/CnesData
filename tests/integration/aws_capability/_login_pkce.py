"""PKCE S256, state e URLs de authorize; segredos fora de repr."""

import base64
import hashlib
import secrets
from dataclasses import dataclass, field
from urllib.parse import quote

from tests.integration.aws_capability._login_ports import CALLBACK_URL, SCOPES


@dataclass(frozen=True)
class Endpoint:
    origin: str = field(repr=False)
    client_id: str = field(repr=False)


@dataclass(frozen=True)
class Authorize:
    state: str = field(repr=False)
    verifier: str = field(repr=False)
    resource: str | None = None


def challenge_of(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def new_authorize(resource: str | None) -> Authorize:
    return Authorize(secrets.token_urlsafe(24), secrets.token_urlsafe(48), resource)


def authorize_url(endpoint: Endpoint, auth: Authorize) -> str:
    pairs = [
        ("response_type", "code"),
        ("client_id", endpoint.client_id),
        ("redirect_uri", CALLBACK_URL),
        ("scope", "+".join(quote(scope, safe="") for scope in SCOPES)),
        ("state", auth.state),
        ("code_challenge", challenge_of(auth.verifier)),
        ("code_challenge_method", "S256"),
    ]
    if auth.resource is not None:
        pairs.append(("resource", auth.resource))
    query = "&".join(
        f"{key}={value if key == 'scope' else quote(value, safe='')}" for key, value in pairs
    )
    return f"{endpoint.origin}/oauth2/authorize?{query}"
