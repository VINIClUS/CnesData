"""Endpoints OAuth do domain: prontidão, troca de código e refresh, só status e rótulos."""

import re
from collections.abc import Callable
from dataclasses import dataclass, field

from tests.integration.aws_capability._login_http import Http, HttpReply
from tests.integration.aws_capability._login_pkce import Endpoint, authorize_url, new_authorize
from tests.integration.aws_capability._login_ports import API_ORIGIN, CALLBACK_URL

READY_ATTEMPTS = 36
READY_PAUSE = 5.0


@dataclass(frozen=True)
class TokenReply:
    status: int
    error: str
    access: str | None = field(default=None, repr=False)
    identity: str | None = field(default=None, repr=False)
    refresh: str | None = field(default=None, repr=False)


def label(value: object) -> str:
    if value is None:
        return "none"
    if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_]{1,40}", value):
        return value
    return "other"


def _text(reply: HttpReply, key: str) -> str | None:
    value = reply.body.get(key)
    return value if isinstance(value, str) and value else None


def _token_reply(reply: HttpReply) -> TokenReply:
    error = "none" if reply.status == 200 else label(reply.body.get("error"))
    return TokenReply(
        reply.status,
        error,
        _text(reply, "access_token"),
        _text(reply, "id_token"),
        _text(reply, "refresh_token"),
    )


@dataclass(frozen=True)
class Grant:
    code: str = field(repr=False)
    verifier: str = field(repr=False)
    redirect_uri: str = CALLBACK_URL


def exchange_code(http: Http, endpoint: Endpoint, grant: Grant) -> TokenReply:
    form = {
        "grant_type": "authorization_code",
        "client_id": endpoint.client_id,
        "code": grant.code,
        "redirect_uri": grant.redirect_uri,
        "code_verifier": grant.verifier,
    }
    return _token_reply(http.post_form(f"{endpoint.origin}/oauth2/token", form))


def refresh_tokens(http: Http, endpoint: Endpoint, refresh_token: str) -> TokenReply:
    form = {
        "grant_type": "refresh_token",
        "client_id": endpoint.client_id,
        "refresh_token": refresh_token,
    }
    return _token_reply(http.post_form(f"{endpoint.origin}/oauth2/token", form))


def await_ready(
    http: Http, endpoint: Endpoint, pause: Callable[[float], None], published: Callable[[], bool]
) -> tuple[int, int | None, bool]:
    """Returns: tentativas, a tentativa em que o DNS publicou e se a página respondeu 200."""
    url = authorize_url(endpoint, new_authorize(API_ORIGIN))
    published_at: int | None = None
    for attempt in range(1, READY_ATTEMPTS + 1):
        if published_at is None and published():
            published_at = attempt
        if published_at is not None and http.get(url).status == 200:
            return attempt, published_at, True
        if attempt < READY_ATTEMPTS:
            pause(READY_PAUSE)
    return READY_ATTEMPTS, published_at, False
