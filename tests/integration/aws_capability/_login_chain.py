"""Cadeia de authorizes: um login humano rende todos os códigos, cada um com state e PKCE."""

from collections.abc import Callable
from dataclasses import dataclass, field

from tests.integration.aws_capability._login_pkce import (
    Authorize,
    Endpoint,
    authorize_url,
    new_authorize,
)
from tests.integration.aws_capability._login_ports import API_ORIGIN, ListenerPort

LOGIN_SECONDS = 600.0
STEPS: tuple[tuple[str, str | None], ...] = (
    ("a", API_ORIGIN),
    ("b", None),
    ("c", API_ORIGIN),
    ("d", API_ORIGIN),
)
DONE_PAGE = "login concluido; avise o operador e feche esta aba"
REFUSED_PAGE = "login recusado; avise o operador"


@dataclass(frozen=True)
class Plan:
    auths: dict[str, Authorize] = field(repr=False)
    urls: dict[str, str] = field(repr=False)


@dataclass(frozen=True)
class Chain:
    outcome: str
    codes: dict[str, str] = field(default_factory=dict, repr=False)
    error: str = "none"


def make_plan(endpoint: Endpoint) -> Plan:
    auths = {name: new_authorize(resource) for name, resource in STEPS}
    return Plan(auths, {name: authorize_url(endpoint, auth) for name, auth in auths.items()})


def follow(
    listener: ListenerPort, plan: Plan, limit: float, now: Callable[[], float]
) -> Chain:
    """Returns: códigos em memória; o outcome nomeia timeout, state ou erro do callback."""
    end = now() + limit
    codes: dict[str, str] = {}
    names = [name for name, _ in STEPS]
    for index, name in enumerate(names):
        remaining = end - now()
        callback = listener.wait(remaining) if remaining > 0 else None
        if callback is None:
            return Chain("timeout", codes)
        if callback.error is not None:
            listener.finish(REFUSED_PAGE)
            return Chain("callback_error", codes, _short(callback.error))
        if callback.state != plan.auths[name].state:
            listener.finish(REFUSED_PAGE)
            return Chain("state_mismatch", codes)
        if not callback.code:
            listener.finish(REFUSED_PAGE)
            return Chain("missing_code", codes)
        codes[name] = callback.code
        if index + 1 < len(names):
            listener.redirect(plan.urls[names[index + 1]])
    listener.finish(DONE_PAGE)
    return Chain("ok", codes)


def _short(error: str) -> str:
    return error if error.isidentifier() and len(error) <= 40 else "other"
