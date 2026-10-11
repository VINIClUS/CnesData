"""Authorization code + PKCE S256 por HTTP puro, contra o Floci ou um domínio Cognito real."""
from __future__ import annotations

import base64
import hashlib
import secrets
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urljoin, urlsplit

if TYPE_CHECKING:
    import httpx

FLOCI_PATH_PREFIX = "/cognito-idp"
_REDIRECTS = frozenset({301, 302, 303, 307})
_TEXT_INPUTS = frozenset({"text", "email"})
_CARRIED_INPUTS = frozenset({"hidden", "submit"})


class PkceFlowError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class HostedUi:
    """Login gerenciado: Floci usa o prefixo `/cognito-idp`; um domínio Cognito real, a raiz."""

    base_url: str
    path_prefix: str = ""

    def url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}{self.path_prefix}{path}"


@dataclass(frozen=True, slots=True)
class AuthorizationRequest:
    client_id: str
    redirect_uri: str
    scope: str
    resource: str | None = None


@dataclass(frozen=True, slots=True)
class UserLogin:
    username: str
    password: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class PkcePair:
    verifier: str = field(repr=False)
    challenge: str

    @classmethod
    def generate(cls) -> PkcePair:
        verifier = secrets.token_urlsafe(64)
        digest = hashlib.sha256(verifier.encode("ascii")).digest()
        return cls(verifier, base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii"))


@dataclass(frozen=True, slots=True)
class AuthorizationGrant:
    code: str = field(repr=False)
    verifier: str = field(repr=False)


@dataclass(slots=True)
class _Form:
    action: str
    fields: dict[str, str] = field(default_factory=dict[str, str])
    username_field: str | None = None
    password_field: str | None = None


class _LoginFormParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.forms: list[_Form] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {name: value or "" for name, value in attrs}
        if tag == "form":
            self.forms.append(_Form(values.get("action", "")))
        elif tag == "input" and self.forms:
            _add_input(self.forms[-1], values)


def _add_input(form: _Form, values: dict[str, str]) -> None:
    name = values.get("name", "")
    kind = values.get("type", "text").lower()
    if not name:
        return
    if kind == "password":
        form.password_field = form.password_field or name
    elif kind in _TEXT_INPUTS:
        form.username_field = form.username_field or name
    elif kind in _CARRIED_INPUTS:
        form.fields[name] = values.get("value", "")


def authorize_params(request: AuthorizationRequest, pkce: PkcePair, state: str) -> dict[str, str]:
    """Returns: query do `/oauth2/authorize` com challenge S256 e `resource` opcional."""
    params = {
        "response_type": "code",
        "client_id": request.client_id,
        "redirect_uri": request.redirect_uri,
        "scope": request.scope,
        "state": state,
        "code_challenge": pkce.challenge,
        "code_challenge_method": "S256",
    }
    if request.resource is not None:
        params["resource"] = request.resource
    return params


def request_authorization(
    http: httpx.Client, ui: HostedUi, request: AuthorizationRequest, login: UserLogin,
) -> AuthorizationGrant:
    """Percorre authorize, formulário de login e callback.

    Raises:
        PkceFlowError: redirecionamento ausente, formulário ausente ou state divergente.
    """
    pkce = PkcePair.generate()
    state = secrets.token_urlsafe(16)
    page = _login_page(http, ui, authorize_params(request, pkce, state))
    action, data = _login_submission(page.text, login)
    callback = http.post(urljoin(str(page.url), action or str(page.url)), data=data)
    return AuthorizationGrant(_callback_code(callback, request.redirect_uri, state), pkce.verifier)


def redeem(
    http: httpx.Client, ui: HostedUi, request: AuthorizationRequest, grant: AuthorizationGrant,
) -> httpx.Response:
    """Returns: resposta crua do `/oauth2/token` para o código e o verifier dados."""
    form = {
        "grant_type": "authorization_code",
        "client_id": request.client_id,
        "code": grant.code,
        "redirect_uri": request.redirect_uri,
        "code_verifier": grant.verifier,
    }
    if request.resource is not None:
        form["resource"] = request.resource
    return http.post(ui.url("/oauth2/token"), data=form)


def pkce_tokens(
    http: httpx.Client, ui: HostedUi, request: AuthorizationRequest, login: UserLogin,
) -> dict[str, Any]:
    """Returns: corpo JSON do token endpoint ao fim do fluxo.

    Raises:
        PkceFlowError: alguma etapa falhou ou o token endpoint não respondeu 200.
    """
    response = redeem(http, ui, request, request_authorization(http, ui, request, login))
    if response.status_code != 200:
        raise PkceFlowError(f"pkce_step=token status={response.status_code}")
    return response.json()


def _login_page(http: httpx.Client, ui: HostedUi, params: dict[str, str]) -> httpx.Response:
    response = http.get(ui.url("/oauth2/authorize"), params=params)
    location = response.headers.get("location")
    if response.status_code not in _REDIRECTS or not location:
        raise PkceFlowError(f"pkce_step=authorize status={response.status_code}")
    page = http.get(urljoin(str(response.url), location))
    if page.status_code != 200:
        raise PkceFlowError(f"pkce_step=login_page status={page.status_code}")
    return page


def _login_submission(html: str, login: UserLogin) -> tuple[str, dict[str, str]]:
    parser = _LoginFormParser()
    parser.feed(html)
    for form in parser.forms:
        if form.username_field and form.password_field:
            fields = {form.username_field: login.username, form.password_field: login.password}
            return form.action, {**form.fields, **fields}
    raise PkceFlowError("pkce_step=login_form reason=missing")


def _callback_code(response: httpx.Response, redirect_uri: str, state: str) -> str:
    location = response.headers.get("location", "")
    if response.status_code not in _REDIRECTS or location.split("?")[0] != redirect_uri:
        raise PkceFlowError(f"pkce_step=login status={response.status_code}")
    query = parse_qs(urlsplit(location).query)
    if query.get("state") != [state]:
        raise PkceFlowError("pkce_step=callback reason=state_mismatch")
    codes = query.get("code")
    if not codes:
        raise PkceFlowError(f"pkce_step=callback reason=no_code error={query.get('error')}")
    return codes[0]
