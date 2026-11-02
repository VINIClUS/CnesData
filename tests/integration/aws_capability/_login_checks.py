"""Verificações do login; cada uma devolve só status, códigos, booleanos e nomes."""

from collections.abc import Mapping
from dataclasses import dataclass

from cnes_infra.auth.oidc import OidcVerifier
from tests.integration.aws_capability._login_chain import Chain, Plan
from tests.integration.aws_capability._login_claims import (
    aud_label,
    claim_names,
    claims_of,
    scope_has_api,
    verdict,
)
from tests.integration.aws_capability._login_http import Http
from tests.integration.aws_capability._login_oauth import (
    Grant,
    TokenReply,
    exchange_code,
    label,
    refresh_tokens,
)
from tests.integration.aws_capability._login_pkce import Endpoint
from tests.integration.aws_capability._login_ports import CALLBACK_URL
from tests.integration.aws_capability._probe_attempt import attempt
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import RequestContext

type Observed = dict[str, object]


@dataclass(frozen=True)
class Kit:
    http: Http
    endpoint: Endpoint
    verifier: OidcVerifier
    commands: CommandPort
    context: RequestContext


def check_discovery(http: Http, issuer: str) -> Observed:
    reply = http.get(f"{issuer}/.well-known/openid-configuration")
    jwks = reply.body.get("jwks_uri")
    return {
        "discovery_status": reply.status,
        "discovery_issuer_ok": reply.body.get("issuer") == issuer,
        "discovery_jwks_https": isinstance(jwks, str) and jwks.startswith("https://"),
    }


def _outcome(prefix: str, reply: TokenReply, kit: Kit) -> Observed:
    claims = claims_of(reply.access)
    return {
        f"{prefix}_status": reply.status,
        f"{prefix}_error": reply.error,
        f"{prefix}_aud": aud_label(claims) if reply.access else "no_token",
        f"{prefix}_verifier": verdict(kit.verifier, reply.access),
    }


def check_primary(kit: Kit, plan: Plan, chain: Chain) -> tuple[Observed, TokenReply]:
    reply = exchange_code(kit.http, kit.endpoint, _grant(plan, chain, "a"))
    observed = _outcome("a", reply, kit)
    claims = claims_of(reply.access)
    identity = claims_of(reply.identity)
    observed.update(
        {
            "a_token_use": label(claims.get("token_use")),
            "a_scope_api": scope_has_api(claims),
            "a_client_id_ok": claims.get("client_id") == kit.endpoint.client_id,
            "a_claims": claim_names(claims),
            "a_id_aud_is_client": identity.get("aud") == kit.endpoint.client_id,
        }
    )
    return observed, reply


def check_refresh(kit: Kit, refresh_token: str | None) -> Observed:
    if refresh_token is None:
        return {"refresh_status": 0, "refresh_error": "no_refresh_token"}
    reply = refresh_tokens(kit.http, kit.endpoint, refresh_token)
    return _outcome("refresh", reply, kit)


def check_sdk_refresh(kit: Kit, refresh_token: str | None) -> Observed:
    if refresh_token is None:
        return {"sdk_refresh": "no_refresh_token"}
    payload = {
        "AuthFlow": "REFRESH_TOKEN_AUTH",
        "ClientId": kit.endpoint.client_id,
        "AuthParameters": {"REFRESH_TOKEN": refresh_token},
    }
    result = attempt(kit.commands, Request("cognito-idp", "initiate-auth", payload), kit.context)
    token = _sdk_access(result.data) if result.ok else None
    claims = claims_of(token)
    return {
        "sdk_refresh": result.code,
        "sdk_refresh_aud": aud_label(claims) if token else "no_token",
        "sdk_refresh_verifier": verdict(kit.verifier, token),
    }


def _sdk_access(data: Mapping[str, object]) -> str | None:
    result = data.get("AuthenticationResult")
    token = result.get("AccessToken") if isinstance(result, Mapping) else None
    return token if isinstance(token, str) and token else None


def check_without_resource(kit: Kit, plan: Plan, chain: Chain) -> Observed:
    reply = exchange_code(kit.http, kit.endpoint, _grant(plan, chain, "b"))
    return _outcome("b", reply, kit)


def _grant(plan: Plan, chain: Chain, name: str, verifier: str | None = None) -> Grant:
    return Grant(chain.codes[name], verifier or plan.auths[name].verifier)


def check_negatives(kit: Kit, plan: Plan, chain: Chain) -> Observed:
    endpoint, http = kit.endpoint, kit.http
    wrong = exchange_code(http, endpoint, _grant(plan, chain, "c", plan.auths["d"].verifier))
    right = exchange_code(http, endpoint, _grant(plan, chain, "c"))
    moved_grant = Grant(chain.codes["d"], plan.auths["d"].verifier, CALLBACK_URL + "x")
    moved = exchange_code(http, endpoint, moved_grant)
    reuse = exchange_code(http, endpoint, _grant(plan, chain, "a"))
    return {
        "neg_wrong_verifier_status": wrong.status,
        "neg_wrong_verifier_error": wrong.error,
        "neg_right_after_wrong_status": right.status,
        "neg_right_after_wrong_error": right.error,
        "neg_right_after_wrong_consumed": right.status != 200,
        "neg_redirect_status": moved.status,
        "neg_redirect_error": moved.error,
        "neg_reuse_status": reuse.status,
        "neg_reuse_error": reuse.error,
    }


def check_password_auth(kit: Kit, username: str, password: str) -> Observed:
    payload = {
        "AuthFlow": "USER_PASSWORD_AUTH",
        "ClientId": kit.endpoint.client_id,
        "AuthParameters": {"USERNAME": username, "PASSWORD": password},
    }
    result = attempt(kit.commands, Request("cognito-idp", "initiate-auth", payload), kit.context)
    return {"sdk_password_auth": result.code}
