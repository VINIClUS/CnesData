"""Cognito no Floci: tokens SDK e PKCE S256 validados por JWKS e pelo OidcVerifier real."""
from __future__ import annotations

import base64
import json
import secrets
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from botocore.exceptions import ClientError
from jose import jwt as jose_jwt
from jose.exceptions import JWTClaimsError, JWTError

from cnes_infra.auth.jwt import TokenInvalid
from cnes_infra.auth.oidc import OidcVerifier
from tests.integration.floci._cognito import (
    API_SCOPE,
    CALLBACK,
    EMAIL,
    RESOURCE_SERVER,
    SDK_SCOPE,
    CognitoScenario,
    ExpectedClaims,
    delete_scenario,
    discovery,
    jwks,
    provision,
    sdk_tokens,
    verify_rs256,
)
from tests.integration.floci._floci import http_client, unique_name
from tests.integration.floci._oauth_pkce import (
    FLOCI_PATH_PREFIX,
    AuthorizationGrant,
    AuthorizationRequest,
    HostedUi,
    PkceFlowError,
    PkcePair,
    UserLogin,
    authorize_params,
    pkce_tokens,
    redeem,
    request_authorization,
)

if TYPE_CHECKING:
    from contextlib import ExitStack

    from tests.integration.floci._floci import FlociContext

pytestmark = [pytest.mark.floci]

_AWS_ISSUER = "https://cognito-idp.us-east-2.amazonaws.com"
_ACCESS_USE = "access"
_ID_USE = "id"


@dataclass(frozen=True, slots=True)
class _Env:
    cognito: Any
    http: httpx.Client
    scenario: CognitoScenario
    issuer: str
    keys: dict[str, Any]
    ui: HostedUi


@pytest.fixture(scope="module")
def env(floci: FlociContext, module_cleanup: ExitStack) -> _Env:
    cognito = floci.client("cognito-idp")
    scenario = provision(cognito, unique_name())
    module_cleanup.callback(delete_scenario, cognito, scenario)
    http = module_cleanup.enter_context(http_client())
    issuer = f"{floci.endpoint}/{scenario.pool_id}"
    keys = jwks(http, discovery(http, issuer))
    return _Env(cognito, http, scenario, issuer, keys, HostedUi(floci.endpoint, FLOCI_PATH_PREFIX))


@pytest.fixture(scope="module")
def sdk(env: _Env) -> dict[str, Any]:
    return sdk_tokens(env.cognito, env.scenario)


def _wrong() -> str:
    return f"Errada-{secrets.token_hex(4)}1!"


def _request(env: _Env, resource: str | None = None) -> AuthorizationRequest:
    return AuthorizationRequest(env.scenario.client_id, CALLBACK, f"openid {API_SCOPE}", resource)


def _login(env: _Env, password: str | None = None) -> UserLogin:
    return UserLogin(env.scenario.username, password or env.scenario.password)


def _pkce(env: _Env, resource: str | None = None) -> dict[str, Any]:
    with http_client() as browser:
        return pkce_tokens(browser, env.ui, _request(env, resource), _login(env))


def _authorize(env: _Env, **overrides: str) -> httpx.Response:
    params = authorize_params(_request(env), PkcePair.generate(), "estado-fixo")
    with http_client() as browser:
        return browser.get(env.ui.url("/oauth2/authorize"), params={**params, **overrides})


def _tamper(token: str) -> str:
    header, payload, signature = token.split(".")
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    claims["scope"] = f"{claims['scope']} {RESOURCE_SERVER}/admin"
    forged = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
    return f"{header}.{forged}.{signature}"


def _https_discovery(upstream: httpx.Client, issuer: str | None = None) -> httpx.Client:
    # OidcVerifier demands an https jwks_uri and Floci only speaks http: this transport serves
    # Floci's real discovery/JWKS with only the scheme rewritten, so keys and claims stay real.
    def handle(request: httpx.Request) -> httpx.Response:
        response = upstream.get(str(request.url.copy_with(scheme="http")))
        if not request.url.path.endswith("/openid-configuration"):
            return httpx.Response(response.status_code, content=response.content)
        document = response.json()
        document["jwks_uri"] = str(httpx.URL(document["jwks_uri"]).copy_with(scheme="https"))
        if issuer is not None:
            document["issuer"] = issuer
        return httpx.Response(response.status_code, json=document)

    return httpx.Client(transport=httpx.MockTransport(handle))


def test_discovery_publica_issuer_jwks_e_somente_pkce_s256(env: _Env) -> None:
    document = discovery(env.http, env.issuer)

    assert document["issuer"] == env.issuer
    assert document["jwks_uri"] == f"{env.issuer}/.well-known/jwks.json"
    assert document["code_challenge_methods_supported"] == ["S256"]
    assert document["id_token_signing_alg_values_supported"] == ["RS256"]


def test_access_token_assinado_valida_no_jwks(env: _Env, sdk: dict[str, Any]) -> None:
    claims = verify_rs256(sdk["AccessToken"], env.keys, ExpectedClaims(env.issuer))

    assert jose_jwt.get_unverified_header(sdk["AccessToken"])["alg"] == "RS256"
    assert claims["iss"] == env.issuer
    assert claims["token_use"] == _ACCESS_USE
    assert claims["client_id"] == env.scenario.client_id
    assert claims["scope"] == SDK_SCOPE
    assert claims["exp"] > time.time()
    assert "aud" not in claims


def test_id_token_do_fluxo_sdk_traz_aud_do_client(env: _Env, sdk: dict[str, Any]) -> None:
    expected = ExpectedClaims(env.issuer, audience=env.scenario.client_id)
    claims = verify_rs256(sdk["IdToken"], env.keys, expected)

    assert claims["token_use"] == _ID_USE
    assert claims["aud"] == env.scenario.client_id
    assert claims["email"] == EMAIL


def test_id_token_com_aud_de_outro_publico_e_rejeitado(env: _Env, sdk: dict[str, Any]) -> None:
    expected = ExpectedClaims(env.issuer, audience=RESOURCE_SERVER)

    with pytest.raises(JWTClaimsError, match="audience"):
        verify_rs256(sdk["IdToken"], env.keys, expected)


def test_token_com_issuer_diferente_e_rejeitado(env: _Env, sdk: dict[str, Any]) -> None:
    expected = ExpectedClaims(f"{_AWS_ISSUER}/{env.scenario.pool_id}")

    with pytest.raises(JWTClaimsError, match="issuer"):
        verify_rs256(sdk["AccessToken"], env.keys, expected)


def test_token_adulterado_e_rejeitado_pelo_jwks(env: _Env, sdk: dict[str, Any]) -> None:
    with pytest.raises(JWTError, match="Signature"):
        verify_rs256(_tamper(sdk["AccessToken"]), env.keys, ExpectedClaims(env.issuer))


def test_token_nao_valida_no_jwks_de_outro_pool(
    env: _Env, sdk: dict[str, Any], cleanup: ExitStack,
) -> None:
    other = provision(env.cognito, unique_name())
    cleanup.callback(delete_scenario, env.cognito, other)
    other_issuer = f"{env.ui.base_url}/{other.pool_id}"
    other_keys = jwks(env.http, discovery(env.http, other_issuer))

    with pytest.raises(JWTError, match="unknown_kid"):
        verify_rs256(sdk["AccessToken"], other_keys, ExpectedClaims(env.issuer))


def test_senha_errada_no_fluxo_sdk_e_rejeitada(env: _Env) -> None:
    scenario = env.scenario
    wrong = CognitoScenario(scenario.pool_id, scenario.client_id, scenario.username, _wrong())

    with pytest.raises(ClientError, match="NotAuthorizedException"):
        sdk_tokens(env.cognito, wrong)


def test_pkce_s256_emite_tokens_com_escopo_da_api(env: _Env) -> None:
    tokens = _pkce(env)
    access = verify_rs256(tokens["access_token"], env.keys, ExpectedClaims(env.issuer))
    expected_id = ExpectedClaims(env.issuer, env.scenario.client_id, tokens["access_token"])
    identity = verify_rs256(tokens["id_token"], env.keys, expected_id)

    assert access["token_use"] == _ACCESS_USE
    assert set(access["scope"].split()) == {"openid", API_SCOPE}
    assert access["client_id"] == env.scenario.client_id
    assert "aud" not in access
    assert identity["aud"] == env.scenario.client_id


def test_pkce_com_verifier_errado_e_rejeitado_e_consome_o_codigo(env: _Env) -> None:
    request = _request(env)
    with http_client() as browser:
        grant = request_authorization(browser, env.ui, request, _login(env))
        forged = AuthorizationGrant(grant.code, PkcePair.generate().verifier)
        rejected = redeem(browser, env.ui, request, forged)
        retried = redeem(browser, env.ui, request, grant)

    assert (rejected.status_code, rejected.json()["error"]) == (400, "invalid_grant")
    assert (retried.status_code, retried.json()["error"]) == (400, "invalid_grant")


def test_codigo_de_autorizacao_nao_pode_ser_reutilizado(env: _Env) -> None:
    request = _request(env)
    with http_client() as browser:
        grant = request_authorization(browser, env.ui, request, _login(env))
        first = redeem(browser, env.ui, request, grant)
        replay = redeem(browser, env.ui, request, grant)

    assert first.status_code == 200
    assert (replay.status_code, replay.json()["error"]) == (400, "invalid_grant")


@pytest.mark.parametrize(
    "redirect_uri", [f"{CALLBACK}/extra", "http://localhost/callbac", "http://127.0.0.1/callback"],
)
def test_callback_fora_da_lista_exata_e_rejeitado(env: _Env, redirect_uri: str) -> None:
    response = _authorize(env, redirect_uri=redirect_uri)

    assert (response.status_code, response.json()["error"]) == (400, "invalid_request")


def test_metodo_pkce_plain_e_rejeitado(env: _Env) -> None:
    response = _authorize(env, code_challenge_method="plain")

    assert (response.status_code, response.json()["error"]) == (400, "invalid_request")


def test_escopo_fora_do_client_volta_ao_callback_com_invalid_scope(env: _Env) -> None:
    response = _authorize(env, scope=f"openid {RESOURCE_SERVER}/admin")
    location = httpx.URL(response.headers["location"])

    assert response.status_code == 302
    assert str(location.copy_with(query=None)) == CALLBACK
    assert location.params["error_description"] == "invalid_scope"
    assert location.params["state"] == "estado-fixo"


def test_senha_errada_no_login_gerenciado_nao_emite_codigo(env: _Env) -> None:
    with http_client() as browser, pytest.raises(PkceFlowError, match="pkce_step=login status=400"):
        request_authorization(browser, env.ui, _request(env), _login(env, _wrong()))


def test_resource_rfc8707_e_ignorado_e_access_token_segue_sem_aud(env: _Env) -> None:
    plain = verify_rs256(_pkce(env)["access_token"], env.keys, ExpectedClaims(env.issuer))
    bound = _pkce(env, RESOURCE_SERVER)["access_token"]
    claims = verify_rs256(bound, env.keys, ExpectedClaims(env.issuer))

    assert "aud" not in claims
    assert claims["scope"] == plain["scope"]


def test_dominio_de_prefixo_e_armazenado_mas_nao_roteado(
    env: _Env, resource_name: str, cleanup: ExitStack,
) -> None:
    pool_id = env.scenario.pool_id
    env.cognito.create_user_pool_domain(Domain=resource_name, UserPoolId=pool_id)
    cleanup.callback(env.cognito.delete_user_pool_domain, Domain=resource_name, UserPoolId=pool_id)
    described = env.cognito.describe_user_pool_domain(Domain=resource_name)["DomainDescription"]
    document = discovery(env.http, env.issuer)

    assert (described["Status"], described["UserPoolId"]) == ("ACTIVE", pool_id)
    assert document["authorization_endpoint"] == env.ui.url("/oauth2/authorize")
    assert document["token_endpoint"] == env.ui.url("/oauth2/token")


def test_oidc_verifier_aceita_id_token_com_aud_do_client(env: _Env, sdk: dict[str, Any]) -> None:
    with _https_discovery(env.http) as client:
        principal = OidcVerifier(env.issuer, env.scenario.client_id, client).verify(sdk["IdToken"])

    assert principal.issuer == env.issuer
    assert principal.subject == jose_jwt.get_unverified_claims(sdk["IdToken"])["sub"]
    assert principal.email == EMAIL


def test_oidc_verifier_rejeita_access_token_sem_aud_mesmo_com_resource(env: _Env) -> None:
    token = _pkce(env, RESOURCE_SERVER)["access_token"]

    with _https_discovery(env.http) as client, pytest.raises(TokenInvalid, match=r"^audience$"):
        OidcVerifier(env.issuer, RESOURCE_SERVER, client).verify(token)


def test_oidc_verifier_rejeita_aud_de_outro_publico(env: _Env, sdk: dict[str, Any]) -> None:
    with _https_discovery(env.http) as client, pytest.raises(TokenInvalid, match=r"^audience$"):
        OidcVerifier(env.issuer, RESOURCE_SERVER, client).verify(sdk["IdToken"])


def test_oidc_verifier_rejeita_iss_diferente_do_configurado(env: _Env, sdk: dict[str, Any]) -> None:
    configured = env.issuer.replace("http://", "https://", 1)

    with (
        _https_discovery(env.http, configured) as client,
        pytest.raises(TokenInvalid, match=r"^issuer$"),
    ):
        OidcVerifier(configured, env.scenario.client_id, client).verify(sdk["IdToken"])


def test_oidc_verifier_recusa_jwks_http_publicado_pelo_floci(
    env: _Env, sdk: dict[str, Any],
) -> None:
    with http_client() as client, pytest.raises(TokenInvalid, match=r"^jwks_uri_not_https$"):
        OidcVerifier(env.issuer, env.scenario.client_id, client).verify(sdk["IdToken"])
