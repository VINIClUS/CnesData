"""Cenário Cognito reutilizável: pool, resource server, client PKCE público, usuário e JWKS."""
from __future__ import annotations

import secrets
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from jose import jwt as jose_jwt
from jose.exceptions import JWTError

if TYPE_CHECKING:
    from collections.abc import Mapping

    import httpx

RESOURCE_SERVER = "https://api.cnesdata.com.br"
API_SCOPE = f"{RESOURCE_SERVER}/api.access"
SDK_SCOPE = "aws.cognito.signin.user.admin"
# Cognito only accepts plain http callbacks on localhost; the URL is never actually served.
CALLBACK = "http://localhost/callback"
USERNAME = "operador"
EMAIL = "operador@cnesdata.invalid"
_AUTH_FLOWS = ["ALLOW_ADMIN_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"]


@dataclass(frozen=True, slots=True)
class CognitoScenario:
    pool_id: str
    client_id: str
    username: str
    password: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class ExpectedClaims:
    issuer: str
    audience: str | None = None
    access_token: str | None = field(default=None, repr=False)


def provision(
    cognito: Any, name: str, pool_options: Mapping[str, Any] | None = None,
) -> CognitoScenario:
    """Cria pool, resource server, client público (code + PKCE) e usuário com senha definitiva.

    Returns: identificadores do cenário e a senha aleatória do usuário.
    """
    pool_id = cognito.create_user_pool(PoolName=name, **(pool_options or {}))["UserPool"]["Id"]
    try:
        return _populate(cognito, pool_id, name)
    except BaseException:
        cognito.delete_user_pool(UserPoolId=pool_id)
        raise


def _populate(cognito: Any, pool_id: str, name: str) -> CognitoScenario:
    cognito.create_resource_server(
        UserPoolId=pool_id, Identifier=RESOURCE_SERVER, Name="cnesdata-api",
        Scopes=[{"ScopeName": "api.access", "ScopeDescription": "Acesso a API CnesData"}],
    )
    client_id = cognito.create_user_pool_client(
        UserPoolId=pool_id, ClientName=f"{name}-spa", GenerateSecret=False,
        ExplicitAuthFlows=_AUTH_FLOWS, AllowedOAuthFlows=["code"],
        AllowedOAuthFlowsUserPoolClient=True, AllowedOAuthScopes=["openid", API_SCOPE],
        CallbackURLs=[CALLBACK], SupportedIdentityProviders=["COGNITO"],
    )["UserPoolClient"]["ClientId"]
    password = f"Cap-{secrets.token_urlsafe(18)}9!"
    cognito.admin_create_user(
        UserPoolId=pool_id, Username=USERNAME, MessageAction="SUPPRESS",
        UserAttributes=[
            {"Name": "email", "Value": EMAIL},
            {"Name": "email_verified", "Value": "true"},
        ],
    )
    cognito.admin_set_user_password(
        UserPoolId=pool_id, Username=USERNAME, Password=password, Permanent=True,
    )
    return CognitoScenario(pool_id, client_id, USERNAME, password)


def delete_scenario(cognito: Any, scenario: CognitoScenario) -> None:
    cognito.delete_user_pool(UserPoolId=scenario.pool_id)


def sdk_tokens(cognito: Any, scenario: CognitoScenario) -> dict[str, Any]:
    """Returns: AuthenticationResult do fluxo ADMIN_USER_PASSWORD_AUTH."""
    response = cognito.admin_initiate_auth(
        UserPoolId=scenario.pool_id,
        ClientId=scenario.client_id,
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": scenario.username, "PASSWORD": scenario.password},
    )
    return response["AuthenticationResult"]


def discovery(http: httpx.Client, issuer: str) -> dict[str, Any]:
    """Returns: documento OIDC publicado em `<issuer>/.well-known/openid-configuration`."""
    response = http.get(f"{issuer}/.well-known/openid-configuration")
    response.raise_for_status()
    return response.json()


def jwks(http: httpx.Client, document: Mapping[str, Any]) -> dict[str, Any]:
    """Returns: JWKS apontado pelo `jwks_uri` do discovery."""
    response = http.get(document["jwks_uri"])
    response.raise_for_status()
    return response.json()


def verify_rs256(token: str, keys: Mapping[str, Any], expected: ExpectedClaims) -> dict[str, Any]:
    """Valida assinatura RS256 pela chave do `kid`, `iss`, `exp` e, se pedidos, `aud`/`at_hash`.

    Raises:
        JWTError: kid desconhecido, assinatura, emissor, expiração ou audiência inválidos.
    """
    kid = jose_jwt.get_unverified_header(token).get("kid")
    key = next((key for key in keys.get("keys", []) if key.get("kid") == kid), None)
    if key is None:
        raise JWTError("jwks=unknown_kid")
    options = {
        "verify_aud": expected.audience is not None,
        "verify_at_hash": expected.access_token is not None,
    }
    return jose_jwt.decode(
        token, key, algorithms=["RS256"], options=options, audience=expected.audience,
        issuer=expected.issuer, access_token=expected.access_token,
    )
