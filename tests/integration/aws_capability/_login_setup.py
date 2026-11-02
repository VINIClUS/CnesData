"""Provisionamento do login espelhando o módulo Cognito do repositório."""

from collections.abc import Mapping
from dataclasses import dataclass, field

from tests.integration.aws_capability._login_handoff import new_password
from tests.integration.aws_capability._login_pkce import Endpoint
from tests.integration.aws_capability._login_ports import CALLBACK_URL, LOGOUT_URL, SCOPES
from tests.integration.aws_capability._safety import run_name
from tests.integration.aws_capability._service_cognito_server_create import create_server
from tests.integration.aws_capability._service_commands import call_command
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

_MIRRORED = (
    "ClientName",
    "AllowedOAuthFlowsUserPoolClient",
    "AllowedOAuthFlows",
    "AllowedOAuthScopes",
    "CallbackURLs",
    "LogoutURLs",
    "SupportedIdentityProviders",
    "ExplicitAuthFlows",
    "PreventUserExistenceErrors",
    "EnableTokenRevocation",
)


@dataclass(frozen=True)
class LoginEnv:
    client_id: str = field(repr=False)
    username: str = field(repr=False)
    password: str = field(repr=False)
    origin: str = field(repr=False)

    @property
    def endpoint(self) -> Endpoint:
        return Endpoint(self.origin, self.client_id)


def client_payload(spec: RunSpec, root: Resource) -> dict[str, object]:
    return {
        "UserPoolId": root.resource_id,
        "ClientName": f"{run_name(spec)}-dashboard",
        "GenerateSecret": False,
        "AllowedOAuthFlowsUserPoolClient": True,
        "AllowedOAuthFlows": ["code"],
        "AllowedOAuthScopes": list(SCOPES),
        "CallbackURLs": [CALLBACK_URL],
        "LogoutURLs": [LOGOUT_URL],
        "SupportedIdentityProviders": ["COGNITO"],
        "ExplicitAuthFlows": ["ALLOW_REFRESH_TOKEN_AUTH"],
        "PreventUserExistenceErrors": "ENABLED",
        "EnableTokenRevocation": True,
    }


def provision(
    commands: CommandPort, spec: RunSpec, root: Resource, context: RequestContext
) -> LoginEnv:
    """Returns: ambiente do login pronto para o humano; Raises: ServiceFailure por etapa."""
    if context.lane != "work" or spec.probe != "cognito-login" or root.name != run_name(spec):
        raise ServiceFailure("probe=login_setup_invalid")
    create_server(commands, spec, root, context)
    client_id = _create_client(commands, spec, root, context)
    username = f"cap-{spec.run_id}@example.invalid"
    password = new_password()
    _create_user(commands, root, (username, password), context)
    _create_domain(commands, spec, root, context)
    _create_branding(commands, root, client_id, context)
    origin = f"https://{run_name(spec)}.auth.{spec.region}.amazoncognito.com"
    return LoginEnv(client_id, username, password, origin)


def _step(
    commands: CommandPort, request: Request, context: RequestContext, step: str
) -> Mapping[str, object]:
    reply = call_command(commands, request, context)
    if not reply.ok or not isinstance(reply.data, Mapping):
        raise ServiceFailure(f"probe=step_failed step={step}")
    return reply.data


def _create_client(
    commands: CommandPort, spec: RunSpec, root: Resource, context: RequestContext
) -> str:
    payload = client_payload(spec, root)
    request = Request("cognito-idp", "create-user-pool-client", payload)
    created = _step(commands, request, context, "client")
    row = created.get("UserPoolClient")
    client_id = row.get("ClientId") if isinstance(row, Mapping) else None
    if not isinstance(client_id, str) or not client_id:
        raise ServiceFailure("probe=step_failed step=client")
    read = Request(
        "cognito-idp",
        "describe-user-pool-client",
        {"UserPoolId": root.resource_id, "ClientId": client_id},
    )
    described = _step(commands, read, context, "client_readback").get("UserPoolClient")
    if (
        not isinstance(described, Mapping)
        or "ClientSecret" in described
        or any(_unordered(described.get(key)) != _unordered(payload[key]) for key in _MIRRORED)
    ):
        raise ServiceFailure("probe=step_failed step=client_readback")
    return client_id


def _unordered(value: object) -> object:
    return sorted(value) if isinstance(value, list) else value


def _create_user(
    commands: CommandPort, root: Resource, credentials: tuple[str, str], context: RequestContext
) -> None:
    username, password = credentials
    create = {
        "UserPoolId": root.resource_id,
        "Username": username,
        "UserAttributes": [
            {"Name": "email", "Value": username},
            {"Name": "email_verified", "Value": "true"},
        ],
        "MessageAction": "SUPPRESS",
    }
    _step(commands, Request("cognito-idp", "admin-create-user", create), context, "user")
    permanent = {
        "UserPoolId": root.resource_id,
        "Username": username,
        "Password": password,
        "Permanent": True,
    }
    request = Request("cognito-idp", "admin-set-user-password", permanent)
    _step(commands, request, context, "password")


def _create_domain(
    commands: CommandPort, spec: RunSpec, root: Resource, context: RequestContext
) -> None:
    payload = {
        "UserPoolId": root.resource_id,
        "Domain": run_name(spec),
        "ManagedLoginVersion": 2,
    }
    _step(commands, Request("cognito-idp", "create-user-pool-domain", payload), context, "domain")


def _create_branding(
    commands: CommandPort, root: Resource, client_id: str, context: RequestContext
) -> None:
    payload = {
        "UserPoolId": root.resource_id,
        "ClientId": client_id,
        "UseCognitoProvidedValues": True,
    }
    request = Request("cognito-idp", "create-managed-login-branding", payload)
    _step(commands, request, context, "branding")
