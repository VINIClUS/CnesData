"""Readbacks frescos de posse e pertença de client Cognito."""

from collections.abc import Mapping

from tests.integration.aws_capability._resource_ids import (
    cognito_child,
    parse_cognito_child,
    parse_cognito_client,
)
from tests.integration.aws_capability._safety import owned
from tests.integration.aws_capability._service_cognito import inspect_pool
from tests.integration.aws_capability._service_commands import (
    call_command,
    is_remote_not_found,
    require_data,
    require_error,
)
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Ownership,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def lookup_client(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership | None:
    """Returns:
    Posse fresca do root ou ausência comprovada do client.
    Raises:
    ServiceFailure: identidade, posse ou leitura inválida.
    """
    root, client_id = parse_cognito_client(resource, spec)
    owner = inspect_pool(commands, spec, root, context)
    if not owned(root, owner, spec):
        raise ServiceFailure("service=not_owned")
    reply = call_command(
        commands,
        Request(
            "cognito-idp",
            "describe-user-pool-client",
            {"UserPoolId": root.resource_id, "ClientId": client_id},
        ),
        context,
    )
    if not reply.ok:
        if is_remote_not_found(reply):
            return None
        require_error(reply)
        raise ServiceFailure("service=describe_failed")
    _require_client(require_data(reply), root.resource_id, client_id)
    return owner


def inspect_client(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership:
    """Returns:
    Posse fresca do root após prova de presença do client.
    Raises:
    ServiceFailure: client ausente ou leitura inválida.
    """
    owner = lookup_client(commands, spec, resource, context)
    if owner is None:
        raise ServiceFailure("service=not_found")
    return owner


def _require_client(data: Mapping[str, object], pool_id: str, client_id: str) -> None:
    client = data.get("UserPoolClient")
    if not isinstance(client, Mapping):
        raise ServiceFailure("service=describe_invalid")
    if client.get("UserPoolId") != pool_id or client.get("ClientId") != client_id:
        raise ServiceFailure("service=describe_invalid")


def lookup_cognito_child(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership | None:
    """Returns:
    Posse fresca após pertença exata ou ausência modelada do filho.
    Raises:
    ServiceFailure: leitura, identidade ou posse inválida.
    """
    root, kind, child = parse_cognito_child(resource, spec)
    if kind == "cognito.client":
        return lookup_client(commands, spec, resource, context)
    if kind == "cognito.branding":
        parent = cognito_child(root, spec, "cognito.client", {"client_id": child["client_id"]})
        owner = inspect_client(commands, spec, parent, context)
        branding_id = read_branding(commands, root, child["client_id"], context)
        if branding_id is None:
            return None
        if branding_id != child["branding_id"]:
            raise ServiceFailure("service=describe_invalid")
        return owner
    owner = inspect_pool(commands, spec, root, context)
    if not owned(root, owner, spec):
        raise ServiceFailure("service=not_owned")
    request = _child_request(root, kind, child)
    missing = "UserNotFoundException" if kind == "cognito.user" else "ResourceNotFoundException"
    data = _read_child(commands, request, "" if kind == "cognito.domain" else missing, context)
    if data is None:
        return None
    _require_child(data, (kind, root.resource_id, spec.account), child)
    return owner


def inspect_cognito_child(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership:
    """Returns:
    Posse fresca após prova completa da presença do filho.
    Raises:
    ServiceFailure: filho ausente ou leitura inválida.
    """
    owner = lookup_cognito_child(commands, spec, resource, context)
    if owner is None:
        raise ServiceFailure("service=not_found")
    return owner


def read_branding(
    commands: CommandPort, root: Resource, client_id: str, context: RequestContext
) -> str | None:
    """Returns:
    ID de branding por client já comprovado ou ausência modelada.
    Raises:
    ServiceFailure: resposta ou pertença inválida.
    """
    request = Request(
        "cognito-idp",
        "describe-managed-login-branding-by-client",
        {"UserPoolId": root.resource_id, "ClientId": client_id, "ReturnMergedResources": False},
    )
    data = _read_child(commands, request, "ResourceNotFoundException", context)
    if data is None:
        return None
    branding = data.get("ManagedLoginBranding")
    if not isinstance(branding, Mapping) or branding.get("UserPoolId") != root.resource_id:
        raise ServiceFailure("service=describe_invalid")
    branding_id = branding.get("ManagedLoginBrandingId")
    if not isinstance(branding_id, str) or not branding_id:
        raise ServiceFailure("service=describe_invalid")
    return branding_id


def _read_child(
    commands: CommandPort, request: Request, missing: str, context: RequestContext
) -> Mapping[str, object] | None:
    reply = call_command(commands, request, context)
    if not reply.ok:
        error = require_error(reply)
        if error.origin == "service" and error.code == missing:
            return None
        raise ServiceFailure("service=describe_failed")
    return require_data(reply)


def _child_request(root: Resource, kind: str, child: Mapping[str, str]) -> Request:
    if kind == "cognito.domain":
        return Request("cognito-idp", "describe-user-pool-domain", {"Domain": child["domain"]})
    if kind == "cognito.server":
        return Request(
            "cognito-idp",
            "describe-resource-server",
            {"UserPoolId": root.resource_id, "Identifier": child["identifier"]},
        )
    return Request(
        "cognito-idp",
        "admin-get-user",
        {"UserPoolId": root.resource_id, "Username": child["username"]},
    )


def _require_child(
    data: Mapping[str, object], scope: tuple[str, str, str], child: Mapping[str, str]
) -> None:
    kind, pool_id, account = scope
    if kind == "cognito.user":
        expected = {"Username": child["username"]}
    elif kind == "cognito.server":
        data = _child_data(data, "ResourceServer")
        expected = {"UserPoolId": pool_id, "Identifier": child["identifier"]}
    else:
        data = _child_data(data, "DomainDescription")
        expected = {"UserPoolId": pool_id, "Domain": child["domain"], "AWSAccountId": account}
    if any(data.get(key) != value for key, value in expected.items()):
        raise ServiceFailure("service=describe_invalid")


def _child_data(data: Mapping[str, object], wrapper: str) -> Mapping[str, object]:
    value = data.get(wrapper)
    if not isinstance(value, Mapping):
        raise ServiceFailure("service=describe_invalid")
    return value
