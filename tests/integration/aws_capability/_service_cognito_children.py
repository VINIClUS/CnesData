"""Readbacks frescos de posse e pertença de client Cognito."""

from collections.abc import Mapping

from tests.integration.aws_capability._resource_ids import parse_cognito_client
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
