"""Remoção protegida de resource servers Cognito."""

from collections.abc import Mapping
from typing import TYPE_CHECKING

from tests.integration.aws_capability._resource_ids import parse_cognito_child
from tests.integration.aws_capability._safety import owned, run_name
from tests.integration.aws_capability._service_cognito import inspect_pool
from tests.integration.aws_capability._service_commands import (
    call_command,
    require_data,
    require_error,
)
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    ServiceFailure,
)

if TYPE_CHECKING:
    from tests.integration.aws_capability.services import AwsServices


def delete_server(resource: Resource, services: "AwsServices", context: RequestContext) -> None:
    """Remove server safely.
    Args: resource, services, context are cleanup inputs.
    Returns: None.
    Raises: ServiceFailure on invalid or unsafe deletion.
    """
    spec = services.spec
    if context.lane != "cleanup" or resource.service != "cognito":
        raise ServiceFailure("service=cognito_delete_invalid")
    if resource.name != run_name(spec):
        raise ServiceFailure("service=cognito_delete_invalid")
    try:
        root, kind, child = parse_cognito_child(resource, spec)
    except ServiceFailure:
        raise ServiceFailure("service=cognito_delete_invalid") from None
    if kind != "cognito.server":
        raise ServiceFailure("service=cognito_delete_invalid")
    identifier = child["identifier"]
    if identifier != "https://api.cnesdata.com.br":
        raise ServiceFailure("service=cognito_delete_invalid")
    owner = inspect_pool(services.ports.commands, spec, root, context)
    if not owned(root, owner, spec):
        raise ServiceFailure("service=not_owned")
    _read_server(services.ports.commands, root, identifier, context)
    _remove_server(services.ports.commands, root, identifier, context)


def _read_server(
    commands: CommandPort,
    root: Resource,
    identifier: str,
    context: RequestContext,
) -> None:
    reply = call_command(
        commands,
        Request(
            "cognito-idp",
            "describe-resource-server",
            {"UserPoolId": root.resource_id, "Identifier": identifier},
        ),
        context,
    )
    if not reply.ok:
        require_error(reply)
        raise ServiceFailure("service=describe_failed")
    data = require_data(reply)
    server = data.get("ResourceServer")
    if set(data) != {"ResourceServer"} or not isinstance(server, Mapping):
        raise ServiceFailure("service=describe_invalid")
    _validate_server(server, root, identifier, root.name)


def _validate_server(
    server: Mapping[str, object],
    root: Resource,
    identifier: str,
    expected_name: str,
) -> None:
    if set(server) != {"UserPoolId", "Identifier", "Name", "Scopes"}:
        raise ServiceFailure("service=describe_invalid")
    if (
        server["UserPoolId"] != root.resource_id
        or server["Identifier"] != identifier
        or server["Name"] != f"{expected_name}-api"
    ):
        raise ServiceFailure("service=describe_invalid")
    scopes = server["Scopes"]
    if not isinstance(scopes, list) or len(scopes) != 1:
        raise ServiceFailure("service=describe_invalid")
    scope = scopes[0]
    if not isinstance(scope, Mapping) or set(scope) != {"ScopeName", "ScopeDescription"}:
        raise ServiceFailure("service=describe_invalid")
    if scope != {"ScopeName": "api.access", "ScopeDescription": "Access to the CnesData API"}:
        raise ServiceFailure("service=describe_invalid")


def _remove_server(
    commands: CommandPort, root: Resource, identifier: str, context: RequestContext
) -> None:
    reply = call_command(
        commands,
        Request(
            "cognito-idp",
            "delete-resource-server",
            {"UserPoolId": root.resource_id, "Identifier": identifier},
        ),
        context,
    )
    if not reply.ok:
        error = require_error(reply)
        if error.origin == "local" and error.code == "operation_failed":
            raise ServiceFailure("service=request_failed")
        raise ServiceFailure("service=cognito_delete_failed")
    if reply.data != {}:
        raise ServiceFailure("service=cognito_delete_invalid")
