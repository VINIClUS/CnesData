"""Remoção protegida dos filhos Cognito do login: client, domain, usuário e branding."""

from typing import TYPE_CHECKING

from tests.integration.aws_capability._resource_ids import parse_cognito_child
from tests.integration.aws_capability._safety import owned, run_name
from tests.integration.aws_capability._service_cognito_server_delete import delete_server
from tests.integration.aws_capability._service_commands import call_command, require_error
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    ServiceFailure,
)

if TYPE_CHECKING:
    from tests.integration.aws_capability.services import AwsServices

_REMOVALS = {
    "cognito.client": ("delete-user-pool-client", "ClientId", "client_id"),
    "cognito.domain": ("delete-user-pool-domain", "Domain", "domain"),
    "cognito.user": ("admin-delete-user", "Username", "username"),
    "cognito.branding": (
        "delete-managed-login-branding",
        "ManagedLoginBrandingId",
        "branding_id",
    ),
}


def delete_cognito_child(
    resource: Resource, services: "AwsServices", context: RequestContext
) -> None:
    """Args: resource, services e context da limpeza.
    Raises: ServiceFailure para entrada, posse ou remoção inválida.
    """
    spec = services.spec
    if context.lane != "cleanup" or resource.name != run_name(spec):
        raise ServiceFailure("service=cognito_delete_invalid")
    try:
        root, kind, child = parse_cognito_child(resource, spec)
    except ServiceFailure:
        raise ServiceFailure("service=cognito_delete_invalid") from None
    if kind == "cognito.server":
        delete_server(resource, services, context)
        return
    owner = services.inspect(resource, context)
    if not owned(root, owner, spec):
        raise ServiceFailure("service=not_owned")
    operation, field, key = _REMOVALS[kind]
    payload: dict[str, object] = {"UserPoolId": root.resource_id, field: child[key]}
    if kind == "cognito.domain":
        payload = {"UserPoolId": root.resource_id, "Domain": child["domain"]}
    reply = call_command(
        services.ports.commands, Request("cognito-idp", operation, payload), context
    )
    if not reply.ok:
        require_error(reply)
        raise ServiceFailure("service=cognito_delete_failed")
    if reply.data != {}:
        raise ServiceFailure("service=cognito_delete_invalid")
