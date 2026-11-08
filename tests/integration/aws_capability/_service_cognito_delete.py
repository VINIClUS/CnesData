"""Remoção protegida do pool Cognito."""

from collections.abc import Mapping
from typing import TYPE_CHECKING

from tests.integration.aws_capability._inventory import pages
from tests.integration.aws_capability._safety import run_name
from tests.integration.aws_capability._service_cognito_settings import (
    preserve,
    read_snapshot,
    update_payload,
    validate_snapshot,
)
from tests.integration.aws_capability._service_commands import call_command, require_error
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

if TYPE_CHECKING:
    from tests.integration.aws_capability.services import AwsServices


def delete_pool(resource: Resource, services: "AwsServices", context: RequestContext) -> None:
    spec = services.spec
    if (
        context.lane != "cleanup"
        or resource.service != "cognito"
        or resource.name != run_name(spec)
        or not _root_id(resource, spec)
    ):
        raise ServiceFailure("service=cognito_delete_invalid")
    if _children(resource, services, context):
        raise ServiceFailure("service=cognito_children_present")
    pool, tags = read_snapshot(services.ports.commands, spec, resource, context)
    protection = "ACTIVE" if pool.get("DeletionProtection") == "ACTIVE" else "INACTIVE"
    validate_snapshot(pool, tags, spec, protection)
    if pool.get("DeletionProtection") == "ACTIVE":
        payload = update_payload(pool, tags, resource)
        _deactivate(services.ports.commands, resource, payload, context)
        updated, updated_tags = read_snapshot(services.ports.commands, spec, resource, context)
        validate_snapshot(updated, updated_tags, spec, "INACTIVE")
        preserve(pool, updated)
    _delete(services.ports.commands, resource, context)


def _root_id(resource: Resource, spec: RunSpec) -> bool:
    return isinstance(resource.resource_id, str) and resource.resource_id.startswith(
        spec.region + "_"
    )


def _children(
    resource: Resource, services: "AwsServices", context: RequestContext
) -> list[Resource]:
    return pages(lambda cursor: services.dependents(resource, cursor, context), 8)


def _deactivate(
    commands: CommandPort,
    resource: Resource,
    payload: Mapping[str, object],
    context: RequestContext,
) -> None:
    reply = call_command(commands, Request("cognito-idp", "update-user-pool", payload), context)
    if not reply.ok:
        require_error(reply)
        raise ServiceFailure("service=cognito_update_failed")
    if reply.data != {}:
        raise ServiceFailure("service=cognito_update_invalid")


def _delete(commands: CommandPort, resource: Resource, context: RequestContext) -> None:
    reply = call_command(
        commands,
        Request("cognito-idp", "delete-user-pool", {"UserPoolId": resource.resource_id}),
        context,
    )
    if not reply.ok:
        require_error(reply)
        raise ServiceFailure("service=cognito_delete_failed")
    if reply.data != {}:
        raise ServiceFailure("service=cognito_delete_invalid")
