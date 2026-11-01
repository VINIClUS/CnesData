"""Exclusão de inline policies e role com posse fresca e ausência confirmada."""

from typing import TYPE_CHECKING

from tests.integration.aws_capability._resource_ids import parse_iam_child
from tests.integration.aws_capability._safety import owned, run_name
from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_iam import inspect_role
from tests.integration.aws_capability._service_polling import (
    POLL_ATTEMPTS,
    POLL_SECONDS,
    await_absent,
)
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import RequestContext, Resource, ServiceFailure

if TYPE_CHECKING:
    from tests.integration.aws_capability.services import AwsServices


def delete_iam(
    resource: Resource, bypass: bool, services: "AwsServices", context: RequestContext
) -> None:
    """Raises: ServiceFailure para entrada, posse ou resposta inválida."""
    if context.lane != "cleanup" or type(bypass) is not bool:
        raise ServiceFailure("service=iam_delete_invalid")
    if resource.resource_id == resource.name:
        root, policy = resource, None
    else:
        root, policy = parse_iam_child(resource, services.spec)
    if root.resource_id != run_name(services.spec) or root.name != root.resource_id:
        raise ServiceFailure("service=iam_delete_invalid")
    _require_owned(root, services, context)
    if policy is None:
        _delete_role(root, services, context)
    else:
        _delete_policy(root, policy, services, context)


def _require_owned(root: Resource, services: "AwsServices", context: RequestContext) -> None:
    owner = inspect_role(services.ports.commands, services.spec, root, context)
    if not owned(root, owner, services.spec):
        raise ServiceFailure("service=not_owned")


def _delete_policy(
    root: Resource, policy: str, services: "AwsServices", context: RequestContext
) -> None:
    payload: dict[str, object] = {"RoleName": root.resource_id, "PolicyName": policy}
    request = Request("iam", "delete-role-policy", payload)
    require_data(call_command(services.ports.commands, request, context))
    probe = Request("iam", "get-role-policy", payload)
    await_absent(services.ports, probe, "NoSuchEntity", context)


def _delete_role(root: Resource, services: "AwsServices", context: RequestContext) -> None:
    payload: dict[str, object] = {"RoleName": root.resource_id}
    request = Request("iam", "delete-role", payload)
    for attempt in range(POLL_ATTEMPTS):
        reply = call_command(services.ports.commands, request, context)
        if reply.ok:
            probe = Request("iam", "get-role", payload)
            await_absent(services.ports, probe, "NoSuchEntity", context)
            return
        conflict = reply.error is not None and reply.error.code == "DeleteConflict"
        if not conflict:
            break
        if attempt < POLL_ATTEMPTS - 1:
            services.ports.pause(POLL_SECONDS)
    raise ServiceFailure("service=delete_failed")
