"""Remoção das raízes ECR, Logs, ECS e EC2 com posse fresca e esperas limitadas."""

from typing import TYPE_CHECKING

from tests.integration.aws_capability._fargate_residue import purge_revisions
from tests.integration.aws_capability._safety import owned, root_names
from tests.integration.aws_capability._service_commands import require_data
from tests.integration.aws_capability._service_fargate import (
    call,
    failed_with,
    inspect_leaf,
    require_leaf,
    rows_of,
)
from tests.integration.aws_capability.contracts import RequestContext, Resource, ServiceFailure

if TYPE_CHECKING:
    from tests.integration.aws_capability.services import AwsServices

GROUP_ATTEMPTS = 18
GROUP_PAUSE = 4.0
FINAL_SECONDS = 80.0
FINAL_CALLS = 50
STOP_POLLS = 24
STOP_PAUSE = 5.0


def delete_leaf(
    resource: Resource, bypass: bool, services: "AwsServices", context: RequestContext
) -> None:
    """Raises: ServiceFailure para entrada, posse, espera esgotada ou resposta inválida."""
    if context.lane != "cleanup" or type(bypass) is not bool:
        raise ServiceFailure("service=fargate_delete_invalid")
    require_leaf(resource)
    if resource.name not in root_names(services.spec, resource.service):
        raise ServiceFailure("service=fargate_delete_invalid")
    owner = inspect_leaf(services.ports.commands, services.spec, resource, context)
    if not owned(resource, owner, services.spec):
        raise ServiceFailure("service=not_owned")
    _DELETERS[resource.service](resource, services, context)


def _delete_ecr(resource: Resource, services: "AwsServices", context: RequestContext) -> None:
    payload = {"repositoryName": resource.name, "force": True}
    require_data(call(services.ports.commands, "ecr.delete-repository", payload, context))


def _delete_logs(resource: Resource, services: "AwsServices", context: RequestContext) -> None:
    payload = {"logGroupName": resource.name}
    require_data(call(services.ports.commands, "logs.delete-log-group", payload, context))


def _delete_group(resource: Resource, services: "AwsServices", context: RequestContext) -> None:
    payload = {"GroupId": resource.resource_id}
    for attempt in range(GROUP_ATTEMPTS):
        reply = call(services.ports.commands, "ec2.delete-security-group", payload, context)
        if reply.ok or failed_with(reply, "InvalidGroup.NotFound"):
            return
        if not failed_with(reply, "DependencyViolation"):
            raise ServiceFailure("service=delete_failed")
        if attempt == GROUP_ATTEMPTS - 1 or not _can_wait(context):
            break
        services.ports.pause(GROUP_PAUSE)
    raise ServiceFailure("service=security_group_in_use")


def _can_wait(context: RequestContext) -> bool:
    budget = context.budget
    spare = budget.limits.calls - budget.used
    return budget.remaining_seconds(context.lane) > GROUP_PAUSE + FINAL_SECONDS and (
        spare > FINAL_CALLS
    )


def _delete_cluster(resource: Resource, services: "AwsServices", context: RequestContext) -> None:
    commands = services.ports.commands
    _stop_tasks(resource.name, services, context)
    purge_revisions(commands, services.spec, context)
    payload = {"cluster": resource.name}
    require_data(call(commands, "ecs.delete-cluster", payload, context))


def _task_arns(
    cluster: str, status: str, services: "AwsServices", context: RequestContext
) -> list[str]:
    payload = {"cluster": cluster, "desiredStatus": status}
    data = require_data(call(services.ports.commands, "ecs.list-tasks", payload, context))
    arns = data.get("taskArns")
    if not isinstance(arns, list) or not all(isinstance(arn, str) and arn for arn in arns):
        raise ServiceFailure("service=inventory_invalid")
    return arns


def _stop_tasks(cluster: str, services: "AwsServices", context: RequestContext) -> None:
    running = _task_arns(cluster, "RUNNING", services, context)
    for arn in running:
        payload = {"cluster": cluster, "task": arn, "reason": "cnesdata capability cleanup"}
        require_data(call(services.ports.commands, "ecs.stop-task", payload, context))
    stopping = _task_arns(cluster, "STOPPED", services, context)
    pending = sorted({*running, *stopping})
    for _ in range(STOP_POLLS):
        pending = _still_active(cluster, pending, services, context)
        if not pending:
            return
        services.ports.pause(STOP_PAUSE)
    raise ServiceFailure("service=task_not_stopped")


def _still_active(
    cluster: str, arns: list[str], services: "AwsServices", context: RequestContext
) -> list[str]:
    if not arns:
        return []
    payload = {"cluster": cluster, "tasks": arns[:100]}
    data = require_data(call(services.ports.commands, "ecs.describe-tasks", payload, context))
    rows = rows_of(data, "tasks")
    stopped = {row.get("taskArn") for row in rows if row.get("lastStatus") == "STOPPED"}
    return [arn for arn in arns if arn not in stopped]


_DELETERS = {
    "ecr": _delete_ecr,
    "logs": _delete_logs,
    "ec2": _delete_group,
    "ecs": _delete_cluster,
}
