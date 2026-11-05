"""Leituras de ECR, Logs, ECS e EC2 do fargate-write: inventário, posse e ausência."""

import re
from collections.abc import Mapping

from tests.integration.aws_capability._safety import CAP_PREFIX
from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_contracts import CommandPort, Reply
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Ownership,
    Page,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

LEAVES = frozenset({"ecr", "logs", "ecs", "ec2"})
MAX_PAGES = 8
_GROUP_ID = re.compile(r"sg-[a-z0-9]+")


def call(
    commands: CommandPort,
    service: str,
    operation: str,
    payload: Mapping[str, object],
    context: RequestContext,
) -> Reply:
    return call_command(commands, Request(service, operation, dict(payload)), context)


def failed_with(reply: Reply, *codes: str) -> bool:
    return (
        not reply.ok
        and reply.error is not None
        and reply.error.origin == "service"
        and reply.error.code in codes
    )


def rows_of(data: Mapping[str, object], key: str) -> list[Mapping[str, object]]:
    rows = data.get(key)
    if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
        raise ServiceFailure("service=inventory_invalid")
    return rows


def text_of(row: Mapping[str, object], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value:
        raise ServiceFailure("service=inventory_invalid")
    return value


def next_token(data: Mapping[str, object], key: str) -> str | None:
    token = data.get(key)
    if token is None:
        return None
    if not isinstance(token, str) or not token:
        raise ServiceFailure("service=inventory_invalid")
    return token


def require_leaf(resource: Resource) -> None:
    valid = (
        resource.service in LEAVES
        and isinstance(resource.resource_id, str)
        and isinstance(resource.name, str)
        and bool(resource.resource_id)
        and bool(resource.name)
    )
    if valid and resource.service == "ec2":
        valid = _GROUP_ID.fullmatch(resource.resource_id) is not None
    elif valid:
        valid = resource.resource_id == resource.name
    if not valid:
        raise ServiceFailure("service=resource_invalid")


def list_leaf(
    service: str, commands: CommandPort, cursor: str | None, context: RequestContext
) -> Page:
    """Returns: página de recursos com nome cap; Raises: ServiceFailure para resposta inválida."""
    if cursor is not None and (not isinstance(cursor, str) or not cursor):
        raise ServiceFailure("service=cursor_invalid")
    return _LISTERS[service](commands, cursor, context)


def _list_ecr(commands: CommandPort, cursor: str | None, context: RequestContext) -> Page:
    payload: dict[str, object] = {"maxResults": 1000}
    if cursor is not None:
        payload["nextToken"] = cursor
    data = require_data(call(commands, "ecr", "describe-repositories", payload, context))
    names = [text_of(row, "repositoryName") for row in rows_of(data, "repositories")]
    resources = tuple(Resource("ecr", name, name) for name in names)
    return Page(resources, next_token(data, "nextToken"))


def _list_logs(commands: CommandPort, cursor: str | None, context: RequestContext) -> Page:
    payload: dict[str, object] = {"logGroupNamePrefix": CAP_PREFIX, "limit": 50}
    if cursor is not None:
        payload["nextToken"] = cursor
    data = require_data(call(commands, "logs", "describe-log-groups", payload, context))
    names = [text_of(row, "logGroupName") for row in rows_of(data, "logGroups")]
    resources = tuple(Resource("logs", name, name) for name in names)
    return Page(resources, next_token(data, "nextToken"))


def _list_ecs(commands: CommandPort, cursor: str | None, context: RequestContext) -> Page:
    payload: dict[str, object] = {"maxResults": 100}
    if cursor is not None:
        payload["nextToken"] = cursor
    data = require_data(call(commands, "ecs", "list-clusters", payload, context))
    arns = data.get("clusterArns")
    if not isinstance(arns, list) or not all(isinstance(arn, str) and arn for arn in arns):
        raise ServiceFailure("service=inventory_invalid")
    names = [arn.rsplit("/", 1)[-1] for arn in arns]
    resources = tuple(Resource("ecs", name, name) for name in names)
    return Page(resources, next_token(data, "nextToken"))


def _list_ec2(commands: CommandPort, cursor: str | None, context: RequestContext) -> Page:
    payload: dict[str, object] = {
        "Filters": [{"Name": "group-name", "Values": [CAP_PREFIX + "*"]}],
        "MaxResults": 1000,
    }
    if cursor is not None:
        payload["NextToken"] = cursor
    data = require_data(call(commands, "ec2", "describe-security-groups", payload, context))
    rows = rows_of(data, "SecurityGroups")
    resources = tuple(
        Resource("ec2", text_of(row, "GroupId"), text_of(row, "GroupName")) for row in rows
    )
    return Page(resources, next_token(data, "NextToken"))


_LISTERS = {"ecr": _list_ecr, "logs": _list_logs, "ecs": _list_ecs, "ec2": _list_ec2}


def lookup_leaf(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership | None:
    """Returns: posse fresca ou None para ausência conclusiva; Raises: ServiceFailure."""
    require_leaf(resource)
    return _READERS[resource.service](commands, spec, resource, context)


def inspect_leaf(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership:
    owner = lookup_leaf(commands, spec, resource, context)
    if owner is None:
        raise ServiceFailure("service=not_found")
    return owner


def _tags_from_pairs(rows: object, key: str, value: str) -> dict[str, str]:
    if not isinstance(rows, list):
        raise ServiceFailure("service=tags_invalid")
    found: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, Mapping) or set(row) != {key, value}:
            raise ServiceFailure("service=tags_invalid")
        name, content = row[key], row[value]
        if not isinstance(name, str) or not isinstance(content, str) or name in found:
            raise ServiceFailure("service=tags_invalid")
        found[name] = content
    return found


def _region_of(arn: str, expected: str) -> str:
    parts = arn.split(":")
    if len(parts) < 6 or parts[0] != "arn" or not parts[3]:
        raise ServiceFailure("service=describe_invalid")
    if not parts[5].startswith(expected):
        raise ServiceFailure("service=describe_invalid")
    return parts[3]


def _read_ecr(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership | None:
    payload = {"repositoryNames": [resource.name]}
    reply = call(commands, "ecr", "describe-repositories", payload, context)
    if failed_with(reply, "RepositoryNotFoundException"):
        return None
    rows = rows_of(require_data(reply), "repositories")
    if len(rows) != 1 or rows[0].get("repositoryName") != resource.name:
        raise ServiceFailure("service=describe_invalid")
    arn = text_of(rows[0], "repositoryArn")
    region = _region_of(arn, "repository/")
    tags = require_data(
        call(commands, "ecr", "list-tags-for-resource", {"resourceArn": arn}, context)
    )
    return Ownership(
        text_of(rows[0], "registryId"), _tags_from_pairs(tags.get("tags"), "Key", "Value"), region
    )


def _read_logs(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership | None:
    group = _find_log_group(commands, resource.name, context)
    if group is None:
        return None
    arn = text_of(group, "arn").removesuffix(":*")
    region = _region_of(arn, "log-group")
    reply = call(commands, "logs", "list-tags-for-resource", {"resourceArn": arn}, context)
    tags = require_data(reply).get("tags")
    if not isinstance(tags, Mapping) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in tags.items()
    ):
        raise ServiceFailure("service=tags_invalid")
    return Ownership(arn.split(":")[4], dict(tags), region)


def _find_log_group(
    commands: CommandPort, name: str, context: RequestContext
) -> Mapping[str, object] | None:
    payload: dict[str, object] = {"logGroupNamePrefix": name, "limit": 50}
    for _ in range(MAX_PAGES):
        data = require_data(call(commands, "logs", "describe-log-groups", payload, context))
        for row in rows_of(data, "logGroups"):
            if row.get("logGroupName") == name:
                return row
        token = next_token(data, "nextToken")
        if token is None:
            return None
        payload["nextToken"] = token
    raise ServiceFailure("service=inventory_invalid")


def _read_ecs(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership | None:
    payload = {"clusters": [resource.name], "include": ["TAGS"]}
    data = require_data(call(commands, "ecs", "describe-clusters", payload, context))
    rows = rows_of(data, "clusters")
    if not rows:
        if any(row.get("reason") == "MISSING" for row in rows_of(data, "failures")):
            return None
        raise ServiceFailure("service=describe_invalid")
    row = rows[0]
    if len(rows) != 1 or row.get("clusterName") != resource.name:
        raise ServiceFailure("service=describe_invalid")
    if row.get("status") == "INACTIVE":
        return None
    if row.get("status") != "ACTIVE":
        raise ServiceFailure("service=describe_invalid")
    arn = text_of(row, "clusterArn")
    region = _region_of(arn, "cluster/")
    tags = _tags_from_pairs(row.get("tags", []), "key", "value")
    return Ownership(arn.split(":")[4], tags, region)


def describe_group(
    commands: CommandPort, resource: Resource, context: RequestContext
) -> Mapping[str, object] | None:
    """Returns: linha do security group ou None se ausente; Raises: ServiceFailure."""
    require_leaf(resource)
    reply = call(
        commands, "ec2", "describe-security-groups", {"GroupIds": [resource.resource_id]}, context
    )
    if failed_with(reply, "InvalidGroup.NotFound"):
        return None
    rows = rows_of(require_data(reply), "SecurityGroups")
    if len(rows) != 1 or rows[0].get("GroupId") != resource.resource_id:
        raise ServiceFailure("service=describe_invalid")
    if rows[0].get("GroupName") != resource.name:
        raise ServiceFailure("service=resource_invalid")
    return rows[0]


def _read_ec2(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership | None:
    row = describe_group(commands, resource, context)
    if row is None:
        return None
    tags = _tags_from_pairs(row.get("Tags", []), "Key", "Value")
    return Ownership(text_of(row, "OwnerId"), tags, spec.region)


_READERS = {"ecr": _read_ecr, "logs": _read_logs, "ecs": _read_ecs, "ec2": _read_ec2}
