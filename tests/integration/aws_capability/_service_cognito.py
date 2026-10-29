"""Leituras frescas de pools Cognito, tags e cursores."""

import re
from collections.abc import Mapping

from tests.integration.aws_capability._service_commands import (
    call_command,
    is_remote_not_found,
    require_data,
    require_error,
)
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability._service_cursor import (
    next_cognito_cursor,
    parse_cognito_cursor,
)
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Ownership,
    Page,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def list_pools(
    commands: CommandPort, spec: RunSpec, value: str | None, context: RequestContext
) -> Page:
    cursor = parse_cognito_cursor(value, spec)
    payload: dict[str, object] = {"MaxResults": 60}
    if cursor.marker is not None:
        payload["NextToken"] = cursor.marker
    reply = call_command(commands, Request("cognito-idp", "list-user-pools", payload), context)
    data = require_data(reply)
    if set(data) - {"UserPools", "NextToken"} or "UserPools" not in data:
        raise ServiceFailure("service=inventory_invalid")
    pools = data["UserPools"]
    if not isinstance(pools, list):
        raise ServiceFailure("service=inventory_invalid")
    resources = tuple(_pool_resource(pool) for pool in pools)
    token = _next_token(data)
    return Page(resources, next_cognito_cursor(token, cursor.seen, spec.run_id))


def lookup_pool(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership | None:
    _require_root(resource, spec)
    reply = call_command(
        commands,
        Request("cognito-idp", "describe-user-pool", {"UserPoolId": resource.resource_id}),
        context,
    )
    if not reply.ok:
        if is_remote_not_found(reply):
            return None
        require_error(reply)
        raise ServiceFailure("service=describe_failed")
    data = require_data(reply)
    arn = _pool_arn(data, spec, resource)
    tags = _read_tags(commands, spec, arn, context)
    return Ownership(spec.account, tags, spec.region)


def inspect_pool(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership:
    owner = lookup_pool(commands, spec, resource, context)
    if owner is None:
        raise ServiceFailure("service=not_found")
    return owner


def _pool_resource(pool: object) -> Resource:
    if not isinstance(pool, Mapping):
        raise ServiceFailure("service=inventory_invalid")
    resource_id, name = pool.get("Id"), pool.get("Name")
    if not isinstance(resource_id, str) or not resource_id:
        raise ServiceFailure("service=inventory_invalid")
    if not isinstance(name, str) or not name:
        raise ServiceFailure("service=inventory_invalid")
    return Resource("cognito", resource_id, name)


def _next_token(data: Mapping[str, object]) -> str | None:
    if "NextToken" not in data:
        return None
    token = data["NextToken"]
    if not isinstance(token, str) or not token:
        raise ServiceFailure("service=inventory_invalid")
    return token


def _require_root(resource: Resource, spec: RunSpec) -> None:
    expected_id = f"{spec.region}_{spec.run_id}"
    expected_name = f"cnesdata-cap-{spec.run_id}"
    if (
        resource.service != "cognito"
        or not _is_regional_pool_id(resource.resource_id, spec.region)
        or not isinstance(resource.name, str)
        or not resource.name
    ):
        raise ServiceFailure("service=resource_invalid")
    if resource.resource_id == expected_id and resource.name != expected_name:
        raise ServiceFailure("service=resource_invalid")


def _is_regional_pool_id(value: object, region: str) -> bool:
    pattern = rf"{re.escape(region)}_[A-Za-z0-9]+"
    return isinstance(value, str) and re.fullmatch(pattern, value) is not None


def _pool_arn(data: Mapping[str, object], spec: RunSpec, resource: Resource) -> str:
    pool = data.get("UserPool")
    if not isinstance(pool, Mapping):
        raise ServiceFailure("service=describe_invalid")
    pool_id, name, arn = pool.get("Id"), pool.get("Name"), pool.get("Arn")
    expected = f"arn:aws:cognito-idp:{spec.region}:{spec.account}:userpool/{resource.resource_id}"
    if pool_id != resource.resource_id or name != resource.name or arn != expected:
        raise ServiceFailure("service=describe_invalid")
    return expected


def _read_tags(
    commands: CommandPort, spec: RunSpec, arn: str, context: RequestContext
) -> dict[str, str]:
    reply = call_command(
        commands,
        Request("cognito-idp", "list-tags-for-resource", {"ResourceArn": arn}),
        context,
    )
    data = require_data(reply)
    if set(data) != {"Tags"} or not isinstance(data["Tags"], Mapping):
        raise ServiceFailure("service=tags_invalid")
    tags = data["Tags"]
    if any(not isinstance(key, str) or not isinstance(value, str) for key, value in tags.items()):
        raise ServiceFailure("service=tags_invalid")
    return dict(tags)
