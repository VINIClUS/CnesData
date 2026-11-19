"""Leituras frescas de roles IAM, ownership e paginação de tags."""

from collections.abc import Mapping

from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_contracts import CommandPort, Reply
from tests.integration.aws_capability._service_cursor import next_iam_cursor, parse_iam_cursor
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Ownership,
    Page,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

_PATH = "/cnesdata-cap/"


def list_roles(
    commands: CommandPort, spec: RunSpec, value: str | None, context: RequestContext
) -> Page:
    """Returns:
        Página IAM e cursor fechado.
    Raises:
        ServiceFailure: cursor ou resposta inválida.
    """
    cursor = parse_iam_cursor(value, spec)
    payload: dict[str, object] = {"MaxItems": 100, "PathPrefix": _PATH}
    if cursor.marker is not None:
        payload["Marker"] = cursor.marker
    data = require_data(call_command(commands, Request("iam", "list-roles", payload), context))
    roles = data.get("Roles")
    if not isinstance(roles, list):
        raise ServiceFailure("service=inventory_invalid")
    resources = tuple(_role_resource(role) for role in roles)
    marker = _continuation(data, "inventory_invalid")
    return Page(resources, next_iam_cursor(marker, cursor.seen, spec.run_id))


def lookup_role(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership | None:
    """Returns:
        Ownership global ou None para NoSuchEntity remoto em GetRole.
    Raises:
        ServiceFailure: recurso ou readback inconclusivo.
    """
    _require_root(resource)
    try:
        reply = call_command(
            commands, Request("iam", "get-role", {"RoleName": resource.resource_id}), context
        )
    except ServiceFailure:
        raise ServiceFailure("service=describe_failed") from None
    if not reply.ok:
        if _remote_no_entity(reply):
            return None
        raise ServiceFailure("service=describe_failed")
    _validate_role(reply.data, spec, resource)
    tags = _read_tags(commands, resource, context)
    return Ownership(spec.account, tags, "aws-global")


def inspect_role(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership:
    """Returns:
        Ownership fresco de role presente.
    Raises:
        ServiceFailure: ausência conclusiva ou leitura inconclusiva.
    """
    owner = lookup_role(commands, spec, resource, context)
    if owner is None:
        raise ServiceFailure("service=not_found")
    return owner


def _role_resource(role: object) -> Resource:
    if not isinstance(role, Mapping):
        raise ServiceFailure("service=inventory_invalid")
    name = role.get("RoleName")
    path = role.get("Path")
    if not isinstance(name, str) or not name:
        raise ServiceFailure("service=inventory_invalid")
    if "Path" in role and (not isinstance(path, str) or not path.startswith(_PATH)):
        raise ServiceFailure("service=inventory_invalid")
    return Resource("iam", name, name)


def _continuation(data: Mapping[str, object], reason: str) -> str | None:
    truncated = data.get("IsTruncated")
    marker_present = "Marker" in data
    if "IsTruncated" not in data:
        if marker_present:
            raise ServiceFailure(f"service={reason}")
        return None
    if type(truncated) is not bool:
        raise ServiceFailure(f"service={reason}")
    if not truncated:
        if marker_present:
            raise ServiceFailure(f"service={reason}")
        return None
    marker = data.get("Marker")
    if not isinstance(marker, str) or not marker:
        raise ServiceFailure(f"service={reason}")
    return marker


def _require_root(resource: Resource) -> None:
    if (
        resource.service != "iam"
        or not isinstance(resource.resource_id, str)
        or not resource.resource_id
        or not isinstance(resource.name, str)
        or not resource.name
        or resource.resource_id != resource.name
    ):
        raise ServiceFailure("service=resource_invalid")


def _remote_no_entity(reply: Reply) -> bool:
    return (
        not reply.ok
        and reply.error is not None
        and reply.error.origin == "service"
        and reply.error.code == "NoSuchEntity"
    )


def _validate_role(data: Mapping[str, object], spec: RunSpec, resource: Resource) -> None:
    role = data.get("Role")
    if not isinstance(role, Mapping):
        raise ServiceFailure("service=describe_invalid")
    expected_arn = f"arn:aws:iam::{spec.account}:role/cnesdata-cap/{resource.resource_id}"
    if (
        role.get("RoleName") != resource.resource_id
        or role.get("Path") != _PATH
        or role.get("Arn") != expected_arn
    ):
        raise ServiceFailure("service=describe_invalid")


def _read_tags(
    commands: CommandPort, resource: Resource, context: RequestContext
) -> dict[str, str]:
    tags: dict[str, str] = {}
    seen: set[str] = set()
    marker: str | None = None
    for _ in range(context.budget.limits.pages):
        payload: dict[str, object] = {"RoleName": resource.resource_id, "MaxItems": 100}
        if marker is not None:
            payload["Marker"] = marker
        data = require_data(
            call_command(commands, Request("iam", "list-role-tags", payload), context)
        )
        rows = data.get("Tags")
        if not isinstance(rows, list):
            raise ServiceFailure("service=tags_invalid")
        for row in rows:
            if not isinstance(row, Mapping) or set(row) != {"Key", "Value"}:
                raise ServiceFailure("service=tags_invalid")
            key, value = row["Key"], row["Value"]
            if not isinstance(key, str) or not isinstance(value, str):
                raise ServiceFailure("service=tags_invalid")
            if key in tags:
                raise ServiceFailure("service=tags_ambiguous")
            tags[key] = value
        marker = _continuation(data, "tags_invalid")
        if marker is None:
            return tags
        if marker in seen:
            raise ServiceFailure("service=tags_invalid")
        seen.add(marker)
    raise ServiceFailure("service=tags_invalid")
