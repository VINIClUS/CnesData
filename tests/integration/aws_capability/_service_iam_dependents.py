"""Descoberta IAM paginada com bloqueio conservador de dependências externas."""

from collections.abc import Mapping
from typing import TYPE_CHECKING

from tests.integration.aws_capability._resource_ids import iam_child, valid_iam_policy_name
from tests.integration.aws_capability._safety import owned
from tests.integration.aws_capability._service_children_cursor import (
    IamChildrenCursor,
    next_iam_children_cursor,
    parse_iam_children_cursor,
)
from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_iam import _continuation, _require_root
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Page,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

if TYPE_CHECKING:
    from tests.integration.aws_capability.services import AwsServices

_LISTS = {
    "iam.policies": ("list-role-policies", "PolicyNames", "iam.attachments"),
    "iam.attachments": ("list-attached-role-policies", "AttachedPolicies", "iam.profiles"),
    "iam.profiles": ("list-instance-profiles-for-role", "InstanceProfiles", None),
}


def iam_dependents(
    parent: Resource, cursor: str | None, services: "AwsServices", context: RequestContext
) -> Page:
    """Returns:
    Uma página inline, transição de coleção ou folha reinspecionada.
    Raises:
    ServiceFailure: leitura inválida, posse insuficiente ou dependência externa.
    """
    if parent.resource_id != parent.name:
        if cursor is not None:
            raise ServiceFailure("service=cursor_invalid")
        services.inspect(parent, context)
        return Page()
    _require_root(parent)
    state = parse_iam_children_cursor(cursor, parent, services.spec)
    owner = services.inspect(parent, context)
    if not owned(parent, owner, services.spec):
        raise ServiceFailure("service=not_owned")
    config = _LISTS[state.kind]
    payload = {"RoleName": parent.resource_id, "MaxItems": 100, **state.markers}
    data = require_data(
        call_command(services.ports.commands, Request("iam", config[0], payload), context)
    )
    resources = _resources(data, state.kind, parent, services.spec)
    next_state = _next_state(data, state)
    if state.kind != "iam.policies" and data[config[1]]:
        raise ServiceFailure("service=unexpected_external_dependency")
    next_cursor = None
    if next_state is not None:
        next_cursor = next_iam_children_cursor(next_state, parent, services.spec)
    return Page(resources, next_cursor)


def _resources(
    data: Mapping[str, object], kind: str, parent: Resource, spec: RunSpec
) -> tuple[Resource, ...]:
    key = _LISTS[kind][1]
    rows = data.get(key)
    if set(data) - {key, "IsTruncated", "Marker"} or not isinstance(rows, list):
        raise ServiceFailure("service=inventory_invalid")
    resources: list[Resource] = []
    for row in rows:
        if kind == "iam.policies":
            if not valid_iam_policy_name(row):
                raise ServiceFailure("service=inventory_invalid")
            resources.append(iam_child(parent, spec, row))
        else:
            _require_external(row, kind)
    return tuple(resources)


def _require_external(row: object, kind: str) -> None:
    if not isinstance(row, Mapping):
        raise ServiceFailure("service=inventory_invalid")
    if kind == "iam.attachments":
        if not valid_iam_policy_name(row.get("PolicyName")):
            raise ServiceFailure("service=inventory_invalid")
        keys = ("PolicyArn",)
    else:
        keys = ("Path", "InstanceProfileName", "InstanceProfileId", "Arn", "CreateDate")
        roles = row.get("Roles")
        if not isinstance(roles, list) or any(not isinstance(role, Mapping) for role in roles):
            raise ServiceFailure("service=inventory_invalid")
    if any(not isinstance(row.get(key), str) or not row[key] for key in keys):
        raise ServiceFailure("service=inventory_invalid")


def _next_state(data: Mapping[str, object], state: IamChildrenCursor) -> IamChildrenCursor | None:
    marker = _continuation(data, "inventory_invalid")
    if marker is not None:
        return IamChildrenCursor(state.kind, {"Marker": marker}, state.seen)
    kind = _LISTS[state.kind][2]
    if kind is None:
        return None
    return IamChildrenCursor(kind, {}, state.seen)
