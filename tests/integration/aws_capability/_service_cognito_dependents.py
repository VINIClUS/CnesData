"""Descoberta conservadora e paginada dos filhos Cognito."""

from collections.abc import Mapping
from typing import TYPE_CHECKING

from tests.integration.aws_capability._resource_ids import cognito_child, parse_cognito_child
from tests.integration.aws_capability._safety import owned
from tests.integration.aws_capability._service_children_cursor import (
    ChildrenCursor,
    next_children_cursor,
    parse_children_cursor,
)
from tests.integration.aws_capability._service_cognito import _pool_arn
from tests.integration.aws_capability._service_cognito_children import read_branding
from tests.integration.aws_capability._service_commands import call_command, require_data
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
    "cognito.clients": (
        "list-user-pool-clients",
        "UserPoolClients",
        "ClientId",
        "client_id",
        "cognito.client",
        "NextToken",
        "MaxResults",
        60,
        "cognito.servers",
    ),
    "cognito.servers": (
        "list-resource-servers",
        "ResourceServers",
        "Identifier",
        "identifier",
        "cognito.server",
        "NextToken",
        "MaxResults",
        50,
        "cognito.users",
    ),
    "cognito.users": (
        "list-users",
        "Users",
        "Username",
        "username",
        "cognito.user",
        "PaginationToken",
        "Limit",
        60,
        None,
    ),
}


def cognito_dependents(
    parent: Resource, cursor: str | None, services: "AwsServices", context: RequestContext
) -> Page:
    """Returns:
    Uma coleção corrente, branding do client ou folha inspecionada.
    Raises:
    ServiceFailure: identidade, cursor, posse ou leitura inválida.
    """
    if isinstance(parent.resource_id, str) and parent.resource_id.lstrip().startswith("{"):
        if cursor is not None:
            raise ServiceFailure("service=cursor_invalid")
        root, kind, child = parse_cognito_child(parent, services.spec)
        services.inspect(parent, context)
        if kind != "cognito.client":
            return Page()
        branding_id = read_branding(services.ports.commands, root, child["client_id"], context)
        if branding_id is None:
            return Page()
        branding = cognito_child(
            root,
            services.spec,
            "cognito.branding",
            {"client_id": child["client_id"], "branding_id": branding_id},
        )
        return Page((branding,))
    state = parse_children_cursor(cursor, parent, services.spec)
    owner = services.inspect(parent, context)
    if not owned(parent, owner, services.spec):
        raise ServiceFailure("service=not_owned")
    return _pool_page(parent, state, services, context)


def _pool_page(
    parent: Resource, state: ChildrenCursor, services: "AwsServices", context: RequestContext
) -> Page:
    if state.kind == "cognito.domains":
        operation = "describe-user-pool"
        payload: dict[str, object] = {"UserPoolId": parent.resource_id}
    else:
        config = _LISTS[state.kind]
        operation = config[0]
        payload = {"UserPoolId": parent.resource_id, config[6]: config[7], **state.markers}
    reply = call_command(
        services.ports.commands, Request("cognito-idp", operation, payload), context
    )
    data = require_data(reply)
    if state.kind == "cognito.domains":
        resources = _domains(data, parent, services.spec)
        next_state = ChildrenCursor("cognito.clients", {}, state.seen)
    else:
        resources = _list_resources(data, parent, state.kind, services.spec)
        next_state = _continuation(data, state)
    cursor = None
    if next_state is not None:
        cursor = next_children_cursor(next_state, parent, services.spec)
    return Page(resources, cursor)


def _domains(data: Mapping[str, object], parent: Resource, spec: RunSpec) -> tuple[Resource, ...]:
    _pool_arn(data, spec, parent)
    pool = data["UserPool"]
    if not isinstance(pool, Mapping):
        raise ServiceFailure("service=inventory_invalid")
    resources: list[Resource] = []
    for key in ("Domain", "CustomDomain"):
        if key in pool:
            domain = pool[key]
            if not isinstance(domain, str) or not domain:
                raise ServiceFailure("service=inventory_invalid")
            resources.append(cognito_child(parent, spec, "cognito.domain", {"domain": domain}))
    if len(set(resources)) != len(resources):
        raise ServiceFailure("service=inventory_invalid")
    return tuple(resources)


def _list_resources(
    data: Mapping[str, object], parent: Resource, kind: str, spec: RunSpec
) -> tuple[Resource, ...]:
    config = _LISTS[kind]
    if set(data) - {config[1], config[5]} or config[1] not in data:
        raise ServiceFailure("service=inventory_invalid")
    rows = data[config[1]]
    if not isinstance(rows, list):
        raise ServiceFailure("service=inventory_invalid")
    resources: list[Resource] = []
    for row in rows:
        value = _row_id(row, parent, (config[2], kind != "cognito.users"))
        resources.append(cognito_child(parent, spec, config[4], {config[3]: value}))
    return tuple(resources)


def _row_id(row: object, parent: Resource, scope: tuple[str, bool]) -> str:
    if not isinstance(row, Mapping):
        raise ServiceFailure("service=inventory_invalid")
    if scope[1] and row.get("UserPoolId") != parent.resource_id:
        raise ServiceFailure("service=inventory_invalid")
    value = row.get(scope[0])
    if not isinstance(value, str) or not value:
        raise ServiceFailure("service=inventory_invalid")
    return value


def _continuation(data: Mapping[str, object], state: ChildrenCursor) -> ChildrenCursor | None:
    config = _LISTS[state.kind]
    if config[5] in data:
        token = data[config[5]]
        if not isinstance(token, str) or not token:
            raise ServiceFailure("service=inventory_invalid")
        return ChildrenCursor(state.kind, {config[5]: token}, state.seen)
    if config[8] is None:
        return None
    return ChildrenCursor(config[8], {}, state.seen)
