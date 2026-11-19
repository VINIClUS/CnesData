"""Inventário e cursores de tabelas DynamoDB."""

from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability._service_cursor import next_cursor, parse_cursor
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Page,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def list_tables(
    commands: CommandPort, spec: RunSpec, value: str | None, context: RequestContext
) -> Page:
    cursor = parse_cursor(value, spec)
    payload: dict[str, object] = {"Limit": 100}
    if cursor.marker is not None:
        payload["ExclusiveStartTableName"] = cursor.marker
    reply = call_command(commands, Request("dynamodb", "list-tables", payload), context)
    data = require_data(reply)
    names = data.get("TableNames")
    marker: str | None = None
    if not isinstance(names, list) or any(not isinstance(name, str) or not name for name in names):
        raise ServiceFailure("service=inventory_invalid")
    if set(data) - {"TableNames", "LastEvaluatedTableName"}:
        raise ServiceFailure("service=inventory_invalid")
    if "LastEvaluatedTableName" in data:
        candidate = data["LastEvaluatedTableName"]
        if not isinstance(candidate, str) or not candidate:
            raise ServiceFailure("service=inventory_invalid")
        marker = candidate
    resources = tuple(Resource("dynamodb", name, name) for name in names)
    return Page(resources, next_cursor(marker, cursor.seen, spec.run_id))
