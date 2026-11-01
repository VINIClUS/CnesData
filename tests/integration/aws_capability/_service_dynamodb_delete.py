"""Exclusão DynamoDB com ownership fresco e aceitação vinculada."""

from collections.abc import Mapping

from tests.integration.aws_capability._safety import run_name, tags
from tests.integration.aws_capability._service_commands import (
    call_command,
    is_remote_not_found,
    require_data,
    require_error,
)
from tests.integration.aws_capability._service_contracts import CommandPort, ServicePorts
from tests.integration.aws_capability._service_dynamodb import _read_tags, _table_arn
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def delete_table(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> None:
    """Raises: ServiceFailure para exclusão inválida; BudgetExhausted ao esgotar cleanup."""
    if (
        context.lane != "cleanup"
        or resource.service != "dynamodb"
        or resource.name != run_name(spec)
        or resource.resource_id != resource.name
    ):
        raise ServiceFailure("service=ddb_delete_invalid")
    table = _describe(commands, spec, resource, context)
    _require_owner(commands, spec, resource, context)
    if table["DeletionProtectionEnabled"] is True:
        request = Request(
            "dynamodb",
            "update-table",
            {"TableName": resource.name, "DeletionProtectionEnabled": False},
        )
        data = require_data(call_command(commands, request, context))
        updated = _bound_table(data.get("TableDescription"), spec, resource)
        if (
            updated.get("TableStatus") not in ("ACTIVE", "UPDATING")
            or updated.get("DeletionProtectionEnabled") is not False
        ):
            raise ServiceFailure("service=ddb_delete_state")
        current = _describe(commands, spec, resource, context)
        if current["DeletionProtectionEnabled"] is not False:
            raise ServiceFailure("service=ddb_delete_state")
        _require_owner(commands, spec, resource, context)
    request = Request("dynamodb", "delete-table", {"TableName": resource.name})
    data = require_data(call_command(commands, request, context))
    deleted = _bound_table(data.get("TableDescription"), spec, resource)
    if deleted.get("TableStatus") != "DELETING":
        raise ServiceFailure("service=ddb_delete_state")


def _bound_table(raw: object, spec: RunSpec, resource: Resource) -> Mapping[str, object]:
    if not isinstance(raw, Mapping):
        raise ServiceFailure("service=ddb_delete_state")
    try:
        _table_arn({"Table": raw}, spec, resource)
    except ServiceFailure:
        raise ServiceFailure("service=ddb_delete_state") from None
    return raw


def _describe(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Mapping[str, object]:
    request = Request("dynamodb", "describe-table", {"TableName": resource.name})
    reply = call_command(commands, request, context)
    if not reply.ok:
        if is_remote_not_found(reply):
            raise ServiceFailure("service=ddb_delete_state")
        require_error(reply)
        raise ServiceFailure("service=describe_failed")
    table = _bound_table(require_data(reply).get("Table"), spec, resource)
    if (
        table.get("TableStatus") != "ACTIVE"
        or type(table.get("DeletionProtectionEnabled")) is not bool
    ):
        raise ServiceFailure("service=ddb_delete_state")
    return table


def _require_owner(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> None:
    arn = f"arn:aws:dynamodb:{spec.region}:{spec.account}:table/{resource.name}"
    current = _read_tags(commands, spec, arn, context)
    if any(current.get(key) != value for key, value in tags(spec).items()):
        raise ServiceFailure("service=ddb_delete_owner")


POLL_SECONDS = 2.0
POLL_ATTEMPTS = 30


def await_table_gone(
    ports: ServicePorts, spec: RunSpec, resource: Resource, context: RequestContext
) -> None:
    """Raises: ServiceFailure para status diferente de DELETING ou erro diferente de ausência."""
    request = Request("dynamodb", "describe-table", {"TableName": resource.name})
    for attempt in range(POLL_ATTEMPTS):
        reply = call_command(ports.commands, request, context)
        if not reply.ok:
            if is_remote_not_found(reply):
                return
            require_error(reply)
            raise ServiceFailure("service=describe_failed")
        table = _bound_table(require_data(reply).get("Table"), spec, resource)
        if table.get("TableStatus") != "DELETING":
            raise ServiceFailure("service=ddb_delete_state")
        last = attempt == POLL_ATTEMPTS - 1
        if last or context.budget.remaining_seconds(context.lane) <= POLL_SECONDS:
            return
        ports.pause(POLL_SECONDS)
