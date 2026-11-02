"""Espera limitada por ACTIVE da tabela e de todos os GSIs, compartilhada pelos probes."""

from collections.abc import Mapping

from tests.integration.aws_capability._service_commands import (
    call_command,
    is_remote_not_found,
    require_data,
)
from tests.integration.aws_capability._service_contracts import ServicePorts
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import RequestContext, ServiceFailure

READY_POLLS = 30
READY_SECONDS = 2.0


def await_active_table(
    ports: ServicePorts, name: str, context: RequestContext
) -> Mapping[str, object]:
    """Returns: descrição da tabela ACTIVE; Raises: ServiceFailure se não ativar a tempo."""
    request = Request("dynamodb", "describe-table", {"TableName": name})
    for poll in range(READY_POLLS):
        reply = call_command(ports.commands, request, context)
        if reply.ok or not is_remote_not_found(reply):
            table = require_data(reply).get("Table")
            if isinstance(table, Mapping) and _active(table):
                return table
        if poll < READY_POLLS - 1:
            ports.pause(READY_SECONDS)
    raise ServiceFailure("probe=table_not_ready")


def _active(table: Mapping[str, object]) -> bool:
    indexes = table.get("GlobalSecondaryIndexes")
    rows = indexes if isinstance(indexes, list) else []
    return table.get("TableStatus") == "ACTIVE" and all(
        isinstance(row, Mapping) and row.get("IndexStatus") == "ACTIVE" for row in rows
    )
