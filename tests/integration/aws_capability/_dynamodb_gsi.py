"""Propagação limitada de GSI esparso com rejeição remota de leitura forte."""

import json
import logging
import time
from collections.abc import Mapping

from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

logger = logging.getLogger(__name__)


def verify_gsi(
    commands: CommandPort, spec: RunSpec, root: Resource, context: RequestContext
) -> Mapping[str, object]:
    """Args: commands, spec, root, context: porta, execução, tabela e orçamento.
    Returns: estado e polls da inclusão e da remoção; código da leitura forte rejeitada.
    Raises: ServiceFailure para resposta inválida; BudgetExhausted ao esgotar work.
    """
    partition = f"cap#{spec.run_id}"
    indexed = _indexed(partition)
    sparse = {"pk": {"S": f"{partition}#gsi"}, "sk": {"S": "sparse"}}
    for item in (indexed, sparse):
        request = Request("dynamodb", "put-item", {"TableName": root.name, "Item": item})
        require_data(call_command(commands, request, context))
    for item in (indexed, sparse):
        _get(commands, root, item, context)
    request = Request("dynamodb", "query", _query(root, partition))
    include = _poll(commands, request, (indexed, False), context)
    key = {name: indexed[name] for name in ("pk", "sk")}
    remove = Request(
        "dynamodb",
        "update-item",
        {
            "TableName": root.name,
            "Key": key,
            "UpdateExpression": "REMOVE #gpk, #gsk",
            "ExpressionAttributeNames": {"#gpk": "gsi6pk", "#gsk": "gsi6sk"},
        },
    )
    require_data(call_command(commands, remove, context))
    _get(commands, root, key, context)
    removal = _poll(commands, request, (indexed, True), context)
    _reject_strong(commands, request, context)
    return {
        "gsi_include_status": "observed",
        "gsi_include_polls": include,
        "gsi_remove_status": "observed",
        "gsi_remove_polls": removal,
        "gsi_consistent_read_code": "ValidationException",
    }


def _indexed(partition: str) -> dict[str, object]:
    return {
        "pk": {"S": f"{partition}#gsi"},
        "sk": {"S": "indexed"},
        "gsi6pk": {"S": f"{partition}#pending"},
        "gsi6sk": {"S": "item#001"},
    }


def _query(root: Resource, partition: str) -> dict[str, object]:
    return {
        "TableName": root.name,
        "IndexName": "gsi6",
        "KeyConditionExpression": "#gpk=:gpk",
        "ExpressionAttributeNames": {"#gpk": "gsi6pk"},
        "ExpressionAttributeValues": {":gpk": {"S": f"{partition}#pending"}},
        "Limit": 2,
    }


def _get(
    commands: CommandPort, root: Resource, item: Mapping[str, object], context: RequestContext
) -> None:
    request = Request(
        "dynamodb",
        "get-item",
        {
            "TableName": root.name,
            "Key": {name: item[name] for name in ("pk", "sk")},
            "ConsistentRead": True,
        },
    )
    data = require_data(call_command(commands, request, context))
    if json.dumps(data.get("Item"), sort_keys=True) != json.dumps(item, sort_keys=True):
        raise ServiceFailure("probe=ddb_gsi_response_invalid")


def _poll(
    commands: CommandPort,
    request: Request,
    target: tuple[Mapping[str, object], bool],
    context: RequestContext,
) -> int:
    indexed, removal = target
    pages = 0
    for poll in range(1, 5):
        rows, consumed = _read(commands, request, indexed, context)
        pages += consumed
        present = _validate_rows(rows, indexed)
        if present != removal:
            _observe(removal, "observed", poll, pages)
            return poll
        if poll < 4:
            _sleep(context)
    _observe(removal, "unobserved", 4, pages)
    reason = "probe=ddb_gsi_removal_unobserved" if removal else "probe=ddb_gsi_inclusion_unobserved"
    raise ServiceFailure(reason)


def _read(
    commands: CommandPort,
    request: Request,
    indexed: Mapping[str, object],
    context: RequestContext,
) -> tuple[list[object], int]:
    rows: list[object] = []
    seen: set[str] = set()
    for page_number in range(1, 3):
        data = require_data(call_command(commands, request, context))
        page = data.get("Items")
        if not isinstance(page, list):
            raise ServiceFailure("probe=ddb_gsi_response_invalid")
        rows.extend(page)
        _validate_rows(rows, indexed)
        cursor = _cursor(data, indexed, seen)
        if cursor is None:
            return rows, page_number
        request = Request(
            request.service, request.operation, {**request.payload, "ExclusiveStartKey": cursor}
        )
    raise ServiceFailure("probe=ddb_gsi_pages_exhausted")


def _cursor(
    data: Mapping[str, object], indexed: Mapping[str, object], seen: set[str]
) -> Mapping[str, object] | None:
    cursor = data.get("LastEvaluatedKey", {})
    if not isinstance(cursor, Mapping):
        raise ServiceFailure("probe=ddb_gsi_cursor_invalid")
    if not cursor:
        return None
    second = {**indexed, "sk": {"S": "indexed#002"}, "gsi6sk": {"S": "item#002"}}
    encoded = json.dumps(cursor, sort_keys=True)
    if encoded not in [json.dumps(item, sort_keys=True) for item in (indexed, second)]:
        raise ServiceFailure("probe=ddb_gsi_cursor_invalid")
    if encoded in seen:
        raise ServiceFailure("probe=ddb_gsi_cursor_invalid")
    seen.add(encoded)
    return cursor


def _validate_rows(rows: list[object], indexed: Mapping[str, object]) -> bool:
    if not rows:
        return False
    if len(rows) != 1 or json.dumps(rows[0], sort_keys=True) != json.dumps(indexed, sort_keys=True):
        raise ServiceFailure("probe=ddb_gsi_response_invalid")
    return True


def _sleep(context: RequestContext) -> None:
    context.budget.check(context.lane)
    time.sleep(min(1.0, context.budget.remaining_seconds(context.lane)))
    context.budget.check(context.lane)


def _observe(removal: bool, status: str, polls: int, pages: int) -> None:
    if removal:
        logger.info(
            "probe=ddb_ttl_gsi observation=gsi_remove status=%s polls=%d pages=%d",
            status,
            polls,
            pages,
        )
    else:
        logger.info(
            "probe=ddb_ttl_gsi observation=gsi_include status=%s polls=%d pages=%d",
            status,
            polls,
            pages,
        )


def _reject_strong(commands: CommandPort, request: Request, context: RequestContext) -> None:
    request = Request(
        request.service, request.operation, {**request.payload, "ConsistentRead": True}
    )
    reply = call_command(commands, request, context)
    error = reply.error
    if (
        reply.ok
        or error is None
        or error.origin != "service"
        or error.code != "ValidationException"
    ):
        raise ServiceFailure("probe=ddb_gsi_rejection_invalid")
    logger.info(
        "probe=ddb_ttl_gsi observation=gsi_consistent_read "
        "status=remote_rejected code=ValidationException"
    )
