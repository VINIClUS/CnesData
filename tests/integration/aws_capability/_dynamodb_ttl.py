"""Presença TTL em leituras fortes completas e limitadas."""

import json
import logging
from collections.abc import Mapping, Sequence

from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_contracts import CommandPort, ServicePorts
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

logger = logging.getLogger(__name__)


def verify_ttl(ports: ServicePorts, spec: RunSpec, root: Resource, context: RequestContext) -> None:
    """Args: ports, spec, root, context: portas, execução, tabela e orçamento.
    Returns: None após observações parciais.
    Raises: ServiceFailure para resposta inválida; BudgetExhausted ao esgotar work.
    """
    anchor = int(ports.now())
    partition = f"cap#{spec.run_id}#ttl"
    expired = _item(partition, "expired", anchor - 3600)
    current = _item(partition, "current", anchor + 3600)
    for item in (expired, current):
        request = Request("dynamodb", "put-item", {"TableName": root.name, "Item": item})
        require_data(call_command(ports.commands, request, context))
    present = _get(ports.commands, root, expired, context)
    if not _get(ports.commands, root, current, context):
        raise ServiceFailure("probe=ddb_ttl_response_invalid")
    logger.info(
        "probe=ddb_ttl_gsi observation=ttl_get expired_present=%s current_present=%s", present, True
    )
    payload = {"TableName": root.name, "ConsistentRead": True, "Limit": 2}
    query = {
        **payload,
        "KeyConditionExpression": "#pk=:pk",
        "ExpressionAttributeNames": {"#pk": "pk"},
        "ExpressionAttributeValues": {":pk": {"S": partition}},
    }
    for operation, body in (("query", query), ("scan", payload)):
        rows = _read(ports.commands, Request("dynamodb", operation, body), partition, context)
        present = _validate_rows(rows, (expired, current))
        _observe(operation, present)


def _item(partition: str, sort_key: str, expires: int) -> Mapping[str, object]:
    return {"pk": {"S": partition}, "sk": {"S": sort_key}, "expires_at": {"N": str(expires)}}


def _get(
    commands: CommandPort, root: Resource, item: Mapping[str, object], context: RequestContext
) -> bool:
    key = {name: item[name] for name in ("pk", "sk")}
    request = Request(
        "dynamodb", "get-item", {"TableName": root.name, "Key": key, "ConsistentRead": True}
    )
    data = require_data(call_command(commands, request, context))
    if "Item" not in data:
        return False
    if json.dumps(data["Item"], sort_keys=True) != json.dumps(item, sort_keys=True):
        raise ServiceFailure("probe=ddb_ttl_response_invalid")
    return True


def _read(
    commands: CommandPort, request: Request, partition: str, context: RequestContext
) -> list[object]:
    rows: list[object] = []
    seen: set[str] = set()
    for _ in range(2):
        data = require_data(call_command(commands, request, context))
        page = data.get("Items")
        if not isinstance(page, list):
            raise ServiceFailure("probe=ddb_ttl_response_invalid")
        rows.extend(page)
        cursor = _cursor(data, partition, seen)
        if cursor is None:
            return rows
        request = Request(
            request.service, request.operation, {**request.payload, "ExclusiveStartKey": cursor}
        )
    raise ServiceFailure("probe=ddb_ttl_pages_exhausted")


def _cursor(
    data: Mapping[str, object], partition: str, seen: set[str]
) -> Mapping[str, object] | None:
    cursor = data.get("LastEvaluatedKey", {})
    if not isinstance(cursor, Mapping):
        raise ServiceFailure("probe=ddb_ttl_cursor_invalid")
    if not cursor:
        return None
    encoded = json.dumps(cursor, sort_keys=True)
    allowed = [
        json.dumps({"pk": {"S": partition}, "sk": {"S": key}}, sort_keys=True)
        for key in ("expired", "current")
    ]
    if encoded not in allowed or encoded in seen:
        raise ServiceFailure("probe=ddb_ttl_cursor_invalid")
    seen.add(encoded)
    return cursor


def _validate_rows(rows: Sequence[object], expected: Sequence[Mapping[str, object]]) -> bool:
    actual = [json.dumps(row, sort_keys=True) for row in rows]
    expired, current = [json.dumps(item, sort_keys=True) for item in expected]
    if len(set(actual)) != len(actual) or current not in actual:
        raise ServiceFailure("probe=ddb_ttl_response_invalid")
    if any(row not in (expired, current) for row in actual):
        raise ServiceFailure("probe=ddb_ttl_response_invalid")
    return expired in actual


def _observe(operation: str, present: bool) -> None:
    if operation == "query":
        logger.info(
            "probe=ddb_ttl_gsi observation=ttl_query expired_present=%s current_present=%s",
            present,
            True,
        )
    else:
        logger.info(
            "probe=ddb_ttl_gsi observation=ttl_scan expired_present=%s current_present=%s",
            present,
            True,
        )
