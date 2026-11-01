"""Transações DynamoDB parciais com cardinalidade e readbacks fortes limitados."""

import json
from collections.abc import Mapping, Sequence

from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_contracts import CommandPort, Reply
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def verify_transactions(
    commands: CommandPort, spec: RunSpec, root: Resource, context: RequestContext
) -> Mapping[str, object]:
    """Args: commands, spec, root, context: porta, execução, tabela e contexto.
    Returns: observações não sensíveis (códigos e contagens).
    Raises: ServiceFailure para resposta inválida; BudgetExhausted ao esgotar work.
    """
    partition = f"cap#{spec.run_id}"
    hundred = _items(f"{partition}#tx100", 100)
    request = _write_request(root, hundred, f"{spec.run_id}-tx100")
    require_data(call_command(commands, request, context))
    _verify_readback(_read_group(commands, root, f"{partition}#tx100", context), hundred)
    request = _write_request(root, _items(f"{partition}#tx101", 101), f"{spec.run_id}-tx101")
    rejected = call_command(commands, request, context)
    _verify_rejection(rejected, False)
    request = _duplicate_request(root, f"{partition}#txdup", f"{spec.run_id}-txdup")
    duplicate = call_command(commands, request, context)
    _verify_rejection(duplicate, True)
    _verify_readback(_read_group(commands, root, f"{partition}#txdup", context), [])
    return {
        "tx_100_accepted": True,
        "tx_100_readback_count": len(hundred),
        "tx_101_error_code": _code(rejected),
        "tx_duplicate_error_code": _code(duplicate),
        "tx_duplicate_reason_codes": _reason_codes(duplicate),
        "tx_duplicate_readback_count": 0,
    }


def _code(reply: Reply) -> str:
    return reply.error.code if reply.error is not None else ""


def _reason_codes(reply: Reply) -> list[str]:
    reasons = reply.error.details.get("CancellationReasons") if reply.error is not None else None
    if not isinstance(reasons, list):
        return []
    return [str(reason["Code"]) for reason in reasons if isinstance(reason, Mapping)]


def _items(partition: str, count: int) -> list[Mapping[str, object]]:
    return [
        {"pk": {"S": partition}, "sk": {"S": f"item#{index:03d}"}, "version": {"N": "1"}}
        for index in range(count)
    ]


def _write_request(root: Resource, items: Sequence[Mapping[str, object]], token: str) -> Request:
    return Request(
        "dynamodb",
        "transact-write-items",
        {
            "TransactItems": [{"Put": {"TableName": root.name, "Item": item}} for item in items],
            "ClientRequestToken": token,
        },
    )


def _duplicate_request(root: Resource, partition: str, token: str) -> Request:
    target = {"pk": {"S": partition}, "sk": {"S": "target"}}
    sibling = {"pk": {"S": partition}, "sk": {"S": "sibling"}}
    return Request(
        "dynamodb",
        "transact-write-items",
        {
            "TransactItems": [
                {"Put": {"TableName": root.name, "Item": {**target, "version": {"N": "1"}}}},
                {
                    "ConditionCheck": {
                        "TableName": root.name,
                        "Key": target,
                        "ConditionExpression": "attribute_not_exists(#pk)",
                        "ExpressionAttributeNames": {"#pk": "pk"},
                    }
                },
                {"Put": {"TableName": root.name, "Item": {**sibling, "version": {"N": "1"}}}},
            ],
            "ClientRequestToken": token,
        },
    )


def _query_payload(root: Resource, partition: str) -> dict[str, object]:
    return {
        "TableName": root.name,
        "KeyConditionExpression": "#pk=:pk",
        "ExpressionAttributeNames": {"#pk": "pk"},
        "ExpressionAttributeValues": {":pk": {"S": partition}},
        "ConsistentRead": True,
        "Limit": 60,
    }


def _read_group(
    commands: CommandPort, root: Resource, partition: str, context: RequestContext
) -> list[object]:
    payload = _query_payload(root, partition)
    items: list[object] = []
    seen: set[str] = set()
    for _ in range(8):
        data = require_data(call_command(commands, Request("dynamodb", "query", payload), context))
        page = data.get("Items")
        if not isinstance(page, list):
            raise ServiceFailure("probe=ddb_transactions_response_invalid")
        items.extend(page)
        cursor = _query_cursor(data, partition, seen)
        if cursor is None:
            return items
        payload = {**payload, "ExclusiveStartKey": cursor}
    raise ServiceFailure("probe=ddb_transactions_pages_exhausted")


def _query_cursor(
    data: Mapping[str, object], partition: str, seen: set[str]
) -> Mapping[str, object] | None:
    cursor = data.get("LastEvaluatedKey", {})
    if not isinstance(cursor, Mapping):
        raise ServiceFailure("probe=ddb_transactions_query_invalid")
    if not cursor:
        return None
    if set(cursor) != {"pk", "sk"} or cursor.get("pk") != {"S": partition}:
        raise ServiceFailure("probe=ddb_transactions_query_invalid")
    sort_key = cursor.get("sk")
    if (
        not isinstance(sort_key, Mapping)
        or set(sort_key) != {"S"}
        or not isinstance(sort_key["S"], str)
        or not sort_key["S"]
    ):
        raise ServiceFailure("probe=ddb_transactions_query_invalid")
    encoded = json.dumps(cursor, sort_keys=True)
    if encoded in seen:
        raise ServiceFailure("probe=ddb_transactions_query_invalid")
    seen.add(encoded)
    return cursor


def _verify_readback(items: Sequence[object], expected: Sequence[Mapping[str, object]]) -> None:
    if len(items) != len(expected) or not all(isinstance(item, Mapping) for item in items):
        raise ServiceFailure("probe=ddb_transactions_readback_invalid")
    keys = [
        json.dumps({key: item.get(key) for key in ("pk", "sk")}, sort_keys=True)
        for item in items
        if isinstance(item, Mapping)
    ]
    actual_items = sorted(json.dumps(item, sort_keys=True) for item in items)
    expected_items = sorted(json.dumps(item, sort_keys=True) for item in expected)
    if len(set(keys)) != len(keys) or actual_items != expected_items:
        raise ServiceFailure("probe=ddb_transactions_readback_invalid")


def _verify_rejection(reply: Reply, duplicate: bool) -> None:
    error = reply.error
    if reply.ok or error is None or error.origin != "service":
        raise ServiceFailure("probe=ddb_transactions_rejection_invalid")
    if error.code == "ValidationException":
        return
    if (
        not duplicate
        or error.code != "TransactionCanceledException"
        or not _duplicate_reasons(error.details.get("CancellationReasons"))
    ):
        raise ServiceFailure("probe=ddb_transactions_rejection_invalid")


def _duplicate_reasons(reasons: object) -> bool:
    if not isinstance(reasons, list) or len(reasons) != 3:
        return False
    if not all(
        isinstance(reason, Mapping) and type(reason.get("Code")) is str for reason in reasons
    ):
        return False
    return [reason["Code"] for reason in reasons] in [
        ["ValidationError", "None", "None"],
        ["None", "ValidationError", "None"],
        ["ValidationError", "ValidationError", "None"],
    ]
