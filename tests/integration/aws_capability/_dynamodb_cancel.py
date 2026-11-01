"""Cancelamento DynamoDB com razões ordenadas e três readbacks fortes."""

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


def verify_cancel(
    commands: CommandPort, spec: RunSpec, root: Resource, context: RequestContext
) -> Mapping[str, object]:
    """Returns: observações não sensíveis.
    Raises: ServiceFailure para cancelamento inválido; BudgetExhausted ao esgotar work."""
    keys = [
        {"pk": {"S": f"cap#{spec.run_id}"}, "sk": {"S": f"cancel#{index}"}} for index in range(1, 4)
    ]
    for key in keys:
        data = require_data(call_command(commands, _seed_request(root, key), context))
        if data.get("Attributes") != {**key, "version": {"N": "1"}}:
            raise ServiceFailure("probe=ddb_cancel_response_invalid")
    reply = call_command(commands, _transaction_request(root, keys, spec.run_id), context)
    codes = _verify_cancellation(reply)
    for key in keys:
        request = Request(
            "dynamodb", "get-item", {"TableName": root.name, "Key": key, "ConsistentRead": True}
        )
        data = require_data(call_command(commands, request, context))
        if data.get("Item") != {**key, "version": {"N": "1"}}:
            raise ServiceFailure("probe=ddb_cancel_readback_invalid")
    return {
        "cancel_error_code": "TransactionCanceledException",
        "cancel_reason_codes": codes,
        "cancel_readback_count": len(keys),
    }


def _seed_request(root: Resource, key: Mapping[str, object]) -> Request:
    return Request(
        "dynamodb",
        "update-item",
        {
            "TableName": root.name,
            "Key": key,
            "UpdateExpression": "SET #v=:new",
            "ConditionExpression": "attribute_not_exists(#pk) AND attribute_not_exists(#sk)",
            "ExpressionAttributeNames": {"#v": "version", "#pk": "pk", "#sk": "sk"},
            "ExpressionAttributeValues": {":new": {"N": "1"}},
            "ReturnValues": "ALL_NEW",
        },
    )


def _transaction_request(
    root: Resource, keys: Sequence[Mapping[str, object]], run_id: str
) -> Request:
    return Request(
        "dynamodb",
        "transact-write-items",
        {
            "TransactItems": [
                {"Update": _transaction_update(root, key, "0" if index == 1 else "1")}
                for index, key in enumerate(keys)
            ],
            "ClientRequestToken": f"{run_id}-cancel",
        },
    )


def _transaction_update(
    root: Resource, key: Mapping[str, object], expected: str
) -> Mapping[str, object]:
    return {
        "TableName": root.name,
        "Key": key,
        "UpdateExpression": "SET #v=:new",
        "ConditionExpression": "#v=:expected",
        "ExpressionAttributeNames": {"#v": "version"},
        "ExpressionAttributeValues": {":new": {"N": "2"}, ":expected": {"N": expected}},
    }


def _verify_cancellation(reply: Reply) -> list[str]:
    if (
        reply.ok
        or reply.error is None
        or reply.error.origin != "service"
        or reply.error.code != "TransactionCanceledException"
    ):
        raise ServiceFailure("probe=ddb_cancel_rejection_invalid")
    reasons = reply.error.details.get("CancellationReasons")
    if not isinstance(reasons, list) or len(reasons) != 3:
        raise ServiceFailure("probe=ddb_cancel_reasons_invalid")
    codes = [reason.get("Code") if isinstance(reason, Mapping) else None for reason in reasons]
    if codes != ["None", "ConditionalCheckFailed", "None"]:
        raise ServiceFailure("probe=ddb_cancel_reasons_invalid")
    return ["None", "ConditionalCheckFailed", "None"]
