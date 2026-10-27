"""Token DynamoDB com replay após deleção e rejeição remota de payload distinto."""

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


def verify_token(
    commands: CommandPort, spec: RunSpec, root: Resource, context: RequestContext
) -> None:
    """Raises: ServiceFailure para token inválido; BudgetExhausted ao esgotar work."""
    key = {"pk": {"S": f"cap#{spec.run_id}"}, "sk": {"S": "token"}}
    item = {**key, "version": {"N": "1"}}
    original = _transaction_request(root, spec.run_id, "1")
    read = Request(
        "dynamodb", "get-item", {"TableName": root.name, "Key": key, "ConsistentRead": True}
    )
    require_data(call_command(commands, original, context))
    _verify_read(commands, read, item, context)
    data = require_data(call_command(commands, _delete_request(root, key), context))
    if data.get("Attributes") != item:
        raise ServiceFailure("probe=ddb_token_response_invalid")
    _verify_read(commands, read, None, context)
    require_data(call_command(commands, original, context))
    _verify_read(commands, read, None, context)
    changed = _transaction_request(root, spec.run_id, "2")
    reply = call_command(commands, changed, context)
    if (
        reply.ok
        or reply.error is None
        or reply.error.origin != "service"
        or reply.error.code != "IdempotentParameterMismatchException"
    ):
        raise ServiceFailure("probe=ddb_token_rejection_invalid")
    _verify_read(commands, read, None, context)


def _transaction_request(root: Resource, run_id: str, version: str) -> Request:
    return Request(
        "dynamodb",
        "transact-write-items",
        {
            "TransactItems": [
                {
                    "Put": {
                        "TableName": root.name,
                        "Item": {
                            "pk": {"S": f"cap#{run_id}"},
                            "sk": {"S": "token"},
                            "version": {"N": version},
                        },
                        "ConditionExpression": "attribute_not_exists(#pk) AND "
                        "attribute_not_exists(#sk)",
                        "ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk"},
                    }
                }
            ],
            "ClientRequestToken": f"{run_id}-token",
        },
    )


def _delete_request(root: Resource, key: Mapping[str, object]) -> Request:
    return Request(
        "dynamodb",
        "delete-item",
        {
            "TableName": root.name,
            "Key": key,
            "ConditionExpression": "#v=:expected",
            "ExpressionAttributeNames": {"#v": "version"},
            "ExpressionAttributeValues": {":expected": {"N": "1"}},
            "ReturnValues": "ALL_OLD",
        },
    )


def _verify_read(
    commands: CommandPort,
    request: Request,
    expected: Mapping[str, object] | None,
    context: RequestContext,
) -> None:
    data = require_data(call_command(commands, request, context))
    if expected is None:
        if "Item" in data:
            raise ServiceFailure("probe=ddb_token_readback_invalid")
    elif data.get("Item") != expected:
        raise ServiceFailure("probe=ddb_token_readback_invalid")
