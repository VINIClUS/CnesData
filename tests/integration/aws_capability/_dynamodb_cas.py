"""CAS DynamoDB parcial com readbacks fortes e rejeição remota de versão stale."""

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


def verify_cas(
    commands: CommandPort, spec: RunSpec, root: Resource, context: RequestContext
) -> Mapping[str, object]:
    """Returns: observações não sensíveis.
    Raises: ServiceFailure para CAS inválido; BudgetExhausted ao esgotar work."""
    key = {"pk": {"S": f"cap#{spec.run_id}"}, "sk": {"S": "cas"}}
    item_one = {**key, "version": {"N": "1"}}
    item_two = {**key, "version": {"N": "2"}}
    read = Request(
        "dynamodb", "get-item", {"TableName": root.name, "Key": key, "ConsistentRead": True}
    )
    _verify_write(commands, _update_request(root, key, "1"), item_one, context)
    _verify_read(commands, read, item_one, context)
    _verify_write(commands, _update_request(root, key, "2"), item_two, context)
    _verify_read(commands, read, item_two, context)
    reply = call_command(commands, _update_request(root, key, "3"), context)
    if (
        reply.ok
        or reply.error is None
        or reply.error.origin != "service"
        or reply.error.code != "ConditionalCheckFailedException"
    ):
        raise ServiceFailure("probe=ddb_cas_rejection_invalid")
    _verify_read(commands, read, item_two, context)
    return {
        "cas_write_versions": [1, 2],
        "cas_stale_error_code": reply.error.code,
        "cas_final_version": 2,
    }


def _update_request(root: Resource, key: Mapping[str, object], version: str) -> Request:
    names = {"#v": "version"}
    values = {":new": {"N": version}}
    condition = "#v=:expected"
    if version == "1":
        names.update({"#pk": "pk", "#sk": "sk"})
        condition = "attribute_not_exists(#pk) AND attribute_not_exists(#sk)"
    else:
        values[":expected"] = {"N": "1"}
    return Request(
        "dynamodb",
        "update-item",
        {
            "TableName": root.name,
            "Key": key,
            "UpdateExpression": "SET #v=:new",
            "ConditionExpression": condition,
            "ExpressionAttributeNames": names,
            "ExpressionAttributeValues": values,
            "ReturnValues": "ALL_NEW",
        },
    )


def _verify_write(
    commands: CommandPort, request: Request, expected: Mapping[str, object], context: RequestContext
) -> None:
    data = require_data(call_command(commands, request, context))
    if data.get("Attributes") != expected:
        raise ServiceFailure("probe=ddb_cas_response_invalid")


def _verify_read(
    commands: CommandPort, request: Request, expected: Mapping[str, object], context: RequestContext
) -> None:
    data = require_data(call_command(commands, request, context))
    if data.get("Item") != expected:
        raise ServiceFailure("probe=ddb_cas_readback_invalid")
