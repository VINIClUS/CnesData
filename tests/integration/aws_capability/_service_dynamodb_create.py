"""Criação atômica do root DynamoDB com identidade validada."""

from collections.abc import Mapping

from tests.integration.aws_capability._safety import run_name, tags
from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Creation,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def create_table(
    commands: CommandPort, spec: RunSpec, creation: Creation, context: RequestContext
) -> Resource:
    """Returns: Identidade aceita da tabela, sem confirmar prontidão.

    Raises: ServiceFailure para entrada, request ou identidade inválida.
    """
    name = run_name(spec)
    if creation.spec != spec or creation.name != name or creation.tags != tags(spec):
        raise ServiceFailure("service=creation_invalid")
    request = Request("dynamodb", "create-table", _payload(spec))
    data = require_data(call_command(commands, request, context))
    table = data.get("TableDescription")
    expected = f"arn:aws:dynamodb:{spec.region}:{spec.account}:table/{name}"
    if (
        not isinstance(table, Mapping)
        or table.get("TableName") != name
        or table.get("TableArn") != expected
        or table.get("TableStatus") not in ("CREATING", "ACTIVE")
    ):
        raise ServiceFailure("service=create_invalid")
    return Resource("dynamodb", name, name)


def _payload(spec: RunSpec) -> dict[str, object]:
    prefixes = ["", *(f"gsi{index}" for index in range(1, 7))]
    return {
        "TableName": run_name(spec),
        "BillingMode": "PAY_PER_REQUEST",
        "DeletionProtectionEnabled": True,
        "AttributeDefinitions": [
            {"AttributeName": f"{prefix}{key}", "AttributeType": "S"}
            for prefix in prefixes
            for key in ("pk", "sk")
        ],
        "KeySchema": _key_schema(""),
        "GlobalSecondaryIndexes": [
            {
                "IndexName": prefix,
                "KeySchema": _key_schema(prefix),
                "Projection": {"ProjectionType": "ALL"},
            }
            for prefix in prefixes[1:]
        ],
        "Tags": [{"Key": key, "Value": value} for key, value in tags(spec).items()],
    }


def _key_schema(prefix: str) -> list[dict[str, str]]:
    return [
        {"AttributeName": f"{prefix}pk", "KeyType": "HASH"},
        {"AttributeName": f"{prefix}sk", "KeyType": "RANGE"},
    ]
