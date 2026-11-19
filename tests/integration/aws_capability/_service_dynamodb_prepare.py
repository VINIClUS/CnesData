"""Prontidão, ownership fresco e configuração única de TTL DynamoDB."""

import time
from collections.abc import Mapping

from tests.integration.aws_capability._safety import tags
from tests.integration.aws_capability._service_commands import (
    call_command,
    is_remote_not_found,
    require_data,
    require_error,
)
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability._service_dynamodb import _read_tags
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def prepare_table(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> None:
    """Raises: ServiceFailure para preparo inválido; BudgetExhausted ao esgotar work."""
    arn = _wait_ready(commands, spec, resource, context)
    _require_owner(commands, spec, arn, context)
    status = _ttl_status(_describe_ttl(commands, resource, context), True)
    if status == "ENABLED":
        return
    if status == "DISABLED":
        arn = _ready_arn(commands, spec, resource, context)
        if arn is None:
            raise ServiceFailure("probe=ddb_table_invalid")
        _require_owner(commands, spec, arn, context)
        _enable_ttl(commands, resource, context)
    else:
        _pause(context)
    _wait_ttl(commands, resource, context)


def _wait_ready(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> str:
    while True:
        arn = _ready_arn(commands, spec, resource, context)
        if arn is not None:
            return arn
        _pause(context)


def _ready_arn(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> str | None:
    reply = call_command(
        commands, Request("dynamodb", "describe-table", {"TableName": resource.name}), context
    )
    if not reply.ok:
        if is_remote_not_found(reply):
            return None
        require_error(reply)
        raise ServiceFailure("service=describe_failed")
    data = require_data(reply)
    table = data.get("Table")
    expected = f"arn:aws:dynamodb:{spec.region}:{spec.account}:table/{resource.name}"
    if (
        not isinstance(table, Mapping)
        or table.get("TableName") != resource.name
        or table.get("TableArn") != expected
    ):
        raise ServiceFailure("probe=ddb_table_invalid")
    status = _table_status(table.get("TableStatus"))
    if status != "ACTIVE":
        return None
    if _schema_ready(table):
        return expected
    return None


def _table_status(status: object) -> str:
    if not isinstance(status, str) or status not in ("ACTIVE", "CREATING", "UPDATING"):
        raise ServiceFailure("probe=ddb_table_invalid")
    return status


def _schema_ready(table: Mapping[str, object]) -> bool:
    billing = table.get("BillingModeSummary")
    if (
        not isinstance(billing, Mapping)
        or billing.get("BillingMode") != "PAY_PER_REQUEST"
        or table.get("DeletionProtectionEnabled") is not True
    ):
        raise ServiceFailure("probe=ddb_table_invalid")
    prefixes = ["", *(f"gsi{index}" for index in range(1, 7))]
    expected = {(f"{prefix}{key}", "S") for prefix in prefixes for key in ("pk", "sk")}
    if _pairs(table.get("AttributeDefinitions"), ("AttributeName", "AttributeType")) != expected:
        raise ServiceFailure("probe=ddb_table_invalid")
    _require_keys(table.get("KeySchema"), "")
    return _indexes_ready(table.get("GlobalSecondaryIndexes"))


def _pairs(raw: object, fields: tuple[str, str]) -> set[tuple[str, str]]:
    if not isinstance(raw, list):
        raise ServiceFailure("probe=ddb_table_invalid")
    pairs: set[tuple[str, str]] = set()
    for item in raw:
        if not isinstance(item, Mapping):
            raise ServiceFailure("probe=ddb_table_invalid")
        key, value = item.get(fields[0]), item.get(fields[1])
        if not isinstance(key, str) or not isinstance(value, str):
            raise ServiceFailure("probe=ddb_table_invalid")
        pairs.add((key, value))
    if len(pairs) != len(raw):
        raise ServiceFailure("probe=ddb_table_invalid")
    return pairs


def _require_keys(raw: object, prefix: str) -> None:
    expected = {(f"{prefix}pk", "HASH"), (f"{prefix}sk", "RANGE")}
    if _pairs(raw, ("AttributeName", "KeyType")) != expected:
        raise ServiceFailure("probe=ddb_table_invalid")


def _indexes_ready(raw: object) -> bool:
    if not isinstance(raw, list) or len(raw) != 6:
        raise ServiceFailure("probe=ddb_table_invalid")
    remaining = {f"gsi{index}" for index in range(1, 7)}
    ready = True
    for index in raw:
        if not isinstance(index, Mapping):
            raise ServiceFailure("probe=ddb_table_invalid")
        name = index.get("IndexName")
        if not isinstance(name, str) or name not in remaining:
            raise ServiceFailure("probe=ddb_table_invalid")
        remaining.remove(name)
        ready = _index_ready(index, name) and ready
    return ready


def _index_ready(index: Mapping[str, object], name: str) -> bool:
    _require_keys(index.get("KeySchema"), name)
    projection = index.get("Projection")
    if not isinstance(projection, Mapping) or projection.get("ProjectionType") != "ALL":
        raise ServiceFailure("probe=ddb_table_invalid")
    return _table_status(index.get("IndexStatus")) == "ACTIVE"


def _require_owner(commands: CommandPort, spec: RunSpec, arn: str, context: RequestContext) -> None:
    current = _read_tags(commands, spec, arn, context)
    if any(current.get(key) != value for key, value in tags(spec).items()):
        raise ServiceFailure("probe=ddb_ownership_invalid")


def _describe_ttl(
    commands: CommandPort, resource: Resource, context: RequestContext
) -> Mapping[str, object]:
    request = Request("dynamodb", "describe-time-to-live", {"TableName": resource.name})
    return require_data(call_command(commands, request, context))


def _ttl_status(data: Mapping[str, object], allow_disabled: bool) -> str:
    ttl = data.get("TimeToLiveDescription")
    if not isinstance(ttl, Mapping):
        raise ServiceFailure("probe=ddb_ttl_invalid")
    status, attribute = ttl.get("TimeToLiveStatus"), ttl.get("AttributeName")
    if status in ("ENABLED", "ENABLING") and attribute == "expires_at":
        return status
    if (
        allow_disabled
        and status == "DISABLED"
        and ("AttributeName" not in ttl or attribute == "expires_at")
    ):
        return "DISABLED"
    raise ServiceFailure("probe=ddb_ttl_invalid")


def _enable_ttl(commands: CommandPort, resource: Resource, context: RequestContext) -> None:
    request = Request(
        "dynamodb",
        "update-time-to-live",
        {
            "TableName": resource.name,
            "TimeToLiveSpecification": {"Enabled": True, "AttributeName": "expires_at"},
        },
    )
    data = require_data(call_command(commands, request, context))
    echo = data.get("TimeToLiveSpecification")
    if (
        not isinstance(echo, Mapping)
        or echo.get("Enabled") is not True
        or echo.get("AttributeName") != "expires_at"
    ):
        raise ServiceFailure("probe=ddb_ttl_invalid")


def _wait_ttl(commands: CommandPort, resource: Resource, context: RequestContext) -> None:
    while _ttl_status(_describe_ttl(commands, resource, context), False) != "ENABLED":
        _pause(context)


def _pause(context: RequestContext) -> None:
    budget = context.budget
    budget.check("work")
    seconds = min(1.0, budget.remaining_seconds("work"))
    if seconds <= 0:
        raise BudgetExhausted("budget=deadline")
    time.sleep(seconds)
    budget.check("work")
