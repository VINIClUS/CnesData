"""Probe ddb-throughput: teto OnDemand 100/25 lido de volta e carga só observada."""

import re
from collections.abc import Mapping, Sequence

from tests.integration.aws_capability._safety import run_name
from tests.integration.aws_capability._service_commands import (
    call_command,
    is_remote_not_found,
    require_data,
)
from tests.integration.aws_capability._service_contracts import CommandPort, Reply, ServicePorts
from tests.integration.aws_capability._service_dynamodb_create import ON_DEMAND_THROUGHPUT
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

INDEXES = tuple(f"gsi{index}" for index in range(1, 7))
TRANSACTION_ITEMS = 100
BATCHES = 4
BATCH_ITEMS = 25
READY_POLLS = 30
READY_SECONDS = 2.0
MINIMUM_ELAPSED = 0.001
AUTH_CODES = frozenset(
    {
        "AccessDenied",
        "AccessDeniedException",
        "UnrecognizedClientException",
        "ExpiredTokenException",
        "InvalidSignatureException",
        "MissingAuthenticationToken",
    }
)


def probe_throughput(
    resources: Sequence[Resource], spec: RunSpec, ports: ServicePorts, context: RequestContext
) -> Mapping[str, object]:
    """Returns: códigos, contagens e taxa observados; teto lido de volta.

    Raises: ServiceFailure para entrada, teto divergente, acesso negado ou transporte.
    """
    name = run_name(spec)
    root = Resource("dynamodb", name, name)
    if context.lane != "work" or list(resources) != [root]:
        raise ServiceFailure("probe=throughput_invalid")
    _await_throughput(ports, root, context)
    observed: dict[str, object] = _transaction(ports.commands, spec, root, context)
    observed.update(_batches(ports, spec, root, context))
    observed.update(_readback(ports.commands, root, context))
    return observed


def _await_throughput(ports: ServicePorts, root: Resource, context: RequestContext) -> None:
    request = Request("dynamodb", "describe-table", {"TableName": root.name})
    for poll in range(READY_POLLS):
        reply = call_command(ports.commands, request, context)
        if reply.ok or not is_remote_not_found(reply):
            table = require_data(reply).get("Table")
            if isinstance(table, Mapping) and _active(table):
                _require_throughput(table)
                return
        if poll < READY_POLLS - 1:
            ports.pause(READY_SECONDS)
    raise ServiceFailure("probe=table_not_ready")


def _active(table: Mapping[str, object]) -> bool:
    indexes = table.get("GlobalSecondaryIndexes")
    rows = indexes if isinstance(indexes, list) else []
    return table.get("TableStatus") == "ACTIVE" and all(
        isinstance(row, Mapping) and row.get("IndexStatus") == "ACTIVE" for row in rows
    )


def _require_throughput(table: Mapping[str, object]) -> None:
    if table.get("OnDemandThroughput") != ON_DEMAND_THROUGHPUT:
        raise ServiceFailure("probe=throughput_mismatch target=table")
    rows = table.get("GlobalSecondaryIndexes")
    found = {
        row.get("IndexName"): row.get("OnDemandThroughput")
        for row in (rows if isinstance(rows, list) else [])
        if isinstance(row, Mapping)
    }
    for name in INDEXES:
        if found.get(name) != ON_DEMAND_THROUGHPUT:
            raise ServiceFailure(f"probe=throughput_mismatch target={name}")


def _observe(commands: CommandPort, request: Request, context: RequestContext) -> Reply:
    reply = call_command(commands, request, context)
    if reply.ok:
        return reply
    error = reply.error
    if error is None or error.origin != "service" or error.code in AUTH_CODES:
        raise ServiceFailure("probe=throughput_call_failed")
    return reply


def _code(reply: Reply) -> str:
    if reply.ok or reply.error is None:
        return "ok"
    return reply.error.code if re.fullmatch(r"[A-Za-z0-9]{1,64}", reply.error.code) else "Other"


def _item(partition: str, sort: str) -> dict[str, object]:
    return {"pk": {"S": partition}, "sk": {"S": sort}, "version": {"N": "1"}}


def _transaction(
    commands: CommandPort, spec: RunSpec, root: Resource, context: RequestContext
) -> dict[str, object]:
    partition = f"cap#{spec.run_id}#tx"
    puts = [
        {"Put": {"TableName": root.name, "Item": _item(partition, f"item#{index:03d}")}}
        for index in range(TRANSACTION_ITEMS)
    ]
    request = Request("dynamodb", "transact-write-items", {"TransactItems": puts})
    reply = _observe(commands, request, context)
    return {"tx_code": _code(reply), "tx_reason_codes": _reasons(reply)}


def _reasons(reply: Reply) -> dict[str, int]:
    rows = reply.error.details.get("CancellationReasons") if reply.error is not None else None
    counts: dict[str, int] = {}
    for row in rows if isinstance(rows, list) else []:
        code = str(row.get("Code")) if isinstance(row, Mapping) else "Other"
        code = code if re.fullmatch(r"[A-Za-z0-9]{1,64}", code) else "Other"
        counts[code] = counts.get(code, 0) + 1
    return dict(sorted(counts.items()))


def _batches(
    ports: ServicePorts, spec: RunSpec, root: Resource, context: RequestContext
) -> dict[str, object]:
    observed: dict[str, object] = {}
    accepted = 0
    start = ports.now()
    for number in range(1, BATCHES + 1):
        partition = f"cap#{spec.run_id}#batch#{number}"
        writes = [
            {"PutRequest": {"Item": _item(partition, f"item#{index:02d}")}}
            for index in range(BATCH_ITEMS)
        ]
        request = Request("dynamodb", "batch-write-item", {"RequestItems": {root.name: writes}})
        reply = _observe(ports.commands, request, context)
        left = _unprocessed(reply)
        observed[f"batch_{number}_code"] = _code(reply)
        observed[f"batch_{number}_unprocessed"] = left
        accepted += BATCH_ITEMS - left
    elapsed = max(ports.now() - start, MINIMUM_ELAPSED)
    observed["batch_accepted_items"] = accepted
    observed["batch_elapsed_seconds"] = round(elapsed, 2)
    observed["batch_rate_items_per_second"] = round(accepted / elapsed, 2)
    return observed


def _unprocessed(reply: Reply) -> int:
    if not reply.ok:
        return BATCH_ITEMS
    pending = reply.data.get("UnprocessedItems", {})
    if not isinstance(pending, Mapping) or not all(isinstance(v, list) for v in pending.values()):
        raise ServiceFailure("probe=throughput_call_failed")
    return sum(len(rows) for rows in pending.values())


def _readback(
    commands: CommandPort, root: Resource, context: RequestContext
) -> dict[str, object]:
    total = 0
    start: object = None
    for _ in range(context.budget.limits.pages):
        payload: dict[str, object] = {
            "TableName": root.name,
            "Select": "COUNT",
            "ConsistentRead": True,
        }
        if start is not None:
            payload["ExclusiveStartKey"] = start
        reply = _observe(commands, Request("dynamodb", "scan", payload), context)
        if not reply.ok:
            return {"readback_code": _code(reply), "readback_count": total}
        count = reply.data.get("Count")
        if type(count) is not int:
            raise ServiceFailure("probe=throughput_call_failed")
        total += count
        start = reply.data.get("LastEvaluatedKey")
        if start is None:
            return {"readback_code": "ok", "readback_count": total}
    raise ServiceFailure("probe=readback_incomplete")
