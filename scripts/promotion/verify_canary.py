"""Confere marcador forte e objetos canary retidos sem mutar a entrega."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any

from cnes_domain.control_plane.entities import OutboxEvent
from release.verify_activation import canonical, sha, timestamp
from release.verify_candidate import require
from scripts.promotion.run_canary import (
    Node,
    RunObservation,
    bounded,
    call,
    validate_contract,
    validate_run,
)
from scripts.promotion.seed_canary import pending_item


def _native_time(value: Any) -> datetime:
    require(
        type(value) is str
        and re.fullmatch(r"20\d{2}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z", value),
        "canary=native_timestamp",
    )
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def audit_keys(contract: Node) -> list[str]:
    """Returns: os dois objetos exatos do evento autenticado, sem wildcard."""
    event = contract["event"]
    created = timestamp(event["created_at"])
    return [
        f"audit/.event-id/{event['event_id']}.json",
        f"audit/{event['tenant_id']}/{created:%Y/%m/%d}/{event['event_id']}.json",
    ]


def _leading(partition: str) -> Node:
    return {
        "ForAllValues:StringEquals": {"dynamodb:LeadingKeys": [partition]},
        "Null": {"dynamodb:LeadingKeys": "false"},
    }


def _statement(action: Any, resource: Any, condition: Node | None = None) -> Node:
    value = {"Effect": "Allow", "Action": action, "Resource": resource}
    return value if condition is None else value | {"Condition": condition}


def _read_policy(contract: Node, task: str) -> list[Node]:
    from scripts.promotion.run_canary import task_arn

    task_arn(contract, task)
    table = (
        f"arn:aws:dynamodb:{contract['region']}:{contract['account_id']}:table/{contract['table']}"
    )
    objects = ["arn:aws:s3:::" + contract["bucket"] + "/" + key for key in audit_keys(contract)]
    return [
        _statement("ecs:DescribeTasks", task, {"ArnEquals": {"ecs:cluster": contract["cluster"]}}),
        _statement(
            "dynamodb:Query", table + "/index/" + contract["index"], _leading("OUTBOX#PENDING")
        ),
        _statement("dynamodb:GetItem", table, _leading("TENANT#_GLOBAL")),
        _statement(["s3:GetObject", "s3:GetObjectRetention"], objects),
    ]


def session_policy(contract: Node, actor: str, task: str | None) -> str:
    """Returns: policy STS exata <=2048 bytes, interseção com a role base."""
    validate_contract(contract)
    require(actor in {"seeder", "verifier"}, "canary=actor")
    if actor == "seeder":
        require(task is None, "canary=seeder_scope")
        table = (
            f"arn:aws:dynamodb:{contract['region']}:{contract['account_id']}:"
            f"table/{contract['table']}"
        )
        statements = [_statement("dynamodb:PutItem", table, _leading("TENANT#_GLOBAL"))]
    elif task is not None:
        statements = _read_policy(contract, task)
    else:
        statements = [
            _statement(
                "ecs:RunTask",
                contract["revisions"]["audit"],
                {"ArnEquals": {"ecs:cluster": contract["cluster"]}},
            ),
            _statement(
                "iam:PassRole",
                [contract["task_role"], contract["execution_role"]],
                {"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}},
            ),
        ]
    policy = json.dumps({"Version": "2012-10-17", "Statement": statements}, separators=(",", ":"))
    require(0 < len(policy.encode("ascii")) <= 2048, "canary=session_policy_size")
    return policy


def _query(contract: Node) -> Node:
    item = pending_item(contract)
    return {
        "TableName": contract["table"],
        "IndexName": contract["index"],
        "Limit": 1,
        "KeyConditionExpression": "gsi6pk = :pending AND gsi6sk = :sort",
        "ExpressionAttributeValues": {":pending": item["gsi6pk"], ":sort": item["gsi6sk"]},
    }


def _delivered(contract: Node, item: Node, run: RunObservation, now: Any) -> bool:
    expected = pending_item(contract)
    if item == expected:
        return False
    raw = item.get("payload", {}).get("S", "null")
    parsed = OutboxEvent.model_validate_json(raw)
    require(parsed.model_dump_json() == raw, "canary=payload_encoding")
    event = json.loads(item.get("payload", {}).get("S", "null"))
    require(type(event) is dict and event.get("delivered_at") is not None, "canary=delivery_marker")
    delivered = _native_time(event["delivered_at"])
    require(_native_time(run["started_at"]) <= delivered <= now, "canary=old_marker")
    event["delivered_at"] = None
    require(event == contract["event"], "canary=event_binding")
    require(
        set(item) == {"pk", "sk", "entity", "payload"}
        and all(item[k] == expected[k] for k in ("pk", "sk", "entity")),
        "canary=item_binding",
    )
    return True


def _object(contract: Node, context: Node, key: str) -> Node:
    session, clock, deadline = context["session"], context["clock"], context["deadline"]
    request = {
        "Bucket": contract["bucket"],
        "Key": key,
        "ExpectedBucketOwner": contract["account_id"],
    }
    response = call(session, clock, deadline, ("s3", "get_object", request))
    version = response.get("VersionId")
    require(type(version) is str and version not in {"", "null"}, "canary=object_version")
    expected = json.dumps(contract["event"], sort_keys=True, separators=(",", ":")).encode()
    body = response["Body"]
    try:
        raw = bounded(lambda: body.read(len(expected) + 1), clock, deadline)
    finally:
        body.close()
    require(clock.monotonic() < deadline, "canary=timeout")
    require(
        raw == expected and response.get("ContentLength") == len(expected), "canary=object_bytes"
    )
    require(
        _native_time(context["run"]["started_at"]) <= response["LastModified"] <= clock.utcnow(),
        "canary=old_object",
    )
    retention = call(
        session, clock, deadline, ("s3", "get_object_retention", request | {"VersionId": version})
    )
    value = retention.get("Retention", {})
    minimum = timestamp(contract["event"]["created_at"]) + timedelta(days=1)
    require(
        value.get("Mode") == "COMPLIANCE"
        and value.get("RetainUntilDate") is not None
        and value["RetainUntilDate"] >= minimum
        and value["RetainUntilDate"] > clock.utcnow(),
        "canary=retention",
    )
    return {
        "key": key,
        "version_id": version,
        "sha256": sha(raw),
        "retain_until": value["RetainUntilDate"].isoformat(),
    }


def _objects(contract: Node, context: Node) -> list[Node]:
    return [_object(contract, context, key) for key in audit_keys(contract)]


def verify_canary(contract: Node, session: Any, clock: Any, run: Any) -> Node:
    """Returns: veredito fail-closed; orçamento total de polling/leitura é 60 segundos."""
    validate_contract(contract)
    validate_run(run)
    require(
        run["binding_sha256"] == contract["binding_sha256"]
        and run["event_sha256"] == sha(canonical(contract["event"]))
        and run["task_arn"].startswith(contract["cluster"].replace(":cluster/", ":task/") + "/")
        and timestamp(contract["event"]["created_at"])
        <= _native_time(run["started_at"])
        <= clock.utcnow(),
        "canary=run_binding",
    )
    context = {"session": session, "clock": clock, "deadline": clock.monotonic() + 60, "run": run}
    expected = pending_item(contract)
    request = {
        "TableName": contract["table"],
        "ConsistentRead": True,
        "Key": {k: expected[k] for k in ("pk", "sk")},
    }
    while True:
        observed = call(
            session, clock, context["deadline"], ("dynamodb", "query", _query(contract))
        )
        require(
            observed.get("Items", []) in ([], [expected]) and not observed.get("LastEvaluatedKey"),
            "canary=index_binding",
        )
        current = call(session, clock, context["deadline"], ("dynamodb", "get_item", request))
        if current.get("Item") and _delivered(contract, current["Item"], run, clock.utcnow()):
            objects = _objects(contract, context)
            return {
                "status": "success",
                "binding_sha256": contract["binding_sha256"],
                "release_id": contract["release_id"],
                "canary_id": contract["canary_id"],
                "run": dict(run),
                "objects": objects,
            }
        jitter = clock.jitter()
        require(0 <= jitter <= 0.25, "canary=jitter")
        remaining = context["deadline"] - clock.monotonic()
        clock.sleep(min(1 + jitter, remaining))
