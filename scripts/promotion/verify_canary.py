"""Confere marcador forte e objetos canary retidos sem mutar a entrega."""

from __future__ import annotations

import json
from datetime import timedelta
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
    delivered = timestamp(event["delivered_at"])
    require(timestamp(run["started_at"]) <= delivered <= now, "canary=old_marker")
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
        timestamp(context["run"]["started_at"]) <= response["LastModified"] <= clock.utcnow(),
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
    event = contract["event"]
    created = timestamp(event["created_at"])
    keys = [
        f"audit/.event-id/{event['event_id']}.json",
        f"audit/{event['tenant_id']}/{created:%Y/%m/%d}/{event['event_id']}.json",
    ]
    return [_object(contract, context, key) for key in keys]


def verify_canary(contract: Node, session: Any, clock: Any, run: Any) -> Node:
    """Returns: veredito fail-closed; orçamento total de polling/leitura é 60 segundos."""
    validate_contract(contract)
    validate_run(run)
    require(
        run["binding_sha256"] == contract["binding_sha256"]
        and run["event_sha256"] == sha(canonical(contract["event"]))
        and run["task_arn"].startswith(contract["cluster"].replace(":cluster/", ":task/") + "/")
        and timestamp(contract["event"]["created_at"])
        <= timestamp(run["started_at"])
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
