"""Snapshot imutável de fechamento e CAS exato do semáforo observado."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from cnes_domain.control_plane.environment_gate import FenceState, PromotionFence, open_fence
from cnes_infra.control_plane.dynamodb_keys import item_key, timestamp

if TYPE_CHECKING:
    from cnes_domain.control_plane.environment_gate import UnitPermit
    from cnes_infra.control_plane.dynamodb_codec import Action, Item


def canonical_snapshot(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def snapshot_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_snapshot(value)).hexdigest()


def _closed_membership(permit: UnitPermit | None) -> tuple[dict[str, Any], ...]:
    if permit is None:
        return ()
    return (
        {
            "tenant_id": permit.holder.tenant_id,
            "run_id": permit.holder.run_id,
            "dispatch_id": permit.holder.dispatch_id,
            "token": permit.token,
            "generation": permit.generation,
            "fence_version": permit.fence_version,
            "request_sha256": permit.request_sha256,
            "initial_state": permit.start_state
            or ("STARTED" if permit.execution_ref else "RESERVED"),
            "start_state": permit.start_state
            or ("STARTED" if permit.execution_ref else "RESERVED"),
            "execution_ref": permit.execution_ref,
            "promotion_id": permit.canary_promotion_id,
            "acquired_at": timestamp(permit.acquired_at),
            "lease_until": timestamp(permit.lease_until),
        },
    )


def _snapshot_update(
    table: str,
    current: PromotionFence,
    updated: PromotionFence,
    membership: tuple[dict[str, Any], ...],
) -> Action:
    assert updated.changed_at is not None
    values = {
        ":entity": {"S": "PROMOTIONFENCE"},
        ":state": {"S": "CLOSED"},
        ":promotion": {"S": updated.promotion_id},
        ":version": {"N": str(updated.version)},
        ":changed": {"S": timestamp(updated.changed_at)},
        ":membership": {"S": canonical_snapshot(membership).decode()},
        ":membership_sha": {"S": snapshot_sha256(membership)},
    }
    receipt = {
        "environment": updated.environment,
        "promotion_id": updated.promotion_id,
        "version": updated.version,
        "changed_at": timestamp(updated.changed_at),
        "closed_membership_sha256": snapshot_sha256(membership),
        "expires_at": (updated.changed_at + timedelta(seconds=1800))
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
    }
    values[":receipt"] = {"S": canonical_snapshot(receipt).decode()}
    condition = "attribute_not_exists(pk)"
    if current.version:
        condition = "#version = :expected AND #state = :open"
        values.update({":expected": {"N": str(current.version)}, ":open": {"S": "OPEN"}})
    return {
        "Update": {
            "TableName": table,
            "Key": item_key(f"ENV#{updated.environment}#FENCE", "FENCE"),
            "UpdateExpression": "SET #entity = :entity, #state = :state, #promotion = :promotion, "
            "#version = :version, #changed = :changed, "
            "closed_membership = :membership, closed_membership_sha256 = :membership_sha, "
            "close_receipt = :receipt REMOVE confirmed_at",
            "ConditionExpression": condition,
            "ExpressionAttributeNames": {
                "#entity": "entity",
                "#state": "state",
                "#promotion": "promotion_id",
                "#version": "version",
                "#changed": "changed_at",
            },
            "ExpressionAttributeValues": values,
        }
    }


def _snapshot_check(table: str, environment: str, item: Item | None) -> Action:
    request: dict[str, Any] = {
        "TableName": table,
        "Key": item_key(f"ENV#{environment}#SEMAPHORE", "UNIT"),
    }
    if item is None:
        return {"ConditionCheck": request | {"ConditionExpression": "attribute_not_exists(pk)"}}
    known = {
        "token",
        "holder_tenant_id",
        "holder_run_id",
        "holder_dispatch_id",
        "acquired_at",
        "lease_until",
        "execution_ref",
        "generation",
        "request_sha256",
        "fence_version",
        "start_state",
        "canary_promotion_id",
        "settlement",
        "updated_at",
    }
    keys = sorted(known | (set(item) - {"pk", "sk"}))
    names: dict[str, str] = {}
    values: dict[str, Any] = {}
    conditions: list[str] = []
    for index, key in enumerate(keys):
        name, value = f"#v{index}", f":v{index}"
        names[name] = key
        if key in item:
            conditions.append(f"{name} = {value}")
            values[value] = item[key]
        else:
            conditions.append(f"attribute_not_exists({name})")
    return {
        "ConditionCheck": request
        | {
            "ConditionExpression": " AND ".join(conditions),
            "ExpressionAttributeNames": names,
            "ExpressionAttributeValues": values,
        }
    }


def close_transaction(
    table: str,
    current: PromotionFence,
    updated: PromotionFence,
    observed: tuple[UnitPermit | None, Item | None],
) -> tuple[Action, Action]:
    permit, item = observed
    return (
        _snapshot_update(table, current, updated, _closed_membership(permit)),
        _snapshot_check(table, updated.environment, item),
    )


def decode_fence(item: Item | None, environment: str) -> PromotionFence:
    if item is None:
        return open_fence(environment)
    membership = (
        tuple(json.loads(item["closed_membership"]["S"])) if "closed_membership" in item else ()
    )
    return PromotionFence(
        environment=environment,
        state=FenceState(item["state"]["S"]),
        promotion_id=item.get("promotion_id", {}).get("S"),
        version=int(item["version"]["N"]),
        changed_at=datetime.fromisoformat(item["changed_at"]["S"]),
        closed_membership=membership,
        closed_membership_sha256=item.get("closed_membership_sha256", {}).get("S"),
        close_receipt=json.loads(item["close_receipt"]["S"]) if "close_receipt" in item else None,
        confirmed_at=datetime.fromisoformat(item["confirmed_at"]["S"])
        if "confirmed_at" in item
        else None,
    )
