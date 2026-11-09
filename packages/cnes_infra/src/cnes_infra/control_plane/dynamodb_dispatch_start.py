"""Claims e cancelamentos de dispatch serializados com o gate no DynamoDB."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cnes_domain.control_plane.entities import RunDispatch
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.dynamodb_codec import decode_model, payload, put_action
from cnes_infra.control_plane.dynamodb_environment_gate import fence_key, semaphore_key
from cnes_infra.control_plane.dynamodb_keys import dispatch_key, item_key

# These helpers implement the adapter's private transaction boundary.
# ruff: noqa: SLF001
# pyright: reportPrivateUsage=false
if TYPE_CHECKING:
    from cnes_domain.control_plane.commands import CancelReservedRunDispatch, ClaimRunDispatchStart
    from cnes_infra.control_plane.dynamodb_dispatch import DynamoDBDispatch


def claim_start(
    adapter: DynamoDBDispatch,
    command: ClaimRunDispatchStart,
) -> RunDispatch | None:
    item = adapter._get_item(dispatch_key(command.tenant_id, command.run_id))
    if item is None:
        return None
    dispatch = decode_model(item, RunDispatch)
    if dispatch.state is DispatchState.STARTING:
        return None
    invalid = (
        dispatch.state is not DispatchState.RESERVED
        or dispatch.dispatch_id != command.dispatch_id
        or dispatch.generation != command.claim.generation
        or dispatch.lease_until <= command.now
    )
    if invalid:
        return None
    semaphore = adapter._get_item(semaphore_key(command.environment))
    if semaphore is None:
        return None
    updated = dispatch.model_copy(
        update={"state": DispatchState.STARTING, "start_claim": command.claim}
    )
    try:
        adapter._transact(
            (
                _fence_claim(adapter, command),
                _semaphore_claim(adapter, command),
                put_action(adapter._table_name, adapter._dispatch_item(updated), payload(item)),
            )
        )
    except Conflict:
        return None
    return updated


def cancel_reserved(
    adapter: DynamoDBDispatch,
    command: CancelReservedRunDispatch,
) -> RunDispatch | None:
    item = adapter._get_item(dispatch_key(command.tenant_id, command.run_id))
    if item is None:
        return None
    dispatch = decode_model(item, RunDispatch)
    if (
        dispatch.dispatch_id != command.dispatch_id
        or dispatch.state is not DispatchState.RESERVED
        or dispatch.generation != command.claim.generation
    ):
        return None
    updated = dispatch.model_copy(
        update={
            "state": DispatchState.TERMINAL,
            "terminal_outcome": DispatchOutcome.CANCELED,
            "start_claim": command.claim,
        }
    )
    semaphore = adapter._get_item(semaphore_key(command.environment))
    if semaphore is None:
        return None
    expected = {
        "token": {"N": str(command.claim.permit_token)},
        "holder_dispatch_id": {"S": command.dispatch_id},
        "holder_tenant_id": {"S": command.tenant_id},
        "holder_run_id": {"S": command.run_id},
        "generation": {"N": str(command.claim.generation)},
        "request_sha256": {"S": command.claim.request_sha256},
    }
    if any(semaphore.get(key) != value for key, value in expected.items()):
        return None
    check = {
        "ConditionCheck": {
            "TableName": adapter._table_name,
            "Key": item_key(*semaphore_key(command.environment)),
            "ConditionExpression": " AND ".join(f"#v{i} = :v{i}" for i in range(len(expected))),
            "ExpressionAttributeNames": {f"#v{i}": key for i, key in enumerate(expected)},
            "ExpressionAttributeValues": {
                f":v{i}": value for i, value in enumerate(expected.values())
            },
        }
    }
    try:
        adapter._transact(
            (check, put_action(adapter._table_name, adapter._dispatch_item(updated), payload(item)))
        )
    except Conflict:
        return None
    return updated


def _semaphore_claim(adapter: DynamoDBDispatch, command: ClaimRunDispatchStart) -> dict[str, Any]:
    names = {
        "#token": "token",
        "#tenant": "holder_tenant_id",
        "#run": "holder_run_id",
        "#dispatch": "holder_dispatch_id",
        "#generation": "generation",
        "#request": "request_sha256",
        "#fence": "fence_version",
        "#start": "start_state",
        "#initial": "initial_state",
        "#updated": "updated_at",
        "#canary": "canary_promotion_id",
        "#lease": "lease_until",
    }
    values = {
        ":token": {"N": str(command.claim.permit_token)},
        ":tenant": {"S": command.tenant_id},
        ":run": {"S": command.run_id},
        ":dispatch": {"S": command.dispatch_id},
        ":generation": {"N": str(command.claim.generation)},
        ":request": {"S": command.claim.request_sha256},
        ":fence": {"N": str(command.claim.fence_version)},
        ":starting": {"S": DispatchState.STARTING.value},
        ":reserved": {"S": DispatchState.RESERVED.value},
        ":updated": {"S": command.now.isoformat()},
        ":canary": {"S": command.claim.promotion_id or ""},
    }
    return {
        "Update": {
            "TableName": adapter._table_name,
            "Key": item_key(*semaphore_key(command.environment)),
            "UpdateExpression": (
                "SET #generation = :generation, #request = :request, #fence = :fence, "
                "#start = :starting, #initial = :reserved, #updated = :updated"
            ),
            "ConditionExpression": (
                "#token = :token AND #tenant = :tenant AND #run = :run "
                "AND #dispatch = :dispatch AND #lease > :updated AND "
                "(attribute_not_exists(#start) OR #start = :reserved) AND "
                "(attribute_not_exists(#generation) OR #generation = :generation) AND "
                "(attribute_not_exists(#request) OR #request = :request) AND "
                "(attribute_not_exists(#canary) OR #canary = :canary)"
            ),
            "ExpressionAttributeNames": names,
            "ExpressionAttributeValues": values,
        }
    }


def _fence_claim(adapter: DynamoDBDispatch, command: ClaimRunDispatchStart) -> dict[str, Any]:
    return {
        "ConditionCheck": {
            "TableName": adapter._table_name,
            "Key": item_key(*fence_key(command.environment)),
            "ConditionExpression": (
                "(attribute_not_exists(pk) AND :version = :zero) OR "
                "(#version = :version AND (#state = :open OR "
                "(#state = :closed AND #promotion = :promotion)))"
            ),
            "ExpressionAttributeNames": {
                "#state": "state",
                "#promotion": "promotion_id",
                "#version": "version",
            },
            "ExpressionAttributeValues": {
                ":version": {"N": str(command.claim.fence_version)},
                ":zero": {"N": "0"},
                ":open": {"S": "OPEN"},
                ":closed": {"S": "CLOSED"},
                ":promotion": {"S": command.claim.promotion_id or ""},
            },
        }
    }
