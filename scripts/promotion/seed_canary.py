"""Grava uma única identidade canary pendente sem ler nem atualizar domínio."""

from __future__ import annotations

import json
from typing import Any

from botocore.exceptions import ClientError

from cnes_domain.control_plane.entities import OutboxEvent
from cnes_infra.control_plane.dynamodb_codec import encode_model
from cnes_infra.control_plane.dynamodb_keys import key_component, outbox_key, timestamp
from scripts.promotion.run_canary import Node, call, validate_contract


def pending_item(contract: Node) -> Node:
    """Returns: item canônico consumido pelo dispatcher existente, sem semântica nova."""
    event = OutboxEvent.model_validate_json(json.dumps(contract["event"]))
    return encode_model(
        event,
        "OUTBOXEVENT",
        outbox_key(event.event_id),
        {
            "gsi6pk": "OUTBOX#PENDING",
            "gsi6sk": f"{timestamp(event.created_at)}#{key_component(event.event_id)}",
        },
    )


def seed_canary(contract: Node, session: Any, clock: Any) -> Node:
    """Returns: created ou exists-unverified; duplicata nunca é prova de entrega."""
    validate_contract(contract)
    request = {
        "TableName": contract["table"],
        "Item": pending_item(contract),
        "ConditionExpression": "attribute_not_exists(pk)",
    }
    status = "created"
    try:
        call(session, clock, clock.monotonic() + 60, ("dynamodb", "put_item", request))
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            raise
        status = "exists-unverified"
    return {"status": status, "binding_sha256": contract["binding_sha256"]}
