"""Apoio dos testes do observador de shadow: contadores de capacidade e eventos auditados."""

from typing import Any, cast

from cnes_domain.billing.shadow import SHADOW_DENIED_EVENT
from cnes_domain.control_plane.entities import OutboxEvent
from cnes_infra.billing.keys import capacity_usage_key
from cnes_infra.control_plane.dynamodb_codec import decode_model
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.billing.billing_factories import TABLE_NAME


def seed_capacity(client: Any, account: str, **counters: int) -> None:
    item = item_key(*capacity_usage_key(account))
    item |= {name: {"N": str(value)} for name, value in counters.items()}
    client.put_item(TableName=TABLE_NAME, Item=item)


def shadow_events(client: Any) -> list[OutboxEvent]:
    items = client.scan(TableName=TABLE_NAME, ConsistentRead=True)["Items"]
    events = [
        decode_model(item, OutboxEvent)
        for item in items
        if item.get("entity", {}).get("S") == "OUTBOXEVENT"
    ]
    return sorted(
        (event for event in events if event.event_type == SHADOW_DENIED_EVENT),
        key=lambda event: event.event_id,
    )


def shadow_attributes(event: OutboxEvent) -> dict[str, Any]:
    return cast("dict[str, Any]", event.payload["attributes"])


def shadow_reasons(client: Any) -> list[str]:
    return [str(event.payload["reason_code"]) for event in shadow_events(client)]
