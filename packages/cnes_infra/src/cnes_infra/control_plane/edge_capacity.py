"""Consumo atômico da reserva de capacidade na criação de agente Edge novo."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from cnes_domain.billing.models import CapacityKind, ReservationStatus
from cnes_infra.control_plane.dynamodb_codec import payload, put_action

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_infra.control_plane.dynamodb_codec import Action, Item
    from cnes_infra.control_plane.edge_registration import NewEdgeAgent

RESERVATION_EXPIRED = "capacity_reservation_expired"


def usable_agent_reservation(item: Item | None, command: NewEdgeAgent, now: datetime) -> bool:
    """Returns: True se a reserva é deste agente, RESERVED e ainda não venceu."""
    from cnes_infra.billing.dynamodb_quota_items import decode_capacity_reservation

    if item is None or command.fence is None:
        return False
    reservation, tenant_id = decode_capacity_reservation(item)
    return all((
        reservation.status is ReservationStatus.RESERVED,
        reservation.kind is CapacityKind.AGENT,
        reservation.billing_account_id == command.fence.billing_account_id,
        reservation.resource_id == command.agent_id,
        tenant_id == command.tenant_id,
        reservation.expires_at > now,
    ))


def consume_reservation_actions(table: str, item: Item, now: datetime) -> tuple[Action, ...]:
    """Returns: Put condicionado RESERVED→CONSUMED e o evento quota.consumed do outbox."""
    from cnes_infra.billing.dynamodb_items import outbox_item, put_new
    from cnes_infra.billing.dynamodb_quota_items import (
        decode_capacity_reservation,
        encode_capacity_reservation,
        quota_event,
    )

    current, tenant_id = decode_capacity_reservation(item)
    consumed = replace(current, status=ReservationStatus.CONSUMED)
    data: dict[str, str | int] = {
        "billing_account_id": current.billing_account_id,
        "reservation_id": current.reservation_id,
        "kind": current.kind.value,
        "resource_id": current.resource_id,
    }
    event = quota_event("quota.consumed", tenant_id, data, now)
    return (
        put_action(table, encode_capacity_reservation(consumed, tenant_id), payload(item)),
        put_new(table, outbox_item(event)),
    )
