"""Contrato serializável dos eventos de auditoria operacional."""
from copy import deepcopy
from datetime import datetime

from pydantic import field_validator

from cnes_contracts.schema import (
    ControlPlaneModel,
    JsonValue,
    optional_utc,
    require_finite_json,
    require_non_blank,
)


class OutboxEvent(ControlPlaneModel):
    tenant_id: str
    event_id: str
    event_type: str
    aggregate_id: str
    payload: dict[str, JsonValue]
    created_at: datetime
    delivered_at: datetime | None
    _strings = field_validator("tenant_id", "event_id", "event_type", "aggregate_id")(
        require_non_blank
    )
    _payload_value = field_validator("payload")(require_finite_json)
    _datetimes = field_validator("created_at", "delivered_at")(optional_utc)

    def __getattribute__(self, name: str) -> object:
        value = super().__getattribute__(name)
        return deepcopy(value) if name == "payload" else value
