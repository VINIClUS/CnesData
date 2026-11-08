"""Tipos e validações estruturais dos contratos compartilhados."""
from datetime import datetime, timedelta
from math import isfinite

from pydantic import BaseModel, ConfigDict

type JsonValue = bool | int | float | str | list[JsonValue] | dict[str, JsonValue] | None

def require_non_blank(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("blank_value")
    return value

def optional_non_blank(value: str | None) -> str | None:
    return require_non_blank(value) if value is not None else value

def require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("datetime_not_utc")
    return value

def optional_utc(value: datetime | None) -> datetime | None:
    return require_utc(value) if value is not None else value

def require_finite_json(value: JsonValue) -> JsonValue:
    if isinstance(value, float) and not isfinite(value):
        raise ValueError("non_finite_json_float")
    if isinstance(value, list):
        for item in value:
            require_finite_json(item)
    elif isinstance(value, dict):
        for item in value.values():
            require_finite_json(item)
    return value

class ControlPlaneModel(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")
