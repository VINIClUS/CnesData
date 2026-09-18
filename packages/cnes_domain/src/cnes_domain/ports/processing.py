"""Porta de execução do processador e valores imutáveis."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, field_validator

if TYPE_CHECKING:
    from cnes_domain.control_plane.entities import Run, RunDispatch

_LOWER_HEX_16 = re.compile(r"^[0-9a-f]{16}$")


def _require_non_blank(value: str) -> str:
    if not value.strip():
        raise ValueError("blank_value")
    return value


def _optional_non_blank(value: str | None) -> str | None:
    return _require_non_blank(value) if value is not None else value


def _require_hex_id(value: str, name: str) -> str:
    if not _LOWER_HEX_16.fullmatch(value):
        raise ValueError(f"invalid_{name}")
    return value


def _require_positive(value: int) -> int:
    if value < 1:
        raise ValueError("positive_value_required")
    return value


def _require_non_negative(value: int) -> int:
    if value < 0:
        raise ValueError("negative_counter")
    return value


def _require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("datetime_not_utc")
    return value


def _require_unit_ids(values: tuple[str, ...]) -> tuple[str, ...]:
    if not values:
        raise ValueError("unit_ids_required")
    for value in values:
        _require_non_blank(value)
    if len(set(values)) != len(values):
        raise ValueError("duplicate_unit_id")
    return values


class _ExecutionModel(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")


class StartRunExecution(_ExecutionModel):
    tenant_id: str
    run_id: str
    wave_id: str
    dispatch_id: str
    unit_ids: tuple[str, ...]
    max_concurrency: int

    _identities = field_validator("tenant_id", "run_id")(_require_non_blank)
    _wave = field_validator("wave_id")(lambda value: _require_hex_id(value, "wave_id"))
    _dispatch = field_validator("dispatch_id")(
        lambda value: _require_hex_id(value, "dispatch_id")
    )
    _units = field_validator("unit_ids")(_require_unit_ids)
    _concurrency = field_validator("max_concurrency")(_require_positive)


class RunUnitMessage(_ExecutionModel):
    tenant_id: str
    run_id: str
    wave_id: str
    dispatch_id: str
    unit_id: str
    owner: str
    now: datetime
    lease_seconds: int

    _identities = field_validator("tenant_id", "run_id", "unit_id", "owner")(
        _require_non_blank
    )
    _wave = field_validator("wave_id")(lambda value: _require_hex_id(value, "wave_id"))
    _dispatch = field_validator("dispatch_id")(
        lambda value: _require_hex_id(value, "dispatch_id")
    )
    _now = field_validator("now")(_require_utc)
    _lease = field_validator("lease_seconds")(_require_positive)


class CancelRunExecution(_ExecutionModel):
    tenant_id: str
    run_id: str
    execution_ref: str | None

    _identities = field_validator("tenant_id", "run_id")(_require_non_blank)
    _execution_ref = field_validator("execution_ref")(_optional_non_blank)


class ExecutionStatus(StrEnum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"


@runtime_checkable
class ProcessorExecutorPort(Protocol):
    def start(self, request: StartRunExecution) -> str: ...
    def cancel(self, request: CancelRunExecution) -> None: ...
    def status(self, execution_ref: str) -> ExecutionStatus: ...


class ExecutionPermit(_ExecutionModel):
    tenant_id: str
    run_id: str
    max_concurrency: int
    policy_version: int
    fencing_token: int
    binding_context: object | None = None

    _identities = field_validator("tenant_id", "run_id")(_require_non_blank)
    _concurrency = field_validator("max_concurrency")(_require_positive)
    _counters = field_validator("policy_version", "fencing_token")(_require_non_negative)


type ConcurrencyPolicy = Callable[["Run", "RunDispatch", int], ExecutionPermit]
type ExecutionStarted = Callable[["Run", StartRunExecution, str, ExecutionPermit], None]


@dataclass(frozen=True, slots=True)
class ExecutionCallbacks:
    policy: ConcurrencyPolicy
    started: ExecutionStarted


@dataclass(frozen=True, slots=True)
class ExecutionPolicyConfig:
    deployment_limit: int
    dispatch_lease_seconds: int
    callbacks: ExecutionCallbacks

    def __post_init__(self) -> None:
        _require_positive(self.deployment_limit)
        _require_positive(self.dispatch_lease_seconds)
