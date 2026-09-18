"""Object store scoping stage writes to the current unit attempt."""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import BinaryIO, Mapping
    from contextlib import AbstractContextManager as ContextManager

    from cnes_domain.control_plane.entities import RunUnit
    from cnes_domain.ports.object_store import ObjectStat, ObjectStorePort

_FORBIDDEN_SEGMENTS = frozenset({"", ".", ".."})


def unit_attempt_prefix(unit: RunUnit) -> str:
    return f"tmp/{unit.tenant_id}/{unit.run_id}/{unit.unit_id}/{unit.attempt}"


def _validate_key(key: str) -> None:
    if not key or key.startswith("/") or key.endswith("/") or "//" in key or "\\" in key:
        raise ValueError(f"invalid_logical_key key={key}")
    if any(part in _FORBIDDEN_SEGMENTS for part in key.split("/")):
        raise ValueError(f"invalid_logical_key key={key}")


def attempt_object_key(prefix: str, logical_key: str) -> str:
    _validate_key(logical_key)
    return f"{prefix}/{logical_key}"


def _validate_inputs(inputs: Mapping[str, str], prefix: str) -> None:
    physical_targets: list[str] = []
    for logical_key, physical_key in inputs.items():
        _validate_key(logical_key)
        _validate_key(physical_key)
        if physical_key == prefix or physical_key.startswith(f"{prefix}/"):
            raise ValueError(f"input_under_attempt_prefix key={physical_key}")
        physical_targets.append(physical_key)
    if len(set(physical_targets)) != len(physical_targets):
        raise ValueError("duplicate_input_target")


@dataclass(frozen=True, slots=True)
class AttemptObjectStore:
    delegate: ObjectStorePort
    prefix: str
    inputs: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_inputs(self.inputs, self.prefix)

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        stat = self.delegate.put(attempt_object_key(self.prefix, key), body, expected_sha256)
        return replace(stat, key=key)

    def open(self, key: str) -> ContextManager[BinaryIO]:
        physical_key = self.inputs.get(key)
        if physical_key is None:
            raise ValueError(f"input_not_allowlisted key={key}")
        return self.delegate.open(physical_key)

    def stat(self, key: str) -> ObjectStat | None:
        current = self.delegate.stat(attempt_object_key(self.prefix, key))
        if current is not None:
            return replace(current, key=key)
        physical_key = self.inputs.get(key)
        if physical_key is None:
            return None
        found = self.delegate.stat(physical_key)
        return None if found is None else replace(found, key=key)

    def delete(self, key: str) -> None:
        self.delegate.delete(attempt_object_key(self.prefix, key))

    def promote(
        self, source_key: str, destination_key: str, expected_sha256: str
    ) -> ObjectStat:
        raise RuntimeError("promote_forbidden")

    def with_inputs(self, inputs: Mapping[str, str]) -> AttemptObjectStore:
        return replace(self, inputs=dict(inputs))
