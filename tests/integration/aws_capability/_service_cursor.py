"""Cursor fechado para páginas DynamoDB."""

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from tests.integration.aws_capability.contracts import RunSpec, ServiceFailure

_FIELDS = {"v", "run", "service", "parent", "kind", "markers", "seen"}
_MARKER = "ExclusiveStartTableName"


@dataclass(frozen=True)
class Cursor:
    marker: str | None
    seen: tuple[str, ...]


def parse_cursor(value: str | None, spec: RunSpec) -> Cursor:
    if value is None:
        return Cursor(None, ())
    payload = _decode(value)
    _validate_scope(payload, spec)
    marker = _parse_marker(payload["markers"])
    seen = _parse_seen(payload["seen"])
    if marker is None or not seen or marker_digest(marker) != seen[-1]:
        raise ServiceFailure("service=cursor_invalid")
    return Cursor(marker, seen)


def _decode(value: str) -> Mapping[str, Any]:
    try:
        payload = json.loads(value, object_pairs_hook=_object)
    except (TypeError, ValueError, RecursionError):
        raise ServiceFailure("service=cursor_invalid") from None
    if not isinstance(payload, Mapping) or set(payload) != _FIELDS:
        raise ServiceFailure("service=cursor_invalid")
    return payload


def _validate_scope(payload: Mapping[str, Any], spec: RunSpec) -> None:
    if payload["v"] != 1 or isinstance(payload["v"], bool):
        raise ServiceFailure("service=cursor_invalid")
    if payload["run"] != spec.run_id or payload["service"] != "dynamodb":
        raise ServiceFailure("service=cursor_invalid")
    if payload["parent"] is not None or payload["kind"] != "dynamodb.tables":
        raise ServiceFailure("service=cursor_invalid")


def _parse_marker(markers: object) -> str | None:
    if not isinstance(markers, Mapping) or set(markers) not in (set(), {_MARKER}):
        raise ServiceFailure("service=cursor_invalid")
    marker = markers.get(_MARKER)
    if marker is not None and (not isinstance(marker, str) or not marker):
        raise ServiceFailure("service=cursor_invalid")
    return marker


def _parse_seen(raw_seen: object) -> tuple[str, ...]:
    seen = raw_seen
    if not isinstance(seen, list) or any(
        not isinstance(item, str) or len(item) != 64 or not _hex(item) for item in seen
    ):
        raise ServiceFailure("service=cursor_invalid")
    if len(set(seen)) != len(seen):
        raise ServiceFailure("service=cursor_invalid")
    return tuple(seen)


def next_cursor(marker: str | None, seen: tuple[str, ...], run_id: str) -> str | None:
    if marker is None:
        return None
    digest = marker_digest(marker)
    if digest in seen:
        raise ServiceFailure("service=cursor_cycle")
    payload = {
        "v": 1,
        "run": run_id,
        "service": "dynamodb",
        "parent": None,
        "kind": "dynamodb.tables",
        "markers": {_MARKER: marker},
        "seen": [*seen, digest],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def marker_digest(marker: str) -> str:
    encoded = json.dumps({_MARKER: marker}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(("dynamodb.tables" + encoded).encode()).hexdigest()


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate")
        result[key] = value
    return result


def _hex(value: str) -> bool:
    try:
        int(value, 16)
    except ValueError:
        return False
    return True
