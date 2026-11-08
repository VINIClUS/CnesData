"""Cursor fechado para páginas DynamoDB."""

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from tests.integration.aws_capability.contracts import RunSpec, ServiceFailure

_FIELDS = {"v", "run", "service", "parent", "kind", "markers", "seen"}
_DDB_SCOPE = ("dynamodb", "dynamodb.tables", "ExclusiveStartTableName")
_COGNITO_SCOPE = ("cognito", "cognito.pools", "NextToken")
_S3_SCOPE = ("s3", "s3.buckets", "ContinuationToken")
_IAM_SCOPE = ("iam", "iam.roles", "Marker")


@dataclass(frozen=True)
class Cursor:
    marker: str | None
    seen: tuple[str, ...]


def parse_cursor(value: str | None, spec: RunSpec) -> Cursor:
    return _parse_cursor(value, spec, _DDB_SCOPE)


def parse_cognito_cursor(value: str | None, spec: RunSpec) -> Cursor:
    return _parse_cursor(value, spec, _COGNITO_SCOPE)


def parse_s3_cursor(value: str | None, spec: RunSpec) -> Cursor:
    """Returns:
    Cursor fechado do inventário de buckets S3.
    """
    return _parse_cursor(value, spec, _S3_SCOPE)


def parse_iam_cursor(value: str | None, spec: RunSpec) -> Cursor:
    """Returns:
    Cursor fechado do inventário de roles IAM.
    """
    return _parse_cursor(value, spec, _IAM_SCOPE)


def _parse_cursor(value: str | None, spec: RunSpec, scope: tuple[str, str, str]) -> Cursor:
    if value is None:
        return Cursor(None, ())
    payload = _decode(value)
    _validate_scope(payload, spec, scope)
    marker = _parse_marker(payload["markers"], scope[2])
    seen = _parse_seen(payload["seen"])
    if marker is None or not seen or _marker_digest(marker, scope) != seen[-1]:
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


def _validate_scope(payload: Mapping[str, Any], spec: RunSpec, scope: tuple[str, str, str]) -> None:
    if payload["v"] != 1 or isinstance(payload["v"], bool):
        raise ServiceFailure("service=cursor_invalid")
    if payload["run"] != spec.run_id or payload["service"] != scope[0]:
        raise ServiceFailure("service=cursor_invalid")
    if payload["parent"] is not None or payload["kind"] != scope[1]:
        raise ServiceFailure("service=cursor_invalid")


def _parse_marker(markers: object, marker_key: str) -> str | None:
    if not isinstance(markers, Mapping) or set(markers) not in (set(), {marker_key}):
        raise ServiceFailure("service=cursor_invalid")
    marker = markers.get(marker_key)
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
    return _next_cursor(marker, seen, run_id, _DDB_SCOPE)


def next_cognito_cursor(marker: str | None, seen: tuple[str, ...], run_id: str) -> str | None:
    return _next_cursor(marker, seen, run_id, _COGNITO_SCOPE)


def next_s3_cursor(marker: str | None, seen: tuple[str, ...], run_id: str) -> str | None:
    """Returns:
    Continuação S3 com fingerprint e histórico preservados.
    """
    return _next_cursor(marker, seen, run_id, _S3_SCOPE)


def next_iam_cursor(marker: str | None, seen: tuple[str, ...], run_id: str) -> str | None:
    """Returns:
    Continuação IAM com fingerprint e histórico preservados.
    """
    return _next_cursor(marker, seen, run_id, _IAM_SCOPE)


def _next_cursor(
    marker: str | None, seen: tuple[str, ...], run_id: str, scope: tuple[str, str, str]
) -> str | None:
    if marker is None:
        return None
    digest = _marker_digest(marker, scope)
    if digest in seen:
        raise ServiceFailure("service=cursor_cycle")
    payload = {
        "v": 1,
        "run": run_id,
        "service": scope[0],
        "parent": None,
        "kind": scope[1],
        "markers": {scope[2]: marker},
        "seen": [*seen, digest],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def marker_digest(marker: str) -> str:
    return _marker_digest(marker, _DDB_SCOPE)


def _marker_digest(marker: str, scope: tuple[str, str, str]) -> str:
    encoded = json.dumps({scope[2]: marker}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256((scope[1] + encoded).encode()).hexdigest()


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
