"""Cursores fechados das coleções de filhos Cognito."""

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field

from tests.integration.aws_capability.contracts import Limits, Resource, RunSpec, ServiceFailure

_FIELDS = {"v", "run", "service", "parent", "kind", "markers", "seen"}
_MARKERS = {
    "cognito.clients": "NextToken",
    "cognito.servers": "NextToken",
    "cognito.users": "PaginationToken",
}


@dataclass(frozen=True)
class ChildrenCursor:
    kind: str
    markers: Mapping[str, str] = field(repr=False)
    seen: tuple[str, ...] = field(repr=False)


@dataclass(frozen=True)
class S3ChildrenCursor:
    """Args:
    kind: coleção; markers: continuação; seen: histórico de digests.
    """

    kind: str
    markers: Mapping[str, str] = field(repr=False)
    seen: tuple[str, ...] = field(repr=False)


_S3_MARKERS = {
    "s3.versions": ("KeyMarker", "VersionIdMarker"),
    "s3.uploads": ("KeyMarker", "UploadIdMarker"),
}
_IAM_KINDS = {"iam.policies", "iam.attachments", "iam.profiles"}


@dataclass(frozen=True)
class IamChildrenCursor:
    """Args:
    kind: coleção IAM; markers: continuação privada; seen: histórico de digests.
    """

    kind: str
    markers: Mapping[str, str] = field(repr=False)
    seen: tuple[str, ...] = field(repr=False)


def parse_iam_children_cursor(
    value: str | None, parent: Resource, spec: RunSpec
) -> IamChildrenCursor:
    """Returns:
    Estado IAM após validar escopo, markers e histórico.
    Raises:
    ServiceFailure: cursor inválido.
    """
    if value is None:
        return IamChildrenCursor("iam.policies", {}, ())
    payload = _decode(value)
    if (
        type(payload["v"]) is not int
        or payload["v"] != 1
        or payload["run"] != spec.run_id
        or payload["service"] != "iam"
        or payload["parent"] != parent.resource_id
    ):
        raise ServiceFailure("service=cursor_invalid")
    kind = payload["kind"]
    if not isinstance(kind, str) or kind not in _IAM_KINDS:
        raise ServiceFailure("service=cursor_invalid")
    raw = payload["markers"]
    if not isinstance(raw, Mapping) or set(raw) not in (set(), {"Marker"}):
        raise ServiceFailure("service=cursor_invalid")
    markers: dict[str, str] = {}
    for key, marker in raw.items():
        if not isinstance(marker, str) or not marker:
            raise ServiceFailure("service=cursor_invalid")
        markers[key] = marker
    seen = _parse_seen(payload["seen"])
    if _digest(kind, markers) != seen[-1]:
        raise ServiceFailure("service=cursor_invalid")
    return IamChildrenCursor(kind, markers, seen)


def next_iam_children_cursor(state: IamChildrenCursor, parent: Resource, spec: RunSpec) -> str:
    """Returns:
    Cursor IAM da próxima coleção ou página sem leitura antecipada.
    Raises:
    ServiceFailure: ciclo ou histórico excessivo.
    """
    digest = _digest(state.kind, state.markers)
    if digest in state.seen:
        raise ServiceFailure("service=cursor_cycle")
    if len(state.seen) >= Limits().pages:
        raise ServiceFailure("service=cursor_invalid")
    payload = {
        "v": 1,
        "run": spec.run_id,
        "service": "iam",
        "parent": parent.resource_id,
        "kind": state.kind,
        "markers": dict(state.markers),
        "seen": [*state.seen, digest],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def parse_children_cursor(value: str | None, parent: Resource, spec: RunSpec) -> ChildrenCursor:
    """Returns:
    Estado da coleção corrente após validar escopo, markers e histórico.
    Raises:
    ServiceFailure: cursor inválido.
    """
    if value is None:
        return ChildrenCursor("cognito.domains", {}, ())
    payload = _decode(value)
    if type(payload["v"]) is not int or payload["v"] != 1:
        raise ServiceFailure("service=cursor_invalid")
    if payload["run"] != spec.run_id or payload["service"] != "cognito":
        raise ServiceFailure("service=cursor_invalid")
    if payload["parent"] != parent.resource_id:
        raise ServiceFailure("service=cursor_invalid")
    kind = payload["kind"]
    if not isinstance(kind, str) or kind not in _MARKERS:
        raise ServiceFailure("service=cursor_invalid")
    markers = _parse_markers(payload["markers"], kind)
    seen = _parse_seen(payload["seen"])
    if _digest(kind, markers) != seen[-1]:
        raise ServiceFailure("service=cursor_invalid")
    return ChildrenCursor(kind, markers, seen)


def next_children_cursor(state: ChildrenCursor, parent: Resource, spec: RunSpec) -> str:
    """Returns:
    Continuação da coleção ou transição vazia sem consumir outra lista.
    Raises:
    ServiceFailure: ciclo ou histórico excessivo.
    """
    digest = _digest(state.kind, state.markers)
    if digest in state.seen:
        raise ServiceFailure("service=cursor_cycle")
    if len(state.seen) >= Limits().pages:
        raise ServiceFailure("service=cursor_invalid")
    payload = {
        "v": 1,
        "run": spec.run_id,
        "service": "cognito",
        "parent": parent.resource_id,
        "kind": state.kind,
        "markers": dict(state.markers),
        "seen": [*state.seen, digest],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def parse_s3_children_cursor(
    value: str | None, parent: Resource, spec: RunSpec
) -> S3ChildrenCursor:
    """Returns:
    Estado da coleção S3 após validar escopo, markers e histórico.
    Raises:
    ServiceFailure: cursor inválido.
    """
    if value is None:
        return S3ChildrenCursor("s3.versions", {}, ())
    payload = _decode(value)
    if type(payload["v"]) is not int or payload["v"] != 1:
        raise ServiceFailure("service=cursor_invalid")
    if payload["run"] != spec.run_id or payload["service"] != "s3":
        raise ServiceFailure("service=cursor_invalid")
    if payload["parent"] != parent.resource_id:
        raise ServiceFailure("service=cursor_invalid")
    kind = payload["kind"]
    if not isinstance(kind, str) or kind not in _S3_MARKERS:
        raise ServiceFailure("service=cursor_invalid")
    markers = _parse_s3_markers(payload["markers"], kind)
    seen = _parse_seen(payload["seen"])
    if _digest(kind, markers) != seen[-1]:
        raise ServiceFailure("service=cursor_invalid")
    return S3ChildrenCursor(kind, markers, seen)


def next_s3_children_cursor(state: S3ChildrenCursor, parent: Resource, spec: RunSpec) -> str:
    """Returns:
    Cursor S3 da próxima coleção ou página, sem consumir leitura antecipada.
    Raises:
    ServiceFailure: ciclo ou histórico excessivo.
    """
    digest = _digest(state.kind, state.markers)
    if digest in state.seen:
        raise ServiceFailure("service=cursor_cycle")
    if len(state.seen) >= Limits().pages:
        raise ServiceFailure("service=cursor_invalid")
    payload = {
        "v": 1,
        "run": spec.run_id,
        "service": "s3",
        "parent": parent.resource_id,
        "kind": state.kind,
        "markers": dict(state.markers),
        "seen": [*state.seen, digest],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def s3_marker_digest(kind: str, markers: Mapping[str, str]) -> str:
    """Returns:
    Digest canônico da coleção e dos markers S3.
    Raises:
    ServiceFailure: coleção inválida.
    """
    if kind not in _S3_MARKERS:
        raise ServiceFailure("service=cursor_invalid")
    return _digest(kind, markers)


def _decode(value: str) -> Mapping[str, object]:
    try:
        payload = json.loads(value, object_pairs_hook=_object)
    except (TypeError, ValueError, RecursionError):
        raise ServiceFailure("service=cursor_invalid") from None
    if not isinstance(payload, Mapping) or set(payload) != _FIELDS:
        raise ServiceFailure("service=cursor_invalid")
    return payload


def _object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate")
        result[key] = value
    return result


def _parse_markers(raw: object, kind: str) -> dict[str, str]:
    if not isinstance(raw, Mapping) or set(raw) not in (set(), {_MARKERS[kind]}):
        raise ServiceFailure("service=cursor_invalid")
    markers: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(value, str) or not value:
            raise ServiceFailure("service=cursor_invalid")
        markers[key] = value
    return markers


def _parse_s3_markers(raw: object, kind: str) -> dict[str, str]:
    keys = _S3_MARKERS[kind]
    if not isinstance(raw, Mapping) or set(raw) not in (
        set(),
        {keys[0]},
        set(keys),
    ):
        raise ServiceFailure("service=cursor_invalid")
    markers: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(value, str) or not value:
            raise ServiceFailure("service=cursor_invalid")
        markers[key] = value
    return markers


def _parse_seen(raw: object) -> tuple[str, ...]:
    if not isinstance(raw, list) or not 1 <= len(raw) <= Limits().pages:
        raise ServiceFailure("service=cursor_invalid")
    seen: list[str] = []
    for value in raw:
        if not isinstance(value, str) or re.fullmatch("[0-9a-f]{64}", value) is None:
            raise ServiceFailure("service=cursor_invalid")
        seen.append(value)
    if len(set(seen)) != len(seen):
        raise ServiceFailure("service=cursor_invalid")
    return tuple(seen)


def _digest(kind: str, markers: Mapping[str, str]) -> str:
    encoded = json.dumps(dict(markers), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256((kind + encoded).encode()).hexdigest()
