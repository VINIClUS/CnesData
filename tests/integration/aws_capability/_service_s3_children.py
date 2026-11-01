"""Leituras frescas de filhos S3 por identidade pontual ou enumeração conservadora."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from tests.integration.aws_capability._resource_ids import parse_s3_child
from tests.integration.aws_capability._safety import owned
from tests.integration.aws_capability._service_children_cursor import (
    S3ChildrenCursor,
    s3_marker_digest,
)
from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability._service_s3 import lookup_bucket
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Limits,
    Ownership,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def lookup_s3_child(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership | None:
    """Returns:
    Posse fresca ou None para NoSuchUpload remoto após prova do root.
    Raises:
    ServiceFailure: identidade, posse ou leitura inconclusiva.
    """
    root, kind, child = parse_s3_child(resource, spec)
    owner = lookup_bucket(commands, spec, root, context)
    if owner is None:
        raise ServiceFailure("service=lookup_inconclusive")
    if not owned(root, owner, spec):
        raise ServiceFailure("service=not_owned")
    if kind == "s3.upload":
        request = _upload_request(spec, root, child)
        return owner if _upload_present(commands, request, context) else None
    state = S3ChildrenCursor("s3.versions", {}, ())
    seen: set[str] = set()
    for _ in range(Limits().pages):
        data = _read_page(commands, S3Page(spec, root, state), context)
        if _contains(data, kind, child):
            return owner
        next_state = _next_state(data, state)
        if data["IsTruncated"] is False or next_state is None:
            if context.lane == "cleanup":
                return None
            break
        digest = s3_marker_digest(next_state.kind, next_state.markers)
        if digest in seen:
            raise ServiceFailure("service=cursor_cycle")
        seen.add(digest)
        state = next_state
    raise ServiceFailure("service=lookup_inconclusive")


def inspect_s3_child(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership:
    """Returns:
    Posse fresca após prova completa da presença do filho S3.
    Raises:
    ServiceFailure: filho ausente ou leitura inválida.
    """
    owner = lookup_s3_child(commands, spec, resource, context)
    if owner is None:
        raise ServiceFailure("service=not_found")
    return owner


def _upload_request(spec: RunSpec, root: Resource, child: Mapping[str, str]) -> Request:
    return Request(
        "s3api",
        "list-parts",
        {
            "Bucket": root.resource_id,
            "Key": child["key"],
            "UploadId": child["upload_id"],
            "ExpectedBucketOwner": spec.account,
            "MaxParts": 1,
        },
    )


def _upload_present(commands: CommandPort, request: Request, context: RequestContext) -> bool:
    reply = call_command(commands, request, context)
    if (
        not reply.ok
        and reply.error is not None
        and reply.error.origin == "service"
        and reply.error.code == "NoSuchUpload"
    ):
        return False
    data = require_data(reply)
    fields = ("Bucket", "Key", "UploadId")
    if any(not isinstance(data.get(field), str) or not data[field] for field in fields):
        raise ServiceFailure("service=inventory_invalid")
    if "IsTruncated" in data and type(data["IsTruncated"]) is not bool:
        raise ServiceFailure("service=inventory_invalid")
    parts = data.get("Parts", [])
    if not isinstance(parts, list) or any(not isinstance(part, Mapping) for part in parts):
        raise ServiceFailure("service=inventory_invalid")
    if any(data[field] != request.payload[field] for field in fields):
        raise ServiceFailure("service=lookup_inconclusive")
    return True


@dataclass(frozen=True)
class S3Page:
    """Args:
    spec: escopo da execução; root: bucket; state: coleção e continuação.
    """

    spec: RunSpec
    root: Resource
    state: S3ChildrenCursor


def _read_page(
    commands: CommandPort, page: S3Page, context: RequestContext
) -> Mapping[str, object]:
    operation, limit_key, limit = _operation(page.state.kind)
    payload: dict[str, object] = {
        "Bucket": page.root.resource_id,
        "ExpectedBucketOwner": page.spec.account,
        limit_key: limit,
        **page.state.markers,
    }
    data = require_data(call_command(commands, Request("s3api", operation, payload), context))
    field = "Bucket" if page.state.kind == "s3.uploads" else "Name"
    if field in data and data[field] != page.root.resource_id:
        raise ServiceFailure("service=inventory_invalid")
    _validate_page(data, page.state.kind)
    groups = ("Uploads",) if page.state.kind == "s3.uploads" else ("Versions", "DeleteMarkers")
    for group in groups:
        _group_rows(data, group)
    return data


def _operation(kind: str) -> tuple[str, str, int]:
    if kind == "s3.versions":
        return "list-object-versions", "MaxKeys", 1000
    if kind == "s3.uploads":
        return "list-multipart-uploads", "MaxUploads", 1000
    raise ServiceFailure("service=cursor_invalid")


def _contains(data: Mapping[str, object], kind: str, child: Mapping[str, str]) -> bool:
    groups = ("DeleteMarkers",) if kind == "s3.marker" else ("Versions",)
    value_key, wire_key = "version_id", "VersionId"
    for row in _group_rows(data, groups[0]):
        if row.get("Key") == child["key"] and row.get(wire_key) == child[value_key]:
            return True
    return False


def _group_rows(data: Mapping[str, object], group: str) -> tuple[Mapping[str, object], ...]:
    value = data.get(group, [])
    if not isinstance(value, list):
        raise ServiceFailure("service=inventory_invalid")
    rows: list[Mapping[str, object]] = []
    for row in value:
        if not isinstance(row, Mapping):
            raise ServiceFailure("service=inventory_invalid")
        fields = ("Key", "UploadId") if group == "Uploads" else ("Key", "VersionId")
        if any(not isinstance(row.get(field), str) or not row[field] for field in fields):
            raise ServiceFailure("service=inventory_invalid")
        rows.append(row)
    return tuple(rows)


def _next_state(data: Mapping[str, object], state: S3ChildrenCursor) -> S3ChildrenCursor | None:
    _validate_page(data, state.kind)
    if data["IsTruncated"] is False:
        return (
            None if state.kind == "s3.uploads" else S3ChildrenCursor("s3.uploads", {}, state.seen)
        )
    markers = _next_markers(data, state.kind)
    digest = s3_marker_digest(state.kind, markers)
    if digest in state.seen:
        raise ServiceFailure("service=cursor_cycle")
    return S3ChildrenCursor(state.kind, markers, state.seen)


def _validate_page(data: Mapping[str, object], kind: str) -> None:
    truncated = data.get("IsTruncated")
    if type(truncated) is not bool:
        raise ServiceFailure("service=inventory_invalid")
    for key in ("KeyMarker", "VersionIdMarker", "UploadIdMarker"):
        if key in data and not isinstance(data[key], str):
            raise ServiceFailure("service=inventory_invalid")
    next_keys = (
        ("NextKeyMarker", "NextVersionIdMarker")
        if kind == "s3.versions"
        else ("NextKeyMarker", "NextUploadIdMarker")
    )
    if truncated is False:
        if any(key in data for key in next_keys):
            raise ServiceFailure("service=inventory_invalid")
        return
    primary = data.get("NextKeyMarker")
    if not isinstance(primary, str) or not primary:
        raise ServiceFailure("service=inventory_invalid")
    secondary = next_keys[1]
    if secondary in data and (not isinstance(data[secondary], str) or not data[secondary]):
        raise ServiceFailure("service=inventory_invalid")


def _next_markers(data: Mapping[str, object], kind: str) -> dict[str, str]:
    secondary = "NextVersionIdMarker" if kind == "s3.versions" else "NextUploadIdMarker"
    markers = {"KeyMarker": cast("str", data["NextKeyMarker"])}
    if secondary in data:
        markers[secondary.replace("Next", "")] = cast("str", data[secondary])
    return markers
