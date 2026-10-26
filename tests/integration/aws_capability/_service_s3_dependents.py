"""Descoberta paginada de filhos S3 com prova de posse fresca."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

from tests.integration.aws_capability._resource_ids import parse_s3_child, s3_child
from tests.integration.aws_capability._safety import owned
from tests.integration.aws_capability._service_children_cursor import (
    next_s3_children_cursor,
    parse_s3_children_cursor,
)
from tests.integration.aws_capability._service_s3_children import (
    S3Page,
    _next_state,
    _read_page,
)
from tests.integration.aws_capability.contracts import (
    Page,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

if TYPE_CHECKING:
    from tests.integration.aws_capability.services import AwsServices


def s3_dependents(
    parent: Resource, cursor: str | None, services: "AwsServices", context: RequestContext
) -> Page:
    """Returns:
    Uma página de versões, markers ou uploads do bucket S3.
    Raises:
    ServiceFailure: identidade, cursor, posse ou leitura inválida.
    """
    if _is_child(parent):
        if cursor is not None:
            raise ServiceFailure("service=cursor_invalid")
        parse_s3_child(parent, services.spec)
        services.inspect(parent, context)
        return Page()
    state = parse_s3_children_cursor(cursor, parent, services.spec)
    owner = services.inspect(parent, context)
    if not owned(parent, owner, services.spec):
        raise ServiceFailure("service=not_owned")
    data = _read_page(services.ports.commands, S3Page(services.spec, parent, state), context)
    resources = _resources(data, state.kind, parent, services.spec)
    next_state = _next_state(data, state)
    if next_state is None:
        return Page(resources)
    return Page(resources, next_s3_children_cursor(next_state, parent, services.spec))


def _is_child(resource: Resource) -> bool:
    return resource.resource_id != resource.name


def _resources(
    data: Mapping[str, object], kind: str, parent: Resource, spec: RunSpec
) -> tuple[Resource, ...]:
    _validate_parent(data, parent)
    groups = ("Uploads",) if kind == "s3.uploads" else ("Versions", "DeleteMarkers")
    output: list[Resource] = []
    for group in groups:
        for row in _rows(data, group):
            if kind == "s3.uploads":
                child_kind = "s3.upload"
                child = {"key": cast("str", row["Key"]), "upload_id": cast("str", row["UploadId"])}
            else:
                child_kind = "s3.marker" if group == "DeleteMarkers" else "s3.version"
                child = {
                    "key": cast("str", row["Key"]),
                    "version_id": cast("str", row["VersionId"]),
                }
            output.append(s3_child(parent, spec, child_kind, child))
    return tuple(output)


def _rows(data: Mapping[str, object], group: str) -> tuple[Mapping[str, object], ...]:
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


def _validate_parent(data: Mapping[str, object], parent: Resource) -> None:
    field = "Bucket" if "Uploads" in data else "Name"
    if field in data and data[field] != parent.resource_id:
        raise ServiceFailure("service=inventory_invalid")
