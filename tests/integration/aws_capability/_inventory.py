"""Varreduras completas com paginação limitada e inspeção obrigatória."""

from collections.abc import Callable, Iterable

from tests.integration.aws_capability._resource_ids import s3_logical_identity
from tests.integration.aws_capability._safety import CAP_PREFIX, SERVICES, SafetyFailure
from tests.integration.aws_capability.contracts import (
    Ownership,
    Page,
    Ports,
    RequestContext,
    Resource,
    ServiceFailure,
)


def pages(fetch: Callable[[str | None], Page], limit: int) -> list[Resource]:
    cursor = None
    seen: set[str] = set()
    seen_resources: set[Resource] = set()
    seen_identities: set[object] = set()
    resources: list[Resource] = []
    for _ in range(limit):
        page = fetch(cursor)
        for resource in page.resources:
            identity = s3_logical_identity(resource)
            if not isinstance(identity, Resource) and identity in seen_identities:
                raise SafetyFailure("inventory_incomplete")
            seen_resources.add(resource)
            if not isinstance(identity, Resource):
                seen_identities.add(identity)
            resources.append(resource)
        if page.cursor is None:
            if len(seen_resources) != len(resources):
                raise SafetyFailure("inventory_incomplete")
            return resources
        if not isinstance(page.cursor, str) or not page.cursor or page.cursor in seen:
            raise SafetyFailure("inventory_incomplete")
        seen.add(page.cursor)
        cursor = page.cursor
    raise SafetyFailure("inventory_incomplete")


def service_counts(resources: Iterable[Resource]) -> dict[str, int]:
    refs = list(resources)
    return {service: sum(ref.service == service for ref in refs) for service in SERVICES}


def inventory(
    ports: Ports, context: RequestContext, known: frozenset[tuple[str, str]] = frozenset()
) -> list[tuple[Resource, Ownership]]:
    rows: list[tuple[Resource, Ownership]] = []
    try:
        for service in SERVICES:
            rows.extend(inventory_service(service, ports, context, known))
    except ServiceFailure:
        raise SafetyFailure("inventory_incomplete") from None
    return rows


def inventory_service(
    service: str,
    ports: Ports,
    context: RequestContext,
    known: frozenset[tuple[str, str]] = frozenset(),
) -> list[tuple[Resource, Ownership]]:
    resources = pages(
        lambda cursor: ports.services.inventory(service, cursor, context),
        ports.policy.limits.pages,
    )
    if any(resource.service != service for resource in resources):
        raise SafetyFailure("inventory_incomplete")
    return [
        (resource, ports.services.inspect(resource, context))
        for resource in resources
        if resource.name.startswith(CAP_PREFIX) or (resource.service, resource.resource_id) in known
    ]


def dependents(resource: Resource, ports: Ports, context: RequestContext) -> list[Resource]:
    try:
        return pages(
            lambda cursor: ports.services.dependents(resource, cursor, context),
            ports.policy.limits.pages,
        )
    except ServiceFailure:
        raise SafetyFailure("inventory_incomplete") from None
