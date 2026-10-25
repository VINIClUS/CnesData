"""Varreduras completas com paginação limitada e inspeção obrigatória."""

from collections.abc import Callable

from tests.integration.aws_capability._safety import SERVICES, SafetyFailure
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
    resources: list[Resource] = []
    for _ in range(limit):
        page = fetch(cursor)
        resources.extend(page.resources)
        if page.cursor is None:
            if len(set(resources)) != len(resources):
                raise SafetyFailure("inventory_incomplete")
            return resources
        if not isinstance(page.cursor, str) or not page.cursor or page.cursor in seen:
            raise SafetyFailure("inventory_incomplete")
        seen.add(page.cursor)
        cursor = page.cursor
    raise SafetyFailure("inventory_incomplete")


def inventory(ports: Ports, context: RequestContext) -> list[tuple[Resource, Ownership]]:
    rows: list[tuple[Resource, Ownership]] = []
    try:
        for service in SERVICES:
            rows.extend(inventory_service(service, ports, context))
    except ServiceFailure:
        raise SafetyFailure("inventory_incomplete") from None
    return rows


def inventory_service(
    service: str, ports: Ports, context: RequestContext
) -> list[tuple[Resource, Ownership]]:
    resources = pages(
        lambda cursor: ports.services.inventory(service, cursor, context),
        ports.policy.limits.pages,
    )
    rows = []
    for resource in resources:
        if resource.service != service:
            raise SafetyFailure("inventory_incomplete")
        rows.append((resource, ports.services.inspect(resource, context)))
    return rows


def dependents(resource: Resource, ports: Ports, context: RequestContext) -> list[Resource]:
    try:
        return pages(
            lambda cursor: ports.services.dependents(resource, cursor, context),
            ports.policy.limits.pages,
        )
    except ServiceFailure:
        raise SafetyFailure("inventory_incomplete") from None
