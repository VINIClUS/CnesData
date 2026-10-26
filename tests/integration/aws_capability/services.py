"""Interfaces de leitura AWS para a fatia B2/R1."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from tests.integration.aws_capability._service_commands import call_cli
from tests.integration.aws_capability._service_contracts import Reply, ServicePorts
from tests.integration.aws_capability._service_dynamodb import (
    inspect_table,
    leaf_dependents,
    lookup_table,
)
from tests.integration.aws_capability._service_identity import read_identity
from tests.integration.aws_capability._service_inventory import list_tables
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Creation,
    Ownership,
    Page,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


@dataclass(frozen=True)
class AwsCommands:
    region: str
    environment: Mapping[str, str] = field(repr=False)
    timeout: float

    def call(self, request: Request, context: RequestContext) -> Reply:
        return call_cli(self.region, self.environment, self.timeout, request, context)


@dataclass(frozen=True)
class AwsServices:
    spec: RunSpec = field(repr=False)
    ports: ServicePorts = field(repr=False)

    def identity(self, context: RequestContext) -> str:
        return read_identity(self.ports.commands, self.spec, context)

    def inventory(self, service: str, cursor: str | None, context: RequestContext) -> Page:
        if service != "dynamodb":
            raise ServiceFailure("service=service_unavailable")
        return list_tables(self.ports.commands, self.spec, cursor, context)

    def inspect(self, resource: Resource, context: RequestContext) -> Ownership:
        _require_dynamodb(resource)
        return inspect_table(self.ports.commands, self.spec, resource, context)

    def lookup(self, resource: Resource, context: RequestContext) -> Ownership | None:
        _require_dynamodb(resource)
        return lookup_table(self.ports.commands, self.spec, resource, context)

    def dependents(self, resource: Resource, cursor: str | None, context: RequestContext) -> Page:
        _require_dynamodb(resource)
        if cursor is not None:
            raise ServiceFailure("service=cursor_invalid")
        return leaf_dependents(self.ports.commands, self.spec, resource, context)

    def create(self, service: str, creation: Creation, context: RequestContext) -> Resource:
        raise ServiceFailure("service=not_implemented")

    def probe(self, resources: Sequence[Resource], spec: RunSpec, context: RequestContext) -> None:
        raise ServiceFailure("service=not_implemented")

    def delete(self, resource: Resource, bypass: bool, context: RequestContext) -> None:
        raise ServiceFailure("service=not_implemented")


def _require_dynamodb(resource: Resource) -> None:
    if resource.service != "dynamodb":
        raise ServiceFailure("service=service_unavailable")
