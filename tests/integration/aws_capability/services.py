"""Interfaces de leitura AWS para a fatia B2/R1."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from tests.integration.aws_capability._service_cognito import (
    inspect_pool,
    list_pools,
    lookup_pool,
)
from tests.integration.aws_capability._service_cognito_children import (
    inspect_cognito_child,
    lookup_cognito_child,
)
from tests.integration.aws_capability._service_cognito_dependents import cognito_dependents
from tests.integration.aws_capability._service_commands import call_cli
from tests.integration.aws_capability._service_contracts import Reply, ServicePorts
from tests.integration.aws_capability._service_dynamodb import (
    inspect_table,
    leaf_dependents,
    lookup_table,
)
from tests.integration.aws_capability._service_dynamodb_create import create_table
from tests.integration.aws_capability._service_iam import inspect_role, list_roles, lookup_role
from tests.integration.aws_capability._service_iam_children import (
    inspect_iam_child,
    lookup_iam_child,
)
from tests.integration.aws_capability._service_iam_dependents import iam_dependents
from tests.integration.aws_capability._service_identity import read_identity
from tests.integration.aws_capability._service_inventory import list_tables
from tests.integration.aws_capability._service_s3 import inspect_bucket, list_buckets, lookup_bucket
from tests.integration.aws_capability._service_s3_children import (
    inspect_s3_child,
    lookup_s3_child,
)
from tests.integration.aws_capability._service_s3_dependents import s3_dependents
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
        if service == "iam":
            return list_roles(self.ports.commands, self.spec, cursor, context)
        if service == "s3":
            return list_buckets(self.ports.commands, self.spec, cursor, context)
        if service == "cognito":
            return list_pools(self.ports.commands, self.spec, cursor, context)
        if service != "dynamodb":
            raise ServiceFailure("service=service_unavailable")
        return list_tables(self.ports.commands, self.spec, cursor, context)

    def inspect(self, resource: Resource, context: RequestContext) -> Ownership:
        if resource.service == "iam":
            inspect = inspect_iam_child if resource.resource_id != resource.name else inspect_role
            return inspect(self.ports.commands, self.spec, resource, context)
        if resource.service == "s3":
            if _is_s3_child(resource):
                return inspect_s3_child(self.ports.commands, self.spec, resource, context)
            return inspect_bucket(self.ports.commands, self.spec, resource, context)
        if resource.service == "cognito":
            if isinstance(resource.resource_id, str) and resource.resource_id.lstrip().startswith(
                "{"
            ):
                return inspect_cognito_child(self.ports.commands, self.spec, resource, context)
            return inspect_pool(self.ports.commands, self.spec, resource, context)
        _require_dynamodb(resource)
        return inspect_table(self.ports.commands, self.spec, resource, context)

    def lookup(self, resource: Resource, context: RequestContext) -> Ownership | None:
        if resource.service == "iam":
            lookup = lookup_iam_child if resource.resource_id != resource.name else lookup_role
            return lookup(self.ports.commands, self.spec, resource, context)
        if resource.service == "s3":
            if _is_s3_child(resource):
                return lookup_s3_child(self.ports.commands, self.spec, resource, context)
            return lookup_bucket(self.ports.commands, self.spec, resource, context)
        if resource.service == "cognito":
            if isinstance(resource.resource_id, str) and resource.resource_id.lstrip().startswith(
                "{"
            ):
                return lookup_cognito_child(self.ports.commands, self.spec, resource, context)
            return lookup_pool(self.ports.commands, self.spec, resource, context)
        _require_dynamodb(resource)
        return lookup_table(self.ports.commands, self.spec, resource, context)

    def dependents(self, resource: Resource, cursor: str | None, context: RequestContext) -> Page:
        if resource.service == "iam":
            return iam_dependents(resource, cursor, self, context)
        if resource.service == "s3":
            return s3_dependents(resource, cursor, self, context)
        if resource.service == "cognito":
            return cognito_dependents(resource, cursor, self, context)
        _require_dynamodb(resource)
        if cursor is not None:
            raise ServiceFailure("service=cursor_invalid")
        return leaf_dependents(self.ports.commands, self.spec, resource, context)

    def create(self, service: str, creation: Creation, context: RequestContext) -> Resource:
        if service != "dynamodb":
            raise ServiceFailure("service=not_implemented")
        return create_table(self.ports.commands, self.spec, creation, context)

    def probe(self, resources: Sequence[Resource], spec: RunSpec, context: RequestContext) -> None:
        raise ServiceFailure("service=not_implemented")

    def delete(self, resource: Resource, bypass: bool, context: RequestContext) -> None:
        raise ServiceFailure("service=not_implemented")


def _require_dynamodb(resource: Resource) -> None:
    if resource.service != "dynamodb":
        raise ServiceFailure("service=service_unavailable")
    if resource.resource_id != resource.name:
        raise ServiceFailure("service=resource_invalid")


def _is_s3_child(resource: Resource) -> bool:
    return resource.resource_id != resource.name
