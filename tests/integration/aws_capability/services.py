"""Interfaces de leitura AWS para a fatia B2/R1."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from tests.integration.aws_capability._dynamodb_probe import probe_dynamodb
from tests.integration.aws_capability._resource_ids import cognito_child
from tests.integration.aws_capability._s3_probe import probe_s3
from tests.integration.aws_capability._service_cognito import (
    inspect_pool,
    list_pools,
    lookup_pool,
)
from tests.integration.aws_capability._service_cognito_children import (
    inspect_cognito_child,
    lookup_cognito_child,
)
from tests.integration.aws_capability._service_cognito_create import create_pool
from tests.integration.aws_capability._service_cognito_delete import delete_pool
from tests.integration.aws_capability._service_cognito_dependents import cognito_dependents
from tests.integration.aws_capability._service_cognito_prepare import prepare_pool
from tests.integration.aws_capability._service_cognito_server_create import (
    create_server,
    lookup_server_exact,
)
from tests.integration.aws_capability._service_cognito_server_delete import delete_server
from tests.integration.aws_capability._service_contracts import Reply, ServicePorts
from tests.integration.aws_capability._service_dynamodb import (
    inspect_table,
    leaf_dependents,
    lookup_table,
)
from tests.integration.aws_capability._service_dynamodb_create import create_table
from tests.integration.aws_capability._service_dynamodb_delete import (
    await_table_gone,
    delete_table,
)
from tests.integration.aws_capability._service_iam import inspect_role, list_roles, lookup_role
from tests.integration.aws_capability._service_iam_children import (
    inspect_iam_child,
    lookup_iam_child,
)
from tests.integration.aws_capability._service_iam_create import create_role
from tests.integration.aws_capability._service_iam_delete import delete_iam
from tests.integration.aws_capability._service_iam_dependents import iam_dependents
from tests.integration.aws_capability._service_identity import read_identity
from tests.integration.aws_capability._service_inventory import list_tables
from tests.integration.aws_capability._service_s3 import inspect_bucket, list_buckets, lookup_bucket
from tests.integration.aws_capability._service_s3_children import (
    inspect_s3_child,
    lookup_s3_child,
)
from tests.integration.aws_capability._service_s3_create import create_bucket
from tests.integration.aws_capability._service_s3_delete import delete_s3
from tests.integration.aws_capability._service_s3_dependents import s3_dependents
from tests.integration.aws_capability._throughput_probe import probe_throughput
from tests.integration.aws_capability.cli import Cli, Request
from tests.integration.aws_capability.contracts import (
    Creation,
    Ownership,
    Page,
    RequestContext,
    Resource,
    RunSpec,
    ServerLookup,
    ServiceFailure,
)

S3_PROFILES = frozenset({"s3-hold", "s3-retention"})
IAM_ROLE_PROFILES = frozenset({"s3-retention", "iam-trust"})


@dataclass(frozen=True)
class AwsCommands:
    region: str
    environment: Mapping[str, str] = field(repr=False)
    timeout: float

    def call(self, request: Request, context: RequestContext) -> Reply:
        service_region = "us-east-1" if request.service in {"sts", "iam"} else self.region
        return Cli(service_region, self.timeout, self.environment, context).call(request)


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
            return self._inspect_cognito(resource, context)
        _require_dynamodb(resource)
        return inspect_table(self.ports.commands, self.spec, resource, context)

    def _inspect_cognito(self, resource: Resource, context: RequestContext) -> Ownership:
        if isinstance(resource.resource_id, str) and resource.resource_id.lstrip().startswith("{"):
            return inspect_cognito_child(self.ports.commands, self.spec, resource, context)
        expected_id = f"{self.spec.region}_{self.spec.run_id}"
        expected_name = f"cnesdata-cap-{self.spec.run_id}"
        if resource.resource_id == expected_id and resource.name != expected_name:
            raise ServiceFailure("service=resource_invalid")
        return inspect_pool(self.ports.commands, self.spec, resource, context)

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
        if service == "cognito":
            if not _cognito_enabled(self.spec):
                raise ServiceFailure("service=not_implemented")
            return create_pool(self.ports.commands, self.spec, creation, context)
        if service == "s3" and self.spec.probe in S3_PROFILES:
            return create_bucket(self.ports.commands, self.spec, creation, context)
        if service == "iam" and self.spec.probe in IAM_ROLE_PROFILES:
            return create_role(self.ports, self.spec, creation, context)
        if service != "dynamodb":
            raise ServiceFailure("service=not_implemented")
        return create_table(self.ports.commands, self.spec, creation, context)

    def probe(
        self, resources: Sequence[Resource], spec: RunSpec, context: RequestContext
    ) -> Mapping[str, object]:
        if self.spec.probe == "cognito-resource-server":
            return self._probe_server(resources, spec, context)
        if self.spec.probe in {"cognito-login", "cognito-pkce-negatives", "cognito-sdk"}:
            if not _cognito_enabled(self.spec):
                raise ServiceFailure("service=not_implemented")
            if spec != self.spec or len(resources) != 1:
                raise ServiceFailure("probe=cognito_prepare_invalid")
            prepare_pool(self.ports.commands, self.spec, resources[0], context)
        if self.spec.probe in S3_PROFILES:
            if spec != self.spec:
                raise ServiceFailure("probe=s3_invalid")
            return probe_s3(resources, spec, self.ports, context)
        if self.spec.probe == "ddb-throughput":
            return probe_throughput(resources, spec, self.ports, context)
        if self.spec.probe not in ("ddb-transactions", "ddb-conditions", "ddb-ttl-gsi"):
            raise ServiceFailure("service=not_implemented")
        if spec != self.spec:
            raise ServiceFailure("probe=ddb_prepare_invalid")
        return probe_dynamodb(resources, spec, self.ports, context)

    def create_child(self, parent: Resource, kind: str, context: RequestContext) -> Resource:
        if kind != "cognito.server":
            raise ServiceFailure("service=cognito_creation_invalid")
        return create_server(self.ports.commands, self.spec, parent, context)

    def lookup_server_exact(self, pending: Resource, context: RequestContext) -> ServerLookup:
        return lookup_server_exact(self.ports.commands, self.spec, pending, context)

    def _probe_server(
        self, resources: Sequence[Resource], spec: RunSpec, context: RequestContext
    ) -> Mapping[str, object]:
        if spec != self.spec or context.lane != "work" or len(resources) != 2:
            raise ServiceFailure("probe=cognito_server_invalid")
        root, child = resources
        expected = cognito_child(
            root, spec, "cognito.server", {"identifier": "https://api.cnesdata.com.br"}
        )
        if root.service != "cognito" or child != expected:
            raise ServiceFailure("probe=cognito_server_invalid")
        lookup = self.lookup_server_exact(child, context)
        if lookup != "exact_present":
            raise ServiceFailure("probe=cognito_server_invalid")
        return {"resource_server_lookup": lookup}

    def settle_delete(self, resource: Resource, context: RequestContext) -> None:
        if resource.service == "dynamodb":
            await_table_gone(self.ports, self.spec, resource, context)

    def delete(self, resource: Resource, bypass: bool, context: RequestContext) -> None:
        if resource.service == "s3":
            delete_s3(resource, bypass, self, context)
            return
        if resource.service == "iam":
            delete_iam(resource, bypass, self, context)
            return
        if resource.service == "cognito":
            if not _cognito_enabled(self.spec):
                raise ServiceFailure("service=not_implemented")
            if type(bypass) is not bool:
                raise ServiceFailure("service=cognito_delete_invalid")
            if _is_cognito_child(resource):
                delete_server(resource, self, context)
                return
            delete_pool(resource, self, context)
            return
        if resource.service != "dynamodb":
            raise ServiceFailure("service=not_implemented")
        if type(bypass) is not bool:
            raise ServiceFailure("service=ddb_delete_invalid")
        delete_table(self.ports.commands, self.spec, resource, context)


def _require_dynamodb(resource: Resource) -> None:
    if resource.service != "dynamodb":
        raise ServiceFailure("service=service_unavailable")
    if resource.resource_id != resource.name:
        raise ServiceFailure("service=resource_invalid")


def _cognito_enabled(spec: RunSpec) -> bool:
    return spec.allow_governance_bypass and spec.probe in {
        "cognito-resource-server",
        "cognito-login",
        "cognito-pkce-negatives",
        "cognito-sdk",
    }


def _is_s3_child(resource: Resource) -> bool:
    return resource.resource_id != resource.name


def _is_cognito_child(resource: Resource) -> bool:
    return isinstance(resource.resource_id, str) and resource.resource_id.lstrip().startswith("{")
