"""Criação atômica do bucket raiz com Object Lock, tags e readback fechado."""

from collections.abc import Mapping

from tests.integration.aws_capability._safety import run_name, tags
from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability._service_s3 import lookup_bucket
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Creation,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def create_bucket(
    commands: CommandPort, spec: RunSpec, creation: Creation, context: RequestContext
) -> Resource:
    """Returns: Identidade do bucket com lock, versionamento e tags confirmados.

    Raises: ServiceFailure para entrada, request ou readback inválidos.
    """
    name = run_name(spec)
    if (
        context.lane != "work"
        or creation.spec != spec
        or creation.name != name
        or creation.tags != tags(spec)
    ):
        raise ServiceFailure("service=creation_invalid")
    require_data(call_command(commands, _create_request(spec, name), context))
    require_data(call_command(commands, _tagging_request(spec, name), context))
    resource = Resource("s3", name, name)
    if not _confirmed(commands, spec, resource, context):
        raise ServiceFailure("service=create_invalid")
    return resource


def _create_request(spec: RunSpec, name: str) -> Request:
    payload: dict[str, object] = {
        "Bucket": name,
        "CreateBucketConfiguration": {"LocationConstraint": spec.region},
        "ObjectLockEnabledForBucket": True,
    }
    return Request("s3api", "create-bucket", payload)


def _tagging_request(spec: RunSpec, name: str) -> Request:
    tag_set = [{"Key": key, "Value": value} for key, value in tags(spec).items()]
    payload: dict[str, object] = {
        "Bucket": name,
        "ExpectedBucketOwner": spec.account,
        "Tagging": {"TagSet": tag_set},
    }
    return Request("s3api", "put-bucket-tagging", payload)


def _confirmed(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> bool:
    owner = lookup_bucket(commands, spec, resource, context)
    versioning = _read(commands, spec, resource, "get-bucket-versioning", context)
    lock = _read(commands, spec, resource, "get-object-lock-configuration", context)
    configuration = lock.get("ObjectLockConfiguration")
    return (
        owner is not None
        and owner.account == spec.account
        and owner.region == spec.region
        and dict(owner.tags) == tags(spec)
        and versioning.get("Status") == "Enabled"
        and isinstance(configuration, Mapping)
        and configuration.get("ObjectLockEnabled") == "Enabled"
        and "Rule" not in configuration
    )


def _read(
    commands: CommandPort,
    spec: RunSpec,
    resource: Resource,
    operation: str,
    context: RequestContext,
) -> Mapping[str, object]:
    payload: dict[str, object] = {
        "Bucket": resource.resource_id,
        "ExpectedBucketOwner": spec.account,
    }
    return require_data(call_command(commands, Request("s3api", operation, payload), context))
