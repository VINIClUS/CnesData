"""Leituras frescas de buckets S3, região e tags."""

from collections.abc import Mapping

from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_contracts import CommandPort, Reply
from tests.integration.aws_capability._service_cursor import next_s3_cursor, parse_s3_cursor
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Ownership,
    Page,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def list_buckets(
    commands: CommandPort, spec: RunSpec, value: str | None, context: RequestContext
) -> Page:
    """Returns:
        Página de buckets e continuação opaca.
    Raises:
        ServiceFailure: cursor ou resposta inválida.
    """
    cursor = parse_s3_cursor(value, spec)
    payload: dict[str, object] = {"MaxBuckets": 1000}
    if cursor.marker is not None:
        payload["ContinuationToken"] = cursor.marker
    data = require_data(call_command(commands, Request("s3api", "list-buckets", payload), context))
    buckets = data.get("Buckets")
    if not isinstance(buckets, list):
        raise ServiceFailure("service=inventory_invalid")
    resources = tuple(_bucket_resource(bucket) for bucket in buckets)
    token = _next_token(data)
    return Page(resources, next_s3_cursor(token, cursor.seen, spec.run_id))


def lookup_bucket(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership | None:
    """Returns:
        Ownership fresco ou None para NoSuchBucket remoto em location.
    Raises:
        ServiceFailure: recurso inválido ou leitura inconclusiva.
    """
    _require_root(resource)
    payload = {"Bucket": resource.resource_id, "ExpectedBucketOwner": spec.account}
    reply = call_command(commands, Request("s3api", "get-bucket-location", payload), context)
    if not reply.ok:
        if _remote_code(reply, "NoSuchBucket"):
            return None
        raise ServiceFailure("service=describe_failed")
    region = _location(require_data(reply))
    tags = _read_tags(commands, spec, resource, context)
    return Ownership(spec.account, tags, region)


def inspect_bucket(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership:
    """Returns:
        Ownership fresco de bucket presente.
    Raises:
        ServiceFailure: ausência conclusiva ou leitura inconclusiva.
    """
    owner = lookup_bucket(commands, spec, resource, context)
    if owner is None:
        raise ServiceFailure("service=not_found")
    return owner


def _bucket_resource(bucket: object) -> Resource:
    if not isinstance(bucket, Mapping):
        raise ServiceFailure("service=inventory_invalid")
    name = bucket.get("Name")
    if not isinstance(name, str) or not name:
        raise ServiceFailure("service=inventory_invalid")
    return Resource("s3", name, name)


def _next_token(data: Mapping[str, object]) -> str | None:
    if "ContinuationToken" not in data:
        return None
    token = data["ContinuationToken"]
    if not isinstance(token, str) or not token:
        raise ServiceFailure("service=inventory_invalid")
    return token


def _require_root(resource: Resource) -> None:
    if (
        resource.service != "s3"
        or not isinstance(resource.resource_id, str)
        or not resource.resource_id
        or not isinstance(resource.name, str)
        or not resource.name
        or resource.resource_id != resource.name
    ):
        raise ServiceFailure("service=resource_invalid")


def _remote_code(reply: Reply, code: str) -> bool:
    return (
        not reply.ok
        and reply.error is not None
        and reply.error.origin == "service"
        and reply.error.code == code
    )


def _location(data: Mapping[str, object]) -> str:
    if "LocationConstraint" not in data:
        raise ServiceFailure("service=describe_invalid")
    region = data["LocationConstraint"]
    if region is None:
        return "us-east-1"
    if not isinstance(region, str) or not region:
        raise ServiceFailure("service=describe_invalid")
    return "eu-west-1" if region == "EU" else region


def _read_tags(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> dict[str, str]:
    payload = {"Bucket": resource.resource_id, "ExpectedBucketOwner": spec.account}
    reply = call_command(commands, Request("s3api", "get-bucket-tagging", payload), context)
    if _remote_code(reply, "NoSuchTagSet"):
        return {}
    data = require_data(reply)
    if set(data) != {"TagSet"} or not isinstance(data["TagSet"], list):
        raise ServiceFailure("service=tags_invalid")
    tags: dict[str, str] = {}
    for item in data["TagSet"]:
        if not isinstance(item, Mapping) or set(item) != {"Key", "Value"}:
            raise ServiceFailure("service=tags_invalid")
        key, value = item["Key"], item["Value"]
        if not isinstance(key, str) or not isinstance(value, str) or key in tags:
            raise ServiceFailure("service=tags_invalid")
        tags[key] = value
    return tags
