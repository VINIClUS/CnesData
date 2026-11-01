"""Exclusão de versões, markers e bucket com hold desligado e bypass explícito."""

from typing import TYPE_CHECKING

from tests.integration.aws_capability._resource_ids import parse_s3_child
from tests.integration.aws_capability._safety import owned, run_name
from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_polling import await_absent
from tests.integration.aws_capability._service_s3 import lookup_bucket
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import RequestContext, Resource, ServiceFailure

if TYPE_CHECKING:
    from tests.integration.aws_capability.services import AwsServices


def delete_s3(
    resource: Resource, bypass: bool, services: "AwsServices", context: RequestContext
) -> None:
    """Raises: ServiceFailure para entrada, posse ou resposta inválida."""
    if context.lane != "cleanup" or type(bypass) is not bool:
        raise ServiceFailure("service=s3_delete_invalid")
    if resource.resource_id == resource.name:
        _delete_root(resource, services, context)
        return
    root, kind, child = parse_s3_child(resource, services.spec)
    if kind not in {"s3.version", "s3.marker"}:
        raise ServiceFailure("service=s3_delete_invalid")
    _require_owned_root(root, services, context)
    base = _object(services, root, child)
    if kind == "s3.version":
        _release_hold(services, base, context)
    if bypass:
        base = {**base, "BypassGovernanceRetention": True}
    request = Request("s3api", "delete-object", base)
    require_data(call_command(services.ports.commands, request, context))


def _delete_root(root: Resource, services: "AwsServices", context: RequestContext) -> None:
    _require_owned_root(root, services, context)
    owner = {"Bucket": root.resource_id, "ExpectedBucketOwner": services.spec.account}
    request = Request("s3api", "delete-bucket", owner)
    require_data(call_command(services.ports.commands, request, context))
    probe = Request("s3api", "get-bucket-location", owner)
    await_absent(services.ports, probe, "NoSuchBucket", context)


def _require_owned_root(root: Resource, services: "AwsServices", context: RequestContext) -> None:
    spec = services.spec
    if root.resource_id != run_name(spec) or root.name != root.resource_id:
        raise ServiceFailure("service=s3_delete_invalid")
    owner = lookup_bucket(services.ports.commands, spec, root, context)
    if owner is None:
        raise ServiceFailure("service=not_found")
    if not owned(root, owner, spec):
        raise ServiceFailure("service=not_owned")


def _object(services: "AwsServices", root: Resource, child: dict[str, str]) -> dict[str, object]:
    return {
        "Bucket": root.resource_id,
        "Key": child["key"],
        "VersionId": child["version_id"],
        "ExpectedBucketOwner": services.spec.account,
    }


def _release_hold(
    services: "AwsServices", base: dict[str, object], context: RequestContext
) -> None:
    commands = services.ports.commands
    reply = call_command(commands, Request("s3api", "get-object-legal-hold", base), context)
    if not reply.ok:
        if reply.error is not None and reply.error.code == "NoSuchObjectLockConfiguration":
            return
        raise ServiceFailure("service=delete_failed")
    hold = reply.data.get("LegalHold")
    status = hold.get("Status") if isinstance(hold, dict) else None
    if status == "OFF":
        return
    if status != "ON":
        raise ServiceFailure("service=delete_failed")
    release = {**base, "LegalHold": {"Status": "OFF"}}
    request = Request("s3api", "put-object-legal-hold", release)
    require_data(call_command(commands, request, context))
