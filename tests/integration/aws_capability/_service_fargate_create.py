"""Criação das raízes ECR, Logs, ECS e EC2 do fargate-write com readback de posse."""

from collections.abc import Mapping
from typing import TYPE_CHECKING

from tests.integration.aws_capability._safety import owned, root_names, tags
from tests.integration.aws_capability._service_commands import require_data
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability._service_fargate import (
    call,
    describe_group,
    inspect_leaf,
    rows_of,
    text_of,
)
from tests.integration.aws_capability.contracts import (
    Creation,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

if TYPE_CHECKING:
    from tests.integration.aws_capability.services import AwsServices

TLS_EGRESS = {
    "IpProtocol": "tcp",
    "FromPort": 443,
    "ToPort": 443,
    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
}
ALL_EGRESS = {"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}
DESCRIPTION = "cnesdata capability probe"
RETENTION_DAYS = 1


def create_leaf(
    service: str, creation: Creation, services: "AwsServices", context: RequestContext
) -> Resource:
    """Returns: recurso criado e confirmado; Raises: ServiceFailure para entrada ou readback."""
    commands, spec = services.ports.commands, services.spec
    if (
        context.lane != "work"
        or creation.spec != spec
        or creation.name not in root_names(spec, service)
        or creation.tags != tags(spec)
    ):
        raise ServiceFailure("service=creation_invalid")
    resource = _CREATORS[service](commands, spec, creation.name, context)
    if service == "ec2":
        _confirm_group(commands, spec, resource, context)
    else:
        owner = inspect_leaf(commands, spec, resource, context)
        if not owned(resource, owner, spec):
            raise ServiceFailure("service=create_invalid")
    return resource


def _upper_tags(spec: RunSpec) -> list[dict[str, str]]:
    return [{"Key": key, "Value": value} for key, value in tags(spec).items()]


def _create_ecr(
    commands: CommandPort, spec: RunSpec, name: str, context: RequestContext
) -> Resource:
    payload = {
        "repositoryName": name,
        "imageTagMutability": "IMMUTABLE",
        "tags": _upper_tags(spec),
    }
    require_data(call(commands, "ecr.create-repository", payload, context))
    return Resource("ecr", name, name)


def _create_logs(
    commands: CommandPort, spec: RunSpec, name: str, context: RequestContext
) -> Resource:
    payload = {"logGroupName": name, "tags": tags(spec)}
    require_data(call(commands, "logs.create-log-group", payload, context))
    retention = {"logGroupName": name, "retentionInDays": RETENTION_DAYS}
    require_data(call(commands, "logs.put-retention-policy", retention, context))
    return Resource("logs", name, name)


def _create_ecs(
    commands: CommandPort, spec: RunSpec, name: str, context: RequestContext
) -> Resource:
    labels = [{"key": key, "value": value} for key, value in tags(spec).items()]
    payload = {"clusterName": name, "tags": labels}
    require_data(call(commands, "ecs.create-cluster", payload, context))
    return Resource("ecs", name, name)


def default_vpc(commands: CommandPort, context: RequestContext) -> str:
    payload = {"Filters": [{"Name": "isDefault", "Values": ["true"]}]}
    data = require_data(call(commands, "ec2.describe-vpcs", payload, context))
    rows = rows_of(data, "Vpcs")
    if len(rows) != 1:
        raise ServiceFailure("service=default_vpc_invalid")
    return text_of(rows[0], "VpcId")


def _create_ec2(
    commands: CommandPort, spec: RunSpec, name: str, context: RequestContext
) -> Resource:
    payload = {
        "GroupName": name,
        "Description": DESCRIPTION,
        "VpcId": default_vpc(commands, context),
        "TagSpecifications": [{"ResourceType": "security-group", "Tags": _upper_tags(spec)}],
    }
    created = require_data(call(commands, "ec2.create-security-group", payload, context))
    group = text_of(created, "GroupId")
    resource = Resource("ec2", group, name)
    allow = {"GroupId": group, "IpPermissions": [TLS_EGRESS]}
    require_data(call(commands, "ec2.authorize-security-group-egress", allow, context))
    drop = {"GroupId": group, "IpPermissions": [ALL_EGRESS]}
    require_data(call(commands, "ec2.revoke-security-group-egress", drop, context))
    return resource


def _confirm_group(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> None:
    row = describe_group(commands, resource, context)
    if row is None or row.get("IpPermissions") != []:
        raise ServiceFailure("service=create_invalid")
    if not _only_tls(row.get("IpPermissionsEgress")):
        raise ServiceFailure("service=create_invalid")
    found = {
        item.get("Key"): item.get("Value")
        for item in rows_of(row, "Tags")
        if isinstance(item, Mapping)
    }
    if found != tags(spec) or row.get("OwnerId") != spec.account:
        raise ServiceFailure("service=create_invalid")


def _only_tls(rules: object) -> bool:
    if not isinstance(rules, list) or len(rules) != 1 or not isinstance(rules[0], Mapping):
        return False
    rule = rules[0]
    ranges = [item.get("CidrIp") for item in rule.get("IpRanges", [])]
    extras = ("Ipv6Ranges", "PrefixListIds", "UserIdGroupPairs")
    return (
        (rule.get("IpProtocol"), rule.get("FromPort"), rule.get("ToPort")) == ("tcp", 443, 443)
        and ranges == ["0.0.0.0/0"]
        and not any(rule.get(key) for key in extras)
    )


_CREATORS = {
    "ecr": _create_ecr,
    "logs": _create_logs,
    "ecs": _create_ecs,
    "ec2": _create_ec2,
}
