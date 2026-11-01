"""Criação atômica do role raiz com boundary, trust exato e readback fechado."""

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass

from tests.integration.aws_capability._operator_inputs import OperatorInputs
from tests.integration.aws_capability._safety import root_names, run_name, tags
from tests.integration.aws_capability._service_commands import call_command, require_data
from tests.integration.aws_capability._service_contracts import CommandPort, Reply, ServicePorts
from tests.integration.aws_capability._service_iam import _read_tags
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Creation,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

PATH = "/cnesdata-cap/"
ATTEMPTS = 4
RETRY_SECONDS = 2.0
SESSION_SECONDS = 3600


@dataclass(frozen=True)
class Target:
    name: str
    principal: str
    boundary: str


def trust_document(principal: str) -> dict[str, object]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": principal},
                "Action": "sts:AssumeRole",
            }
        ],
    }


def create_role(
    ports: ServicePorts, spec: RunSpec, creation: Creation, context: RequestContext
) -> Resource:
    """Returns: Identidade do role com path, boundary, trust, sessão e tags confirmados.

    Raises: ServiceFailure para entrada, operador, request ou readback inválidos.
    """
    name = creation.name
    if (
        context.lane != "work"
        or creation.spec != spec
        or name not in root_names(spec, "iam")
        or creation.tags != tags(spec)
    ):
        raise ServiceFailure("service=creation_invalid")
    if ports.operator is None:
        raise ServiceFailure("service=operator_inputs_required")
    _require_operator_caller(ports.commands, spec, ports.operator, context)
    target = Target(name, _principal(spec, ports.operator, name), ports.operator.boundary_arn)
    created = _create_with_retry(ports, spec, target, context)
    resource = Resource("iam", name, name)
    if not _identity_matches(created.get("Role"), spec, name):
        raise ServiceFailure("service=create_invalid")
    if not _confirmed(ports.commands, spec, target, context):
        raise ServiceFailure("service=create_invalid")
    return resource


def _principal(spec: RunSpec, operator: OperatorInputs, name: str) -> str:
    if name == run_name(spec):
        return operator.role_arn
    return f"arn:aws:iam::{spec.account}:role/cnesdata-cap/{run_name(spec)}"


def _require_operator_caller(
    commands: CommandPort, spec: RunSpec, operator: OperatorInputs, context: RequestContext
) -> None:
    data = require_data(call_command(commands, Request("sts", "get-caller-identity", {}), context))
    arn = data.get("Arn")
    expected = (
        rf"arn:aws:sts::{spec.account}:assumed-role/{re.escape(operator.role_name)}/[\w+=,.@-]+"
    )
    if not isinstance(arn, str) or re.fullmatch(expected, arn) is None:
        raise ServiceFailure("service=operator_mismatch")


def _create_with_retry(
    ports: ServicePorts, spec: RunSpec, target: Target, context: RequestContext
) -> Mapping[str, object]:
    request = Request("iam", "create-role", _payload(spec, target))
    for attempt in range(ATTEMPTS):
        reply = call_command(ports.commands, request, context)
        if reply.ok:
            return require_data(reply)
        if not _malformed_policy(reply) or attempt == ATTEMPTS - 1:
            break
        ports.pause(RETRY_SECONDS)
    raise ServiceFailure("service=create_failed")


def _malformed_policy(reply: Reply) -> bool:
    return (
        reply.error is not None
        and reply.error.origin == "service"
        and reply.error.code == "MalformedPolicyDocument"
    )


def _payload(spec: RunSpec, target: Target) -> dict[str, object]:
    document = json.dumps(trust_document(target.principal), sort_keys=True, separators=(",", ":"))
    return {
        "RoleName": target.name,
        "Path": PATH,
        "AssumeRolePolicyDocument": document,
        "PermissionsBoundary": target.boundary,
        "MaxSessionDuration": SESSION_SECONDS,
        "Tags": [{"Key": key, "Value": value} for key, value in tags(spec).items()],
    }


def _identity_matches(role: object, spec: RunSpec, name: str) -> bool:
    return (
        isinstance(role, Mapping)
        and role.get("RoleName") == name
        and role.get("Path") == PATH
        and role.get("Arn") == f"arn:aws:iam::{spec.account}:role/cnesdata-cap/{name}"
    )


def _confirmed(
    commands: CommandPort, spec: RunSpec, target: Target, context: RequestContext
) -> bool:
    resource = Resource("iam", target.name, target.name)
    request = Request("iam", "get-role", {"RoleName": resource.resource_id})
    role = require_data(call_command(commands, request, context)).get("Role")
    found = _read_tags(commands, resource, context)
    return (
        _identity_matches(role, spec, resource.resource_id)
        and found == tags(spec)
        and _configuration_matches(role, target)
    )


def _configuration_matches(role: object, target: Target) -> bool:
    if not isinstance(role, Mapping):
        return False
    boundary = role.get("PermissionsBoundary")
    duration = role.get("MaxSessionDuration")
    return (
        isinstance(boundary, Mapping)
        and boundary.get("PermissionsBoundaryType") == "Policy"
        and boundary.get("PermissionsBoundaryArn") == target.boundary
        and type(duration) is int
        and duration == SESSION_SECONDS
        and role.get("AssumeRolePolicyDocument") == trust_document(target.principal)
    )
