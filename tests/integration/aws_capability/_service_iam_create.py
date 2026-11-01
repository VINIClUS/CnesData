"""Criação atômica do role raiz com boundary, trust exato e readback fechado."""

import json
import re
from collections.abc import Mapping

from tests.integration.aws_capability._operator_inputs import OperatorInputs
from tests.integration.aws_capability._safety import run_name, tags
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


def trust_document(operator: OperatorInputs) -> dict[str, object]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": operator.role_arn},
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
    name = run_name(spec)
    if (
        context.lane != "work"
        or creation.spec != spec
        or creation.name != name
        or creation.tags != tags(spec)
    ):
        raise ServiceFailure("service=creation_invalid")
    if ports.operator is None:
        raise ServiceFailure("service=operator_inputs_required")
    _require_operator_caller(ports.commands, spec, ports.operator, context)
    created = _create_with_retry(ports, spec, ports.operator, context)
    resource = Resource("iam", name, name)
    if not _identity_matches(created.get("Role"), spec, name):
        raise ServiceFailure("service=create_invalid")
    if not _confirmed(ports.commands, spec, ports.operator, context):
        raise ServiceFailure("service=create_invalid")
    return resource


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
    ports: ServicePorts, spec: RunSpec, operator: OperatorInputs, context: RequestContext
) -> Mapping[str, object]:
    request = Request("iam", "create-role", _payload(spec, operator))
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


def _payload(spec: RunSpec, operator: OperatorInputs) -> dict[str, object]:
    document = json.dumps(trust_document(operator), sort_keys=True, separators=(",", ":"))
    return {
        "RoleName": run_name(spec),
        "Path": PATH,
        "AssumeRolePolicyDocument": document,
        "PermissionsBoundary": operator.boundary_arn,
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
    commands: CommandPort, spec: RunSpec, operator: OperatorInputs, context: RequestContext
) -> bool:
    resource = Resource("iam", run_name(spec), run_name(spec))
    request = Request("iam", "get-role", {"RoleName": resource.resource_id})
    role = require_data(call_command(commands, request, context)).get("Role")
    found = _read_tags(commands, resource, context)
    return (
        _identity_matches(role, spec, resource.resource_id)
        and found == tags(spec)
        and _configuration_matches(role, operator)
    )


def _configuration_matches(role: object, operator: OperatorInputs) -> bool:
    if not isinstance(role, Mapping):
        return False
    boundary = role.get("PermissionsBoundary")
    duration = role.get("MaxSessionDuration")
    return (
        isinstance(boundary, Mapping)
        and boundary.get("PermissionsBoundaryType") == "Policy"
        and boundary.get("PermissionsBoundaryArn") == operator.boundary_arn
        and type(duration) is int
        and duration == SESSION_SECONDS
        and role.get("AssumeRolePolicyDocument") == trust_document(operator)
    )
