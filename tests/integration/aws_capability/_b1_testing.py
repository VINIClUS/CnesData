"""Constantes e entradas compartilhadas pelos testes S3 e IAM do PR-B1."""

import json
from collections.abc import Mapping
from dataclasses import replace
from typing import Protocol

from tests.integration.aws_capability._operator_inputs import OperatorInputs
from tests.integration.aws_capability._service_contracts import CommandPort, ServicePorts, no_pause
from tests.integration.aws_capability._service_read_testing import ReadLab
from tests.integration.aws_capability.contracts import Creation, RequestContext
from tests.integration.aws_capability.services import AwsCommands, AwsServices

ACCOUNT = "111111111111"
OPERATOR_ARN = f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/cnesdata-cap-operator-0a1b2c3d"
BOUNDARY_ARN = f"arn:aws:iam::{ACCOUNT}:policy/cnesdata-cap/cnesdata-cap-boundary-0a1b2c3d"
OPERATOR_KEY = "CAP_OPERATOR_ROLE_ARN"
BOUNDARY_KEY = "CAP_PERMISSIONS_BOUNDARY_ARN"


def operator_environment() -> Mapping[str, str]:
    return {OPERATOR_KEY: OPERATOR_ARN, BOUNDARY_KEY: BOUNDARY_ARN}


NAME = "cnesdata-cap-a1b2c3d4"
RUN = "a1b2c3d4"
TAGS = {"Project": "CnesData", "Purpose": "aws-capability", "Run": RUN, "ExpiresAt": "1060"}
TAG_SET = [{"Key": key, "Value": value} for key, value in TAGS.items()]
SESSION = {
    "AWS_ACCESS_KEY_ID": "SESSION_KEY_CANARY",
    "AWS_SECRET_ACCESS_KEY": "SESSION_SECRET_CANARY",
    "AWS_SESSION_TOKEN": "SESSION_TOKEN_CANARY",
}
TRUST = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Principal": {"AWS": OPERATOR_ARN}, "Action": "sts:AssumeRole"}
    ],
}
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/{NAME}"
TRUST2 = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"AWS": ROLE_ARN}, "Action": "sts:AssumeRole"}],
}


def b1_lab(base: ReadLab, probe: str, pauses: list[float] | None = None) -> ReadLab:
    spec = replace(base.services.spec, probe=probe, allow_governance_bypass=True)
    commands = base.services.ports.commands
    assert isinstance(commands, AwsCommands)

    def session(credentials: Mapping[str, str]) -> CommandPort:
        return AwsCommands(spec.region, {**commands.environment, **credentials}, 7)

    ports = ServicePorts(
        commands,
        base.services.ports.now,
        pauses.append if pauses is not None else no_pause,
        session,
        OperatorInputs(OPERATOR_ARN, BOUNDARY_ARN),
    )
    return replace(base, services=AwsServices(spec, ports))


def fail(lab: ReadLab, code: str, status: int = 254) -> None:
    lab.process.outcomes.append((status, "", json.dumps({"Code": code, "Message": "x"})))


def ok(lab: ReadLab, data: object = None) -> None:
    lab.process.outcomes.append((0, "" if data is None else json.dumps(data), ""))


def creation_for(lab: ReadLab) -> Creation:
    return Creation(lab.services.spec, NAME, dict(TAGS))


class HasContext(Protocol):
    @property
    def context(self) -> RequestContext: ...


def cleanup_context(lab: HasContext) -> RequestContext:
    return RequestContext(lab.context.budget, "cleanup")
