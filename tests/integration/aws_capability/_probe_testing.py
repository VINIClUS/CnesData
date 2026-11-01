"""Laboratório de probes S3: serviços reais sobre a fronteira de comandos com estado."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tests.integration.aws_capability._aws_fake_testing import FakeAws
from tests.integration.aws_capability._aws_fake_types_testing import FakeBucket, FakeRole
from tests.integration.aws_capability._b1_testing import (
    ACCOUNT,
    BOUNDARY_ARN,
    NAME,
    OPERATOR_ARN,
    TAGS,
    TRUST,
    TRUST2,
)
from tests.integration.aws_capability._operator_inputs import OperatorInputs
from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability._service_contracts import ServicePorts
from tests.integration.aws_capability._testing import ProcessBoundary
from tests.integration.aws_capability._worker import acknowledgement
from tests.integration.aws_capability.contracts import (
    Policy,
    Ports,
    RequestBudget,
    RequestContext,
    Resource,
    RunSpec,
)
from tests.integration.aws_capability.services import AwsServices

BUCKET = Resource("s3", NAME, NAME)
ROLE = Resource("iam", NAME, NAME)
ROLE2 = Resource("iam", f"{NAME}-r2", f"{NAME}-r2")
START = 1000.0
DEADLINE = 1180.0


@dataclass
class ProbeLab:
    aws: FakeAws
    services: AwsServices
    budget: RequestBudget
    spec: RunSpec
    pauses: list[float]
    clock: list[float]

    @property
    def context(self) -> RequestContext:
        return RequestContext(self.budget, "work")

    @property
    def resources(self) -> list[Resource]:
        if self.spec.probe == "iam-trust":
            return [ROLE, ROLE2]
        return [BUCKET, ROLE] if self.spec.probe == "s3-retention" else [BUCKET]


def make_probe_lab(
    tmp_path: Path, probe: str, *, seeded: bool = True, **options: Any
) -> ProbeLab:
    clock = [START]
    pauses: list[float] = []

    def advance(seconds: float) -> None:
        clock[0] += seconds

    def pause(seconds: float) -> None:
        pauses.append(seconds)
        advance(seconds)

    aws = FakeAws(lambda: clock[0], advance, **options)
    if seeded:
        if probe != "iam-trust":
            aws.buckets[NAME] = FakeBucket(dict(TAGS))
        if probe in {"s3-retention", "iam-trust"}:
            aws.roles[NAME] = FakeRole(dict(TAGS), TRUST)
        if probe == "iam-trust":
            aws.roles[ROLE2.name] = FakeRole(dict(TAGS), TRUST2)
    spec = RunSpec(
        ACCOUNT, "us-east-2", "a1b2c3d4", DEADLINE, tmp_path, allow_governance_bypass=True,
        probe=probe,
    )
    limits = PROFILES[probe].limits
    budget = RequestBudget(limits, DEADLINE, lambda: clock[0])
    operator = OperatorInputs(OPERATOR_ARN, BOUNDARY_ARN)
    ports = ServicePorts(aws, lambda: clock[0], pause, aws.session, operator)
    return ProbeLab(aws, AwsServices(spec, ports), budget, spec, pauses, clock)


def payload_text(lab: ProbeLab) -> str:
    return " ".join(repr((r.service, r.operation, dict(r.payload))) for r in lab.aws.requests)


def janitor_ports(lab: ProbeLab, *, supervised: bool = False) -> Ports:
    limits = PROFILES[lab.spec.probe].limits
    policy = Policy(ACCOUNT, frozenset({"us-east-2"}), limits)
    processes = ProcessBoundary([])
    if supervised:
        processes.ack = acknowledgement(lab.spec, os.getpid())
    return Ports(lab.services, processes, lambda: lab.clock[0], policy, lab.budget)
