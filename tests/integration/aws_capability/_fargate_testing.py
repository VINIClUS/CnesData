"""Laboratório do perfil fargate-write: fronteira com estado, imagem falsa e semeadura."""

import hashlib
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tests.integration.aws_capability._aws_fake_fargate_testing import (
    DEFAULT_VPC,
    TLS_EGRESS,
    TOKEN_CANARY,
    FakeCluster,
    FakeGroup,
    FakeLogGroup,
    FakeRepo,
)
from tests.integration.aws_capability._aws_fake_testing import FakeAws
from tests.integration.aws_capability._aws_fake_types_testing import FakeRole
from tests.integration.aws_capability._b1_testing import (
    ACCOUNT,
    BOUNDARY_ARN,
    NAME,
    OPERATOR_ARN,
    TRUST,
)
from tests.integration.aws_capability._operator_inputs import OperatorInputs
from tests.integration.aws_capability._probe_testing import START, ProbeLab
from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability._safety import tags
from tests.integration.aws_capability._service_contracts import RegistryLogin, ServicePorts
from tests.integration.aws_capability.contracts import (
    RequestBudget,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)
from tests.integration.aws_capability.services import AwsServices

PROBE = "fargate-write"
IMAGE = "cnesdata-cap-processor:current-develop"
SG_ID = "sg-0cap000000000001"
OTHER_RUN = "cnesdata-cap-ffffffff"
LATENCY = 1.3
ECR = Resource("ecr", NAME, NAME)
LOGS = Resource("logs", NAME, NAME)
EXEC = Resource("iam", f"{NAME}-exec", f"{NAME}-exec")
TASK = Resource("iam", f"{NAME}-task", f"{NAME}-task")
GROUP = Resource("ec2", SG_ID, NAME)
CLUSTER = Resource("ecs", NAME, NAME)
ROOTS = [ECR, LOGS, EXEC, TASK, GROUP, CLUSTER]


@dataclass
class FakeImages:
    aws: FakeAws
    advance: Callable[[float], None]
    seconds: float = 90.0
    digest_override: str | None = None
    pushes: list[tuple[str, str]] = field(default_factory=list)
    logins: list[RegistryLogin] = field(default_factory=list)

    def push(
        self, image: str, target: str, login: RegistryLogin, context: RequestContext
    ) -> str:
        self.logins.append(login)
        self.pushes.append((image, target))
        if login.token != TOKEN_CANARY:
            raise ServiceFailure("push=login_invalid")
        uri, _, tag = target.rpartition(":")
        repo = self.aws.fargate.repos[uri.rsplit("/", 1)[1]]
        self.advance(self.seconds)
        if repo.mutability == "IMMUTABLE" and tag in repo.images:
            raise ServiceFailure("push=tag_immutable")
        digest = "sha256:" + hashlib.sha256(image.encode()).hexdigest()
        repo.images[tag] = digest
        return self.digest_override or digest


@dataclass
class FargateLab(ProbeLab):
    images: FakeImages

    @property
    def resources(self) -> list[Resource]:
        return list(ROOTS)

    @property
    def state(self) -> Any:
        return self.aws.fargate


def make_fargate_lab(
    tmp_path: Path, *, seeded: bool = True, image: str | None = IMAGE, **options: Any
) -> FargateLab:
    clock = [START]
    pauses: list[float] = []

    def advance(seconds: float) -> None:
        clock[0] += seconds

    def pause(seconds: float) -> None:
        pauses.append(seconds)
        advance(seconds)

    options.setdefault("latency", LATENCY)
    aws = FakeAws(lambda: clock[0], advance, **options)
    deadline = START + PROFILES[PROBE].window
    spec = RunSpec(
        ACCOUNT, "us-east-2", "a1b2c3d4", deadline, tmp_path, allow_governance_bypass=True,
        probe=PROBE,
    )
    if seeded:
        seed_fargate(aws, spec)
    budget = RequestBudget(PROFILES[PROBE].limits, deadline, lambda: clock[0])
    images = FakeImages(aws, advance)
    operator = OperatorInputs(OPERATOR_ARN, BOUNDARY_ARN)
    ports = ServicePorts(
        aws, lambda: clock[0], pause, aws.session, operator, images=images, image=image
    )
    return FargateLab(aws, AwsServices(spec, ports), budget, spec, pauses, clock, images)


def seed_fargate(aws: FakeAws, spec: RunSpec, name: str = NAME) -> None:
    owned = tags(spec)
    state = aws.fargate
    state.repos[name] = FakeRepo(dict(owned), "IMMUTABLE")
    state.log_groups[name] = FakeLogGroup(dict(owned), 1)
    state.clusters[name] = FakeCluster(dict(owned))
    state.groups[SG_ID] = FakeGroup(name, DEFAULT_VPC, dict(owned), [], [dict(TLS_EGRESS)])
    state.counter = 1
    for suffix in ("exec", "task"):
        aws.roles[f"{name}-{suffix}"] = FakeRole(dict(owned), TRUST)


def seed_foreign(aws: FakeAws, spec: RunSpec, run_name: str = OTHER_RUN) -> None:
    other = {**tags(spec), "Run": run_name.rsplit("-", 1)[1]}
    state = aws.fargate
    state.repos[run_name] = FakeRepo(dict(other), "IMMUTABLE")
    state.log_groups[run_name] = FakeLogGroup(dict(other), 1)
    state.clusters[run_name] = FakeCluster(dict(other))
    state.groups["sg-0foreign0000001"] = FakeGroup(run_name, DEFAULT_VPC, dict(other))
    aws.roles[f"{run_name}-exec"] = FakeRole(dict(other), TRUST)


def operations(lab: FargateLab, lane: str | None = None) -> list[str]:
    return [
        f"{r.service}.{r.operation}"
        for r, seen in zip(lab.aws.requests, lab.aws.lanes, strict=True)
        if lane is None or seen == lane
    ]


def first(lab: FargateLab, operation: str, lane: str | None = None) -> int:
    return operations(lab, lane).index(operation)


def last(lab: FargateLab, operation: str, lane: str | None = None) -> int:
    found = operations(lab, lane)
    return len(found) - 1 - found[::-1].index(operation)


def requests_of(lab: FargateLab, operation: str) -> list[dict]:
    service, _, name = operation.partition(".")
    return [
        dict(r.payload) for r in lab.aws.requests
        if (r.service, r.operation) == (service, name)
    ]


def empty_state(lab: FargateLab) -> dict[str, int]:
    state, aws = lab.state, lab.aws
    active = [r for r in state.revisions if r.status in {"ACTIVE", "INACTIVE"}]
    clusters = [c for c in state.clusters.values() if c.status == "ACTIVE"]
    return {
        "repos": len(state.repos),
        "log_groups": len(state.log_groups),
        "clusters": len(clusters),
        "revisions": len(active),
        "groups": len(state.groups),
        "roles": len(aws.roles),
    }


def account_free(text: str) -> bool:
    return ACCOUNT not in text and "arn:" not in text
