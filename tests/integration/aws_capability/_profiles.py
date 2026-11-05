"""Perfis fechados de serviços e limites do ensaio efêmero."""

import re
from dataclasses import asdict, dataclass

from tests.integration.aws_capability.contracts import Limits

_SUFFIX = re.compile(r"[a-z0-9]{1,8}")


@dataclass(frozen=True)
class Root:
    service: str
    suffix: str = ""

    def __post_init__(self) -> None:
        if self.suffix and _SUFFIX.fullmatch(self.suffix) is None:
            raise ValueError("root=invalid_suffix")


@dataclass(frozen=True)
class Profile:
    services: tuple[str, ...]
    limits: Limits
    version: int = 1
    children: tuple[str, ...] = ()
    roots: tuple[Root, ...] = ()
    window: float = 180.0

    def planned_roots(self) -> tuple[Root, ...]:
        return self.roots or tuple(Root(service) for service in self.services)

    def snapshot(self, probe: str) -> dict[str, object]:
        return {
            "probe": probe,
            "profile_version": self.version,
            "limits": asdict(self.limits),
        }


PROFILES = {
    "cognito-resource-server": Profile(
        ("cognito",), Limits(512, 8, 2, 240, 256), children=("cognito.server",)
    ),
    "all": Profile(("cognito", "dynamodb", "s3", "iam"), Limits()),
    "cognito-login": Profile(("cognito",), Limits(512, 8, 2, 360, 256), window=1100.0),
    "ddb-transactions": Profile(("dynamodb",), Limits(192, 8, 2, 150, 96)),
    "ddb-conditions": Profile(("dynamodb",), Limits(256, 8, 2, 150, 128)),
    "ddb-ttl-gsi": Profile(("dynamodb",), Limits(256, 8, 2, 150, 128)),
    "ddb-throughput": Profile(("dynamodb",), Limits(128, 8, 2, 150, 64), window=140.0),
    "s3-retention": Profile(("s3", "iam"), Limits(384, 8, 2, 240, 192)),
    "s3-hold": Profile(("s3",), Limits(256, 8, 2, 120, 128)),
    "iam-trust": Profile(
        ("iam",), Limits(176, 8, 2, 150, 104), roots=(Root("iam"), Root("iam", "r2"))
    ),
    "iam-leadingkeys": Profile(
        ("dynamodb", "iam"),
        Limits(270, 8, 2, 285, 150),
        roots=(Root("dynamodb"), Root("iam", "audit"), Root("iam", "api")),
        window=330.0,
    ),
    "iam-s3": Profile(
        ("s3", "iam"),
        Limits(550, 8, 2, 630, 380),
        roots=(
            Root("s3"), Root("iam", "audit"), Root("iam", "unit"), Root("iam", "recovery"),
            Root("iam", "api"),
        ),
        window=425.0,
    ),
    "fargate-write": Profile(
        ("iam", "ecr", "ecs", "logs", "ec2"),
        Limits(300, 8, 2, 480, 160),
        roots=(
            Root("ecr"), Root("logs"), Root("iam", "exec"), Root("iam", "task"),
            Root("ec2"), Root("ecs"),
        ),
        window=480.0,
    ),
}
