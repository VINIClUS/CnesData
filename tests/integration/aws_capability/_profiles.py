"""Perfis fechados de serviços e limites do ensaio efêmero."""

from dataclasses import asdict, dataclass

from tests.integration.aws_capability.contracts import Limits


@dataclass(frozen=True)
class Profile:
    services: tuple[str, ...]
    limits: Limits
    version: int = 1

    def snapshot(self, probe: str) -> dict[str, object]:
        return {
            "probe": probe,
            "profile_version": self.version,
            "limits": asdict(self.limits),
        }


PROFILES = {
    "all": Profile(("cognito", "dynamodb", "s3", "iam"), Limits()),
    "cognito-login": Profile(("cognito",), Limits(512, 8, 2, 90, 256)),
    "cognito-pkce-negatives": Profile(("cognito",), Limits(512, 8, 2, 90, 256)),
    "cognito-sdk": Profile(("cognito",), Limits(256, 8, 2, 90, 128)),
    "ddb-transactions": Profile(("dynamodb",), Limits(192, 8, 2, 60, 96)),
    "ddb-conditions": Profile(("dynamodb",), Limits(256, 8, 2, 60, 128)),
    "ddb-ttl-gsi": Profile(("dynamodb",), Limits(256, 8, 2, 60, 128)),
    "s3-retention": Profile(("s3", "iam"), Limits(384, 8, 2, 90, 192)),
    "s3-hold": Profile(("s3",), Limits(256, 8, 2, 90, 128)),
    "iam-trust": Profile(("iam",), Limits(256, 8, 2, 60, 128)),
    "iam-leadingkeys": Profile(("dynamodb", "iam"), Limits(512, 8, 2, 90, 256)),
    "iam-s3": Profile(("s3", "iam"), Limits(384, 8, 2, 90, 192)),
}
