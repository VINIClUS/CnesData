"""STS da fronteira com estado: trust por principal, permissão de identidade e limites."""

from collections.abc import Callable
from fnmatch import fnmatchcase

from tests.integration.aws_capability._aws_fake_types_testing import (
    ACCESS_KEY,
    OPERATOR,
    SECRET,
    TOKEN,
    FakeRole,
    denied,
    good,
)
from tests.integration.aws_capability._b1_testing import ACCOUNT, NAME, OPERATOR_ARN
from tests.integration.aws_capability._service_contracts import Reply

MAX_CHAINED_SECONDS = 3600


def who_for(name: str) -> str:
    suffix = name.removeprefix(NAME).removeprefix("-")
    return "role" if not suffix else f"role-{suffix}"


def name_for(who: str) -> str:
    return NAME if who == "role" else f"{NAME}-{who.removeprefix('role-')}"


def role_arn(name: str) -> str:
    return f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/{name}"


def token_for(who: str) -> str:
    return TOKEN if who == "role" else f"{TOKEN}_{who}"


def who_of_token(token: str) -> str:
    assert token.startswith(TOKEN)
    return "role" if token == TOKEN else token.removeprefix(TOKEN + "_")


def _listed(value: object) -> list[str]:
    return [value] if isinstance(value, str) else [str(item) for item in value]  # type: ignore[attr-defined]


class StsHandlers:
    roles: dict[str, FakeRole]
    counter: int
    assumes: int
    assume_denials: int
    duration_message: str
    allowed: Callable[[str, str], bool]

    def trust_actions(self, target: FakeRole, principal: str) -> set[str]:
        document = target.trust_at(self.counter)
        actions: set[str] = set()
        for statement in document["Statement"]:  # type: ignore[index]
            if statement["Effect"] == "Allow" and principal in _listed(
                statement["Principal"]["AWS"]
            ):
                actions.update(_listed(statement["Action"]))
        return actions

    def covers(self, who: str, arn: str) -> bool:
        role = self.roles.get(name_for(who))
        if role is None:
            return False
        for actions, effective, document in role.policies.values():
            if "sts:AssumeRole" not in actions or self.counter < effective:
                continue
            for statement in document["Statement"]:  # type: ignore[index]
                if any(fnmatchcase(arn, pattern) for pattern in _listed(statement["Resource"])):
                    return True
        return False

    def _sts_assume_role(self, payload: dict, who: str) -> Reply:
        self.assumes += 1
        arn = str(payload["RoleArn"])
        target = self.roles.get(arn.rsplit("/", 1)[1])
        if target is None or self.assumes <= self.assume_denials:
            return denied()
        if payload["DurationSeconds"] > MAX_CHAINED_SECONDS:
            return denied("ValidationError", self.duration_message)
        principal = OPERATOR_ARN if who == OPERATOR else role_arn(name_for(who))
        actions = self.trust_actions(target, principal)
        permitted = who == OPERATOR or (
            self.allowed(who, "sts:AssumeRole") and self.covers(who, arn)
        )
        required = {"sts:AssumeRole"}
        required |= {"sts:TagSession"} if "Tags" in payload else set()
        required |= {"sts:SetSourceIdentity"} if "SourceIdentity" in payload else set()
        if not permitted or not required <= actions:
            return denied()
        credentials = {
            "AccessKeyId": ACCESS_KEY,
            "SecretAccessKey": SECRET,
            "SessionToken": token_for(who_for(arn.rsplit("/", 1)[1])),
        }
        return good({"Credentials": {**credentials, "Expiration": "2099-01-01T00:00:00Z"}})
