"""Fronteira de comandos com estado mínimo de S3, IAM e STS para probes e ciclo completo."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from tests.integration.aws_capability._aws_fake_ddb_testing import DdbHandlers, FakeTable
from tests.integration.aws_capability._aws_fake_iam_testing import IamHandlers
from tests.integration.aws_capability._aws_fake_s3_testing import S3Handlers
from tests.integration.aws_capability._aws_fake_types_testing import (
    ACCESS_KEY,
    OPERATOR,
    SECRET,
    TOKEN,
    FakeBucket,
    FakeRole,
    denied,
    good,
)
from tests.integration.aws_capability._b1_testing import ACCOUNT
from tests.integration.aws_capability._service_contracts import Reply
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import RequestContext

ROLE_SESSION = "role"


@dataclass
class FakeAws(S3Handlers, IamHandlers, DdbHandlers):
    clock: Callable[[], float]
    advance: Callable[[float], None]
    latency: float = 1.0
    policy_lag: int = 0
    assume_denials: int = 0
    malformed_creates: int = 0
    buckets: dict[str, FakeBucket] = field(default_factory=dict)
    roles: dict[str, FakeRole] = field(default_factory=dict)
    tables: dict[str, FakeTable] = field(default_factory=dict)
    calls: list[tuple[str, str, str]] = field(default_factory=list)
    lanes: list[str] = field(default_factory=list)
    requests: list[Request] = field(default_factory=list)
    counter: int = 0
    assumes: int = 0

    def call(self, request: Request, context: RequestContext) -> Reply:
        return self.dispatch(request, context, OPERATOR)

    def session(self, credentials: Mapping[str, str]) -> "FakeSession":
        assert credentials["AWS_ACCESS_KEY_ID"] == ACCESS_KEY
        assert credentials["AWS_SECRET_ACCESS_KEY"] == SECRET
        assert credentials["AWS_SESSION_TOKEN"] == TOKEN
        return FakeSession(self, ROLE_SESSION)

    def dispatch(self, request: Request, context: RequestContext, who: str) -> Reply:
        context.budget.acquire(context.lane)
        self.advance(self.latency)
        self.counter += 1
        self.calls.append((request.service, request.operation, who))
        self.lanes.append(context.lane)
        self.requests.append(request)
        name = f"{request.service}_{request.operation}".replace("-", "_")
        handler = getattr(self, f"_{name}")
        return handler(dict(request.payload), who)

    def allowed(self, who: str, action: str) -> bool:
        if who == OPERATOR:
            return True
        role = next(iter(self.roles.values()))
        return any(
            action in actions and self.counter >= effective
            for actions, effective, _ in role.policies.values()
        )

    def operations(self, who: str | None = None) -> list[str]:
        return [op for _, op, actor in self.calls if who is None or actor == who]

    def _sts_get_caller_identity(self, payload: dict, who: str) -> Reply:
        arn = f"arn:aws:sts::{ACCOUNT}:assumed-role/cnesdata-cap-operator-0a1b2c3d/session"
        return good({"Account": ACCOUNT, "Arn": arn, "UserId": "synthetic"})

    def _sts_assume_role(self, payload: dict, who: str) -> Reply:
        self.assumes += 1
        role = self.roles.get(str(payload["RoleArn"]).rsplit("/", 1)[1])
        if role is None or self.assumes <= self.assume_denials:
            return denied()
        assert payload["DurationSeconds"] == 900
        credentials = {"AccessKeyId": ACCESS_KEY, "SecretAccessKey": SECRET, "SessionToken": TOKEN}
        return good({"Credentials": {**credentials, "Expiration": "2099-01-01T00:00:00Z"}})

    def _cognito_idp_list_user_pools(self, payload: dict, who: str) -> Reply:
        return good({"UserPools": []})



@dataclass
class FakeSession:
    aws: FakeAws
    who: str

    def call(self, request: Request, context: RequestContext) -> Reply:
        return self.aws.dispatch(request, context, self.who)
