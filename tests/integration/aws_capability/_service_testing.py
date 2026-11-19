"""Gravador independente da fronteira de processo para os contratos B1."""

import json
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tests.integration.aws_capability._cli_testing import capture_input
from tests.integration.aws_capability.cli import Cli, Request
from tests.integration.aws_capability.contracts import Limits, RequestBudget, RequestContext

CANARIES = ("B1_PASSWORD_CANARY", "B1_TOKEN_CANARY", "B1_STDERR_CANARY")
BODY = Path(__file__).parent / "fixtures" / "body.json"
REMOTE_TRANSACTION = {
    "Code": "TransactionCanceledException",
    "Message": CANARIES[2],
    "CancellationReasons": [
        {"Code": "None"},
        {
            "Code": "ConditionalCheckFailed",
            "Message": CANARIES[0],
            "Item": {"pk": {"S": CANARIES[1]}},
        },
        {"Code": "None"},
    ],
}


@dataclass
class ProcessScript:
    outcomes: list[tuple[int, str, str] | BaseException] = field(default_factory=list)
    calls: list[tuple[list[str], dict[str, Any]]] = field(default_factory=list)
    observed_debits: list[int] = field(default_factory=list)
    budget: RequestBudget | None = None

    def run(self, argv: Sequence[str], **options: Any) -> subprocess.CompletedProcess[str]:
        recorded_argv, recorded = capture_input(argv, options)
        self.calls.append((recorded_argv, recorded))
        if self.budget is not None:
            self.observed_debits.append(self.budget.used)
        assert self.outcomes, "recorder=unexpected_request"
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        returncode, stdout, stderr = outcome
        return subprocess.CompletedProcess(list(argv), returncode, stdout, stderr)


@dataclass
class CliLab:
    cli: Cli
    process: ProcessScript
    directory: Path

    def respond(self, outcome: tuple[int, str, str] | BaseException) -> None:
        self.process.outcomes.append(outcome)

    def call(self, request: Request) -> Any:
        method = getattr(self.cli, "call", None)
        assert callable(method), "interface=Cli.call_absent"
        return method(request)


def make_cli_lab(directory: Path) -> CliLab:
    budget = RequestBudget(Limits(), 1060, lambda: 1000)
    environment = {
        "AWS_ACCESS_KEY_ID": "dummy",
        "AWS_SECRET_ACCESS_KEY": "dummy",
        "AWS_SESSION_TOKEN": "dummy",
        "AWS_PROFILE": "unsafe-profile",
        "AWS_DEFAULT_PROFILE": "unsafe-default",
        "AWS_ENDPOINT_URL": "https://endpoint.invalid",
        "AWS_ENDPOINT_URL_S3": "https://endpoint.invalid",
        "AWS_CLI_ERROR_FORMAT": "legacy",
        "AWS_MAX_ATTEMPTS": "99",
    }
    cli = Cli("us-east-2", 7, environment, RequestContext(budget, "work"))
    return CliLab(cli, ProcessScript(budget=budget), directory)


def stdin_payload(process: ProcessScript) -> Mapping[str, object]:
    return json.loads(process.calls[-1][1]["input"])


def private_auth_request() -> Request:
    return Request(
        "cognito-idp",
        "admin-initiate-auth",
        {
            "UserPoolId": "us-east-2_synthetic",
            "ClientId": "syntheticclient",
            "AuthFlow": "ADMIN_USER_PASSWORD_AUTH",
            "AuthParameters": {"USERNAME": "synthetic", "PASSWORD": CANARIES[0]},
        },
    )
