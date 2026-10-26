"""Lab de leitura Cognito com fixtures R2 isoladas."""

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from tests.integration.aws_capability._service_contracts import ServicePorts
from tests.integration.aws_capability._service_read_testing import CANARY, ReadLab
from tests.integration.aws_capability._service_testing import ProcessScript
from tests.integration.aws_capability.contracts import (
    Limits,
    RequestBudget,
    RequestContext,
    Resource,
    RunSpec,
)
from tests.integration.aws_capability.services import AwsCommands, AwsServices

ROOT = Resource("cognito", "us-east-2_a1b2c3d4", "cnesdata-cap-a1b2c3d4")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for key, value in pairs:
        if key in data:
            raise ValueError(f"duplicate_key={key}")
        data[key] = value
    return data


def load_fixture(path: Path) -> Any:
    return json.loads(path.read_text(), object_pairs_hook=_reject_duplicate_keys)


def fixture_data(name: str) -> Any:
    path = Path(__file__).parent / "fixtures" / "cli" / "r2" / f"{name}.json"
    return load_fixture(path)


def respond(lab: ReadLab, data: object) -> None:
    lab.process.outcomes.append((0, json.dumps(data), ""))


def respond_error(lab: ReadLab, name: str) -> None:
    code, stdout, stderr = fixture_data("errors")[name]
    lab.process.outcomes.append((code, stdout, stderr))


def cursor_input(name: str = "valid") -> str:
    return json.dumps(fixture_data("cursors")[name], sort_keys=True, separators=(",", ":"))


def make_read_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ReadLab:
    budget = RequestBudget(Limits(), 1060, lambda: 1000)
    context = RequestContext(budget, "work")
    environment = {
        "AWS_ACCESS_KEY_ID": "dummy",
        "AWS_SECRET_ACCESS_KEY": "dummy",
        "AWS_SESSION_TOKEN": CANARY,
        "AWS_PROFILE": "unsafe-profile",
        "AWS_DEFAULT_PROFILE": "unsafe-default",
        "AWS_ENDPOINT_URL": "https://endpoint.invalid",
        "AWS_ENDPOINT_URL_COGNITO_IDP": "https://cognito.invalid",
        "AWS_ENDPOINT_URL_STS": "https://sts.invalid",
        "AWS_REGION": "us-west-1",
        "AWS_DEFAULT_REGION": "us-west-1",
        "AWS_CONFIG_FILE": "/synthetic/host/config",
        "AWS_SHARED_CREDENTIALS_FILE": "/synthetic/host/credentials",
        "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "false",
        "AWS_PAGER": "unsafe-pager",
        "AWS_CLI_AUTO_PROMPT": "on",
        "AWS_MAX_ATTEMPTS": "99",
        "AWS_EC2_METADATA_DISABLED": "false",
    }
    commands = AwsCommands("us-east-2", environment, 7)
    spec = RunSpec("111111111111", "us-east-2", "a1b2c3d4", 1060, tmp_path)
    services = AwsServices(spec, ServicePorts(commands, budget.now))
    process = ProcessScript(budget=budget)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(subprocess, "run", process.run)
    return ReadLab(services, process, context, tmp_path)
