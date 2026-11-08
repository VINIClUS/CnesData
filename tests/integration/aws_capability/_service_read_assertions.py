"""Asserts literais da fronteira CLI e classificação de feature ausente."""

import json
import os
import traceback
from collections.abc import Mapping, Sequence

from tests.integration.aws_capability._cli_testing import PRIVATE_INPUT
from tests.integration.aws_capability._service_read_testing import CANARY, ReadLab
from tests.integration.aws_capability.contracts import ServiceFailure

ExpectedCall = tuple[str, str, str, Mapping[str, object]]
FIXED_FLAGS = [
    "--cli-input-json",
    PRIVATE_INPUT,
    "--output",
    "json",
    "--cli-error-format",
    "json",
    "--color",
    "off",
    "--no-cli-pager",
    "--no-cli-auto-prompt",
    "--no-paginate",
    "--cli-connect-timeout",
]


def assert_rejected(error: ServiceFailure) -> None:
    assert error.args != ("service=not_implemented",), "interface=feature_absent"


def assert_environment(environment: Mapping[str, str], region: str) -> None:
    assert environment == {
        "AWS_ACCESS_KEY_ID": "dummy",
        "AWS_SECRET_ACCESS_KEY": "dummy",
        "AWS_SESSION_TOKEN": CANARY,
        "AWS_REGION": region,
        "AWS_DEFAULT_REGION": region,
        "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "true",
        "AWS_PAGER": "",
        "AWS_CLI_AUTO_PROMPT": "off",
        "AWS_MAX_ATTEMPTS": "1",
        "AWS_CONFIG_FILE": os.devnull,
        "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
        "AWS_EC2_METADATA_DISABLED": "true",
    }


def assert_private(error: BaseException, lab: ReadLab, captured: str) -> None:
    public = "".join(traceback.format_exception(error)) + repr(error) + captured
    public += repr(lab.services) + repr(lab.services.ports) + repr(lab.services.ports.commands)
    for path in lab.directory.rglob("*"):
        public += str(path.relative_to(lab.directory))
        if path.is_file() and not path.is_symlink():
            public += path.read_text(errors="replace")
    assert CANARY not in public


def assert_process(lab: ReadLab, expected: Sequence[ExpectedCall], timeout: float = 7) -> None:
    assert len(lab.process.calls) == len(expected)
    for (argv, options), (service, operation, region, payload) in zip(
        lab.process.calls, expected, strict=True
    ):
        assert argv == [
            "aws",
            service,
            operation,
            "--region",
            region,
            *FIXED_FLAGS,
            str(int(timeout)),
            "--cli-read-timeout",
            str(int(timeout)),
        ]
        actual = json.loads(options["input"])
        assert actual == payload
        assert {key: type(value) for key, value in actual.items()} == {
            key: type(value) for key, value in payload.items()
        }
        assert options["timeout"] == timeout
        assert options["shell"] is False
        assert options["capture_output"] is True
        assert options["text"] is True
        assert options["check"] is False
        assert_environment(options["env"], region)
    assert lab.context.budget.used == len(expected)
    assert lab.process.observed_debits == list(range(1, len(expected) + 1))
    assert lab.process.outcomes == []
