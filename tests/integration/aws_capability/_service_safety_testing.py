"""Cenários privados isolados para os dois findings de utilidade B1."""

import importlib.util
import json
import shutil
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from tests.integration.aws_capability._service_testing import (
    CANARIES,
    REMOTE_TRANSACTION,
    CliLab,
    make_cli_lab,
    private_auth_request,
)
from tests.integration.aws_capability.cli import Request


@dataclass
class BodyScenario:
    lab: CliLab
    body: Path
    failure: type[Exception]
    request_type: Any


def isolated_body_scenario(directory: Path) -> BodyScenario:
    source = Path(__file__).parent
    copied = directory / "isolated"
    shutil.copytree(source, copied, ignore=shutil.ignore_patterns("__pycache__"))
    name = "_b1_isolated_cli_" + uuid4().hex
    spec = importlib.util.spec_from_file_location(name, copied / "cli.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        del sys.modules[name]
    private = directory / "private"
    private.mkdir()
    lab = make_cli_lab(private)
    cli = module.Cli("us-east-2", 7, lab.cli.environment, lab.cli.context)
    return BodyScenario(
        CliLab(cli, lab.process, private),
        copied / "fixtures/body.json",
        module.CliFailure,
        module.Request,
    )


@contextmanager
def substituted_body(body: Path, kind: str, private: Path) -> Iterator[None]:
    original = body.read_bytes()
    body.unlink()
    if kind in {"private_symlink", "journal_symlink"}:
        body.symlink_to(private)
    elif kind == "directory":
        body.mkdir()
    else:
        contents = {"altered": b"[]", "canary": CANARIES[1].encode(), "newline": b"{}\n"}
        body.write_bytes(contents[kind])
    try:
        yield
    finally:
        if body.is_symlink() or body.is_file():
            body.unlink()
        else:
            body.rmdir()
        body.write_bytes(original)


def call_failure_outcome(failure: str) -> tuple[int, str, str] | BaseException:
    outcomes: dict[str, Any] = {
        "service": (254, CANARIES[1], json.dumps(REMOTE_TRANSACTION)),
        "local": (
            252,
            CANARIES[1],
            json.dumps({"Code": "ParamValidation", "Message": CANARIES[2]}),
        ),
        "timeout": subprocess.TimeoutExpired("aws", 1, CANARIES[1], CANARIES[2]),
        "os": OSError(CANARIES[2]),
        "invalid_json": (0, CANARIES[1], CANARIES[2]),
    }
    return outcomes[failure]


def private_failure_request(failure: str) -> Request:
    if failure != "service":
        return private_auth_request()
    return Request(
        "dynamodb",
        "transact-write-items",
        {
            "TransactItems": [
                {
                    "Put": {
                        "TableName": "synthetic",
                        "Item": {"pk": {"S": "synthetic"}, "secret": {"S": CANARIES[0]}},
                    }
                }
            ],
        },
    )
