"""Fixtures de roots reutilizam o lab real e o canary R1."""

import json
import logging
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from tests.integration.aws_capability._service_read_assertions import ExpectedCall, assert_private
from tests.integration.aws_capability._service_read_testing import ReadLab as BaseReadLab
from tests.integration.aws_capability._service_read_testing import (
    make_read_lab as make_base_read_lab,
)
from tests.integration.aws_capability.contracts import Resource, ServiceFailure

__all__ = [
    "LIST",
    "LOCATION",
    "NEXT",
    "ROOT",
    "TAGS",
    "ReadLab",
    "TeardownProbe",
    "cursor_input",
    "fixture_data",
    "load_fixture",
    "make_read_lab",
    "respond",
    "respond_error",
    "teardown_probe_source",
]

ROOT = Resource("s3", "cnesdata-cap-a1b2c3d4", "cnesdata-cap-a1b2c3d4")
LIST: ExpectedCall = ("s3api", "list-buckets", "us-east-2", {"MaxBuckets": 1000})
NEXT: ExpectedCall = (
    "s3api",
    "list-buckets",
    "us-east-2",
    {"MaxBuckets": 1000, "ContinuationToken": "bucket-next"},
)
LOCATION: ExpectedCall = (
    "s3api",
    "get-bucket-location",
    "us-east-2",
    {"Bucket": "cnesdata-cap-a1b2c3d4", "ExpectedBucketOwner": "111111111111"},
)
TAGS: ExpectedCall = (
    "s3api",
    "get-bucket-tagging",
    "us-east-2",
    {"Bucket": "cnesdata-cap-a1b2c3d4", "ExpectedBucketOwner": "111111111111"},
)


@dataclass
class CapturedChannels:
    caplog: pytest.LogCaptureFixture
    capsys: pytest.CaptureFixture[str]


@dataclass
class ReadLab(BaseReadLab):
    channels: CapturedChannels = field(repr=False)

    def check_private(self, observed: object = None) -> None:
        captured = self.channels.capsys.readouterr()
        public = self.channels.caplog.text + captured.out + captured.err
        records = (
            self.channels.caplog.get_records("setup")
            + self.channels.caplog.get_records("call")
            + self.channels.caplog.get_records("teardown")
        )
        formatter = logging.Formatter()
        public += "".join(formatter.format(record) for record in records)
        error = (
            observed if isinstance(observed, BaseException) else ServiceFailure("service=read_ok")
        )
        if not isinstance(observed, BaseException):
            public += repr(observed)
        assert_private(error, self, public)


def make_read_lab(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> ReadLab:
    caplog.set_level(logging.DEBUG)
    base = make_base_read_lab(monkeypatch, tmp_path)
    return ReadLab(
        base.services, base.process, base.context, base.directory, CapturedChannels(caplog, capsys)
    )


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
    path = Path(__file__).parent / "fixtures" / "cli" / "r3" / f"{name}.json"
    return load_fixture(path)


def respond(lab: ReadLab, data: object) -> None:
    lab.process.outcomes.append((0, json.dumps(data), ""))


def respond_error(lab: ReadLab, name: str) -> None:
    code, stdout, stderr = fixture_data("errors")[name]
    lab.process.outcomes.append((code, stdout, stderr))


def cursor_input(name: str = "valid") -> str:
    return json.dumps(fixture_data("cursors")[name], sort_keys=True, separators=(",", ":"))


@dataclass(eq=False)
class TeardownProbe:
    records: list[dict[str, Any]] = field(default_factory=list)

    def pytest_runtest_makereport(self, item: Any, call: Any) -> None:
        if call.when not in {"call", "teardown"}:
            return
        frame = None
        if call.excinfo is not None:
            last = traceback.extract_tb(call.excinfo.tb)[-1]
            frame = (Path(last.filename).name, last.name)
        lab = item.funcargs["probe_lab"]
        self.records.append(
            {
                "phase": call.when,
                "failed": call.excinfo is not None,
                "last_frame": frame,
                "old_method_survived": item.funcargs["previous_observation"]["old_survived"],
                "physical_calls": len(lab.process.calls),
                "budget_used": lab.context.budget.used,
                "debits": list(lab.process.observed_debits),
            }
        )


def teardown_probe_source(fixture_name: str, channel: str) -> str:
    return _TEARDOWN_PROBE.replace("FIXTURE_NAME", fixture_name).replace("CHANNEL", channel)


_TEARDOWN_PROBE = """import logging
import sys
import pytest
from tests.integration.aws_capability.conftest import offline
from tests.integration.aws_capability.test_service_s3_inventory import read_lab as inventory_lab
from tests.integration.aws_capability.test_service_s3_ownership import read_lab as ownership_lab
from tests.integration.aws_capability.test_service_s3_lookup import read_lab as lookup_lab
from tests.integration.aws_capability._service_roots_testing import LOCATION, respond_error
from tests.integration.aws_capability._service_read_testing import CANARY
from tests.integration.aws_capability._service_read_assertions import assert_private, assert_process
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import ServiceFailure

@pytest.fixture
def probe_lab(request):
    return request.getfixturevalue("FIXTURE_NAME")

@pytest.fixture(autouse=True)
def previous_observation(probe_lab):
    state = {"old_survived": None}
    yield state
    if "CHANNEL" in {"debug", "info", "warning"}:
        records = probe_lab.channels.caplog.get_records("call")
        assert any(CANARY in record.getMessage() for record in records)
        assert CANARY not in probe_lab.channels.caplog.text
        assert_private(ServiceFailure("service=read_ok"), probe_lab,
                       probe_lab.channels.caplog.text)
        state["old_survived"] = True

def test_observacao(probe_lab):
    respond_error(probe_lab, "remote_no_bucket")
    reply = probe_lab.services.ports.commands.call(
        Request("s3api", "get-bucket-location", {
            "Bucket": "cnesdata-cap-a1b2c3d4", "ExpectedBucketOwner": "111111111111"
        }), probe_lab.context)
    assert reply.error.origin == "service" and reply.error.code == "NoSuchBucket"
    message = reply.error.details["Message"]
    assert message == CANARY
    assert_process(probe_lab, [LOCATION])
    probe_lab.check_private(None)
    match "CHANNEL":
        case "stdout":
            sys.stdout.write(message)
        case "stderr":
            sys.stderr.write(message)
        case "debug" | "info" | "warning":
            logging.getLogger("r3.teardown").log(
                {"debug": logging.DEBUG, "info": logging.INFO,
                 "warning": logging.WARNING}["CHANNEL"], message)
            assert message in probe_lab.channels.caplog.text
        case "file":
            (probe_lab.directory / "teardown-leak.txt").write_text(message)
    assert_process(probe_lab, [LOCATION])
"""
