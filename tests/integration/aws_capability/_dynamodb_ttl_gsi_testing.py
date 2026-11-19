"""Lab offline com fronteira pública e requests literais para TTL e GSI."""

import inspect
import json
import logging
import subprocess
import time
from collections.abc import Iterator, Sequence
from copy import deepcopy
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import pytest

from tests.integration.aws_capability._cli_testing import capture_input
from tests.integration.aws_capability._service_ddb_prepare_testing import Privacy, _data
from tests.integration.aws_capability._service_read_assertions import (
    FIXED_FLAGS,
    assert_environment,
)
from tests.integration.aws_capability._service_read_testing import ReadLab, make_read_lab
from tests.integration.aws_capability.cli import Cli
from tests.integration.aws_capability.contracts import Limits, RequestContext, ServiceFailure


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def equal(actual: Any, expected: Any) -> None:
    assert canonical(actual) == canonical(expected)


def fixture_steps() -> list[Any]:
    directory = Path(__file__).parent / "fixtures" / "cli" / "b4"
    return [
        step
        for name in ("ttl", "gsi")
        for step in json.loads((directory / f"ddb_{name}.json").read_text())["steps"]
    ]


def outcome(reply: Any) -> Any:
    if "timeout" in reply:
        return subprocess.TimeoutExpired("aws", 7, output=reply["timeout"], stderr=reply["timeout"])
    return (
        reply["exit"],
        reply.get("raw", json.dumps(reply.get("data", {}))),
        json.dumps(reply["error"]) if "error" in reply else "",
    )


def queue_ttl_gsi_tail(read: ReadLab) -> list[Any]:
    read.services = replace(
        read.services, ports=replace(read.services.ports, now=lambda: 2000000000)
    )
    steps = fixture_steps()
    for step in steps:
        read.process.outcomes.append(outcome(step["reply"]))
    return [tuple(step["request"]) for step in steps]


@dataclass(repr=False)
class TtlGsiLab:
    read: ReadLab
    privacy: Privacy
    original: RequestContext
    current: float = 2000000000
    anchor_calls: int = 0
    advance_after: int = 0
    start: int = 0
    sleeps: list[float] = field(default_factory=list)
    expected: list[Any] = field(default_factory=list, repr=False)
    queued: list[Any] = field(default_factory=list, repr=False)
    timeouts: list[float] = field(default_factory=list)

    def now(self) -> float:
        return self.current

    def anchor(self) -> float:
        self.anchor_calls += 1
        return 2000000000

    def sleep(self, seconds: float) -> None:
        assert 0 < seconds <= 1
        assert seconds == min(1.0, self.original.budget.deadline - self.current)
        self.sleeps.append(seconds)
        self.current += seconds


def assert_call(call: Any, expected: Any, timeout: float) -> None:
    argv, options = call
    service, operation, region, payload = expected
    seconds = str(max(1, int(timeout)))
    assert argv == [
        "aws",
        service,
        operation,
        "--region",
        region,
        *FIXED_FLAGS,
        seconds,
        "--cli-read-timeout",
        seconds,
    ]
    equal(json.loads(options["input"]), payload)
    equal(options["timeout"], timeout)
    assert options["shell"] is False
    assert options["check"] is False
    assert options["capture_output"] is True
    assert options["text"] is True
    assert set(options) == {"input", "timeout", "shell", "check", "capture_output", "text", "env"}
    assert_environment(options["env"], region)


def make_lab(monkeypatch: pytest.MonkeyPatch, path: Path, privacy: Privacy) -> TtlGsiLab:
    read = make_read_lab(monkeypatch, path)
    lab = TtlGsiLab(read, privacy, read.context)
    spec = replace(read.services.spec, probe="ddb-ttl-gsi", deadline=2000000060)
    ports = replace(read.services.ports, now=lab.anchor)
    read.services = replace(read.services, spec=spec, ports=ports)
    read.context.budget.now = lab.now
    read.context.budget.deadline = 2000000060
    read.context.budget.limits = Limits(256, 8, 2, 60, 128)

    def run(argv: Sequence[str], **options: Any) -> subprocess.CompletedProcess[str]:
        frame = inspect.currentframe()
        assert frame is not None
        assert frame.f_back is not None
        cli = frame.f_back.f_locals["self"]
        assert isinstance(cli, Cli)
        assert cli.context is lab.original
        assert cli.context.budget is lab.original.budget is read.process.budget
        assert cli.context.lane == "work"
        index = len(read.process.calls)
        assert index < len(lab.expected), "boundary=unexpected_call"
        timeout = min(7, lab.original.budget.deadline - lab.current)
        lab.timeouts.append(timeout)
        assert_call(capture_input(argv, options), lab.expected[index], timeout)
        try:
            return read.process.run(argv, **options)
        finally:
            if lab.advance_after == index + 1:
                lab.current = lab.original.budget.deadline

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(time, "sleep", lab.sleep)
    return lab


def queue(lab: TtlGsiLab, steps: list[Any]) -> None:
    prep = _data()
    tags = prep["tags"]
    next(tag for tag in tags["Tags"] if tag["Key"] == "ExpiresAt")["Value"] = "2000000060"
    for data in (prep["active"], tags, prep["ttl"]["enabled"]):
        lab.read.process.outcomes.append((0, json.dumps(data), ""))
    lab.expected = [prep["requests"][name] for name in ("describe", "tags", "ttl")]
    for step in steps:
        lab.expected.append(step["request"])
        lab.read.process.outcomes.append(outcome(step["reply"]))
    lab.queued = list(lab.read.process.outcomes)


def trace(lab: TtlGsiLab, count: int, sleeps: list[float]) -> None:
    actual = len(lab.read.process.calls)
    for index, call in enumerate(lab.read.process.calls):
        assert_call(call, lab.expected[index], lab.timeouts[index])
    budget = lab.original.budget
    assert lab.read.context is lab.original
    assert budget is lab.read.context.budget is lab.read.process.budget
    assert lab.original.lane == "work"
    assert budget.used == budget.work_used == lab.start + actual
    assert lab.read.process.observed_debits == list(range(lab.start + 1, budget.used + 1))
    assert budget.cleanup_deadline is None
    assert budget.deadline == lab.read.services.spec.deadline == 2000000060
    assert budget.limits == Limits(256, 8, 2, 60, 128)
    assert 256 - budget.used >= 128
    assert lab.read.process.outcomes == lab.queued[actual:]
    assert_progress((actual, lab.sleeps), (count, sleeps))


def assert_progress(observed: Any, expected: Any) -> None:
    assert observed[0] == expected[0], (
        f"oracle=call_count calls={observed[0]} expected={expected[0]}"
    )
    assert canonical(observed[1]) == canonical(expected[1]), "oracle=sleeps"


def assert_failure(error: BaseException, reason: str) -> None:
    from tests.integration.aws_capability.contracts import BudgetExhausted

    expected_type = BudgetExhausted if reason.startswith("budget=") else ServiceFailure
    assert type(error) is expected_type, "oracle=failure_type"
    assert error.args == (reason,), "oracle=failure_reason"


def records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.getMessage().startswith("probe=ddb_ttl_gsi ")]


def assert_records(caplog: pytest.LogCaptureFixture, expected: list[Any]) -> None:
    assert_observations(caplog.records, expected)
    for secret in ("cap#a1b2c3d4", "111111111111", "cnesdata-cap-", "arn:", "CANARY"):
        assert secret not in caplog.text
    assert "PASS" not in caplog.text
    assert "succeeded" not in caplog.text


def assert_observations(observed: Sequence[logging.LogRecord], expected: list[Any]) -> None:
    assert len(observed) == len(expected), "oracle=record_count"
    for index, (record, (template, args)) in enumerate(zip(observed, expected, strict=True)):
        assert record.levelno == logging.INFO
        assert record.msg == template
        assert isinstance(record.args, tuple)
        assert canonical(record.args) == canonical(args), f"oracle=record_args index={index}"


def literal_records(safe: list[Any]) -> list[logging.LogRecord]:
    return [
        logging.LogRecord("oracle", logging.INFO, "synthetic", 1, template, args, None)
        for template, args in safe
    ]


def _reject_control(action: Any, marker: str) -> str:
    with pytest.raises(AssertionError, match=marker):
        action()
    return marker


def acceptance_controls(safe: list[Any]) -> list[Any]:
    expected = deepcopy(safe)
    expected[3] = (safe[3][0], ("observed", 4, 8))
    expected[4] = (safe[4][0], ("observed", 4, 8))
    reason = "probe=not_implemented"
    assert_progress((34, [1.0] * 6), (34, [1.0] * 6))
    assert_failure(ServiceFailure(reason), reason)
    assert_observations(literal_records(expected), expected)
    evidence: list[Any] = [{"control": "valid", "accepted": True}]
    for name, index, args, count, sleeps in (
        ("early_inclusion", 3, ("observed", 1, 2), 28, 3),
        ("stale_removal", 4, ("observed", 1, 2), 28, 3),
        ("fifth_poll", 3, ("observed", 5, 10), 36, 7),
    ):
        observed = deepcopy(expected)
        observed[index] = (expected[index][0], args)
        count_marker = _reject_control(
            lambda count=count, sleeps=sleeps: assert_progress(
                (count, [1.0] * sleeps), (34, [1.0] * 6)
            ),
            "oracle=call_count",
        )
        record_marker = _reject_control(
            lambda observed=observed: assert_observations(literal_records(observed), expected),
            f"oracle=record_args index={index}",
        )
        markers = [count_marker, record_marker]
        if name == "fifth_poll":
            markers.append(
                _reject_control(
                    lambda: assert_progress((34, [1.0] * 7), (34, [1.0] * 6)), "oracle=sleeps"
                )
            )
        evidence.append(
            {
                "control": name,
                "calls": count,
                "record_args": args,
                "sleeps": sleeps,
                "rejected": markers,
            }
        )
    marker = _reject_control(
        lambda: assert_failure(ServiceFailure(reason), "probe=ddb_gsi_rejection_invalid"),
        "oracle=failure_reason",
    )
    missing = _reject_control(
        lambda: assert_observations(literal_records(expected), expected[:-1]), "oracle=record_count"
    )
    evidence.append({"control": "local252_as_remote254", "rejected": [marker, missing]})
    return evidence


@pytest.fixture
def ttl_lab(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> TtlGsiLab:
    caplog.set_level(logging.INFO)
    return make_lab(monkeypatch, tmp_path, Privacy(caplog, capsys))


@pytest.fixture(autouse=True)
def private_surface(ttl_lab: TtlGsiLab) -> Iterator[None]:
    yield
    ttl_lab.privacy.check(ServiceFailure("control=privacy"), ttl_lab.read)
    assert list(ttl_lab.read.directory.rglob("*")) == []
