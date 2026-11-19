"""Fronteira offline e trace literal dos testes de transações DynamoDB."""

import inspect
import json
import logging
import subprocess
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, cast

import pytest

from tests.integration.aws_capability._service_ddb_prepare_testing import Privacy, _data
from tests.integration.aws_capability._service_read_assertions import (
    FIXED_FLAGS,
    ExpectedCall,
    assert_environment,
)
from tests.integration.aws_capability._service_read_testing import ReadLab, make_read_lab, respond
from tests.integration.aws_capability.cli import Cli
from tests.integration.aws_capability.contracts import Limits, RequestContext, ServiceFailure


@dataclass
class Clock:
    current: float = 1000
    ticks: bool = False
    start: float = 1000

    def now(self) -> float:
        return self.current


@dataclass
class TransactionLab:
    read: ReadLab
    privacy: Privacy
    original: RequestContext
    original_budget: Any
    clock: Clock = field(default_factory=Clock)
    sleeps: list[float] = field(default_factory=list)
    expected: list[ExpectedCall] = field(default_factory=list, repr=False)
    queued: tuple[Any, ...] = field(default=(), repr=False)


def fixture_steps() -> list[Any]:
    directory = Path(__file__).parent / "fixtures" / "cli" / "b4"
    steps = []
    for name in ("100", "query100", "101", "duplicate"):
        path = directory / f"ddb_transactions_{name}.json"
        steps.extend(json.loads(path.read_text())["steps"])
    return steps


def outcome(reply: Any) -> tuple[int, str, str] | BaseException:
    if "timeout" in reply:
        return subprocess.TimeoutExpired("aws", 7, output=reply["timeout"], stderr=reply["timeout"])
    return (
        reply["exit"],
        json.dumps(reply["data"]) if "data" in reply else "",
        json.dumps(reply["error"]) if "error" in reply else "",
    )


def queue_transactions_tail(lab: ReadLab) -> list[ExpectedCall]:
    expected = []
    for step in fixture_steps():
        expected.append(cast("ExpectedCall", tuple(step["request"])))
        lab.process.outcomes.append(outcome(step["reply"]))
    return expected


def queue(lab: TransactionLab, steps: Sequence[Any] | None = None) -> None:
    prep = _data()
    for data in (prep["active"], prep["tags"], prep["ttl"]["enabled"]):
        respond(lab.read, data)
    requests = prep["requests"]
    lab.expected = [tuple(requests[name]) for name in ("describe", "tags", "ttl")]
    if steps is None:
        lab.expected.extend(queue_transactions_tail(lab.read))
    else:
        for step in steps:
            lab.expected.append(cast("ExpectedCall", tuple(step["request"])))
            lab.read.process.outcomes.append(outcome(step["reply"]))
    lab.queued = tuple(lab.read.process.outcomes)
    lab.clock.start = lab.clock.current


def make_transaction_lab(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, privacy: Privacy
) -> TransactionLab:
    read = make_read_lab(monkeypatch, tmp_path)
    lab = TransactionLab(read, privacy, read.context, read.context.budget)
    read.services = replace(
        read.services, spec=replace(read.services.spec, probe="ddb-transactions")
    )
    read.services = replace(read.services, ports=replace(read.services.ports, now=lab.clock.now))
    read.context.budget.now = lab.clock.now
    read.context.budget.limits = Limits(192, 8, 2, 60, 96)

    def record_sleep(seconds: float) -> None:
        lab.sleeps.append(seconds)

    def run(argv: Sequence[str], **options: Any) -> subprocess.CompletedProcess[str]:
        frame = inspect.currentframe()
        assert frame is not None
        assert frame.f_back is not None
        cli = frame.f_back.f_locals["self"]
        assert isinstance(cli, Cli)
        assert cli.context is lab.original
        assert cli.context.budget is lab.original_budget
        assert cli.context.lane == "work"
        try:
            return read.process.run(argv, **options)
        finally:
            if lab.clock.ticks:
                lab.clock.current += 0.5

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(time, "sleep", record_sleep)
    return lab


def assert_call(call: Any, expected: ExpectedCall, timeout: float) -> None:
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
    actual = json.loads(options["input"])
    assert json.dumps(actual, sort_keys=True) == json.dumps(payload, sort_keys=True)
    assert options["timeout"] == timeout
    assert options["shell"] is False
    assert options["check"] is False
    assert options["capture_output"] is True
    assert options["text"] is True
    assert_environment(options["env"], region)


def trace(lab: TransactionLab, count: int, start: int = 0) -> None:
    actual = len(lab.read.process.calls)
    for index, (call, expected) in enumerate(
        zip(lab.read.process.calls, lab.expected[:actual], strict=True)
    ):
        timeout = min(7, 1060 - (lab.clock.start + index * 0.5)) if lab.clock.ticks else 7
        assert_call(call, expected, timeout)
    budget = lab.read.context.budget
    assert lab.read.context is lab.original
    assert lab.read.context.lane == "work"
    assert budget is lab.original_budget is lab.read.process.budget
    assert budget.used == budget.work_used == start + actual
    assert lab.read.process.observed_debits == list(range(start + 1, start + actual + 1))
    assert budget.limits == Limits(192, 8, 2, 60, 96)
    assert budget.deadline == lab.read.services.spec.deadline == 1060
    assert budget.cleanup_deadline is None
    assert budget.limits.calls - budget.limits.cleanup_reserve - budget.work_used == 96 - (
        start + actual
    )
    assert lab.read.process.outcomes == list(lab.queued[actual:])
    assert len(lab.read.process.outcomes) == len(lab.expected) - actual
    assert lab.sleeps == []
    assert actual == count, (
        f"probe=ddb_transactions_semantic_trace_missing calls={actual} expected={count}"
    )


@pytest.fixture
def tx_lab(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> TransactionLab:
    caplog.set_level(logging.DEBUG)
    return make_transaction_lab(monkeypatch, tmp_path, Privacy(caplog, capsys))


@pytest.fixture(autouse=True)
def private_surface(tx_lab: TransactionLab) -> Iterator[None]:
    yield
    tx_lab.privacy.check(ServiceFailure("control=privacy"), tx_lab.read)
    assert list(tx_lab.read.directory.rglob("*")) == []
    assert tx_lab.sleeps == []
