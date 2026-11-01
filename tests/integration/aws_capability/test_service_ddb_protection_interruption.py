"""Janitor reconstruído distingue desproteção interrompida de delete aceito."""

import json
import os
import subprocess
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, cast

import pytest

from tests.integration.aws_capability._durability import DurabilityTrace
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._service_contracts import ServicePorts
from tests.integration.aws_capability._service_ddb_prepare_testing import Privacy
from tests.integration.aws_capability._service_read_assertions import (
    FIXED_FLAGS,
    assert_environment,
)
from tests.integration.aws_capability._service_read_testing import ReadLab, make_read_lab
from tests.integration.aws_capability._service_testing import ProcessScript
from tests.integration.aws_capability._testing import ProcessBoundary
from tests.integration.aws_capability.contracts import Policy, Ports, RequestContext
from tests.integration.aws_capability.lifecycle import janitor
from tests.integration.aws_capability.services import AwsCommands, AwsServices


def fixture(name: str = "ddb_protection_interruption") -> Any:
    path = Path(__file__).parent / "fixtures" / "cli" / "b3" / f"{name}.json"
    return json.loads(path.read_text())


class ProtectionInterruption(BaseException):
    pass


@dataclass(frozen=True)
class Snapshot:
    operation: str
    data: bytes
    inode: int
    events: tuple[tuple, ...]


@dataclass
class Recorder:
    lab: ReadLab
    durability: DurabilityTrace
    snapshots: list[Snapshot] = field(default_factory=list)
    update_reply: Any = None

    def run(self, argv: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        reply = self.lab.process.run(argv, **options)
        operation = argv[2]
        if operation in {"update-table", "delete-table"} or (
            operation == "describe-table" and reply.returncode != 0
        ):
            path = Journal(self.lab.services.spec).path
            self.snapshots.append(
                Snapshot(
                    operation, path.read_bytes(), path.stat().st_ino, tuple(self.durability.events)
                )
            )
        if operation == "update-table":
            self.update_reply = json.loads(reply.stdout)
            raise ProtectionInterruption
        return reply


@dataclass
class Experiment:
    lab: ReadLab
    recorder: Recorder
    clock: list[float]
    before: bytes


def ports(lab: ReadLab) -> Ports:
    return Ports(
        lab.services,
        ProcessBoundary([]),
        lab.context.budget.now,
        Policy("111111111111", frozenset({"us-east-2"})),
        lab.context.budget,
    )


def seed(lab: ReadLab) -> bytes:
    path = Journal(lab.services.spec).path
    data = json.dumps(fixture()["states"]["initial"]).encode()
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    assert path.read_bytes() == data
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    return data


def queue(lab: ReadLab, phase: str) -> None:
    data = fixture("ddb_delete")
    for _, reply in fixture()[phase]:
        if reply in data["failures"]:
            code, stdout, stderr = data["failures"][reply]
            lab.process.outcomes.append((code, stdout, stderr))
        else:
            lab.process.outcomes.append((0, json.dumps(data["replies"][reply]), ""))


@pytest.fixture
def interruption_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Experiment:
    directory = tmp_path / "private"
    prepare_directory(directory)
    lab = make_read_lab(monkeypatch, directory)
    clock = [1000.0]
    lab.context.budget.now = lambda: clock[0]
    lab.context = RequestContext(lab.context.budget, "cleanup")
    lab.services = replace(
        lab.services, spec=replace(lab.services.spec, allow_governance_bypass=True)
    )
    before = seed(lab)
    durability = DurabilityTrace()
    durability.install(monkeypatch)
    recorder = Recorder(lab, durability)
    monkeypatch.setattr(subprocess, "run", recorder.run)
    return Experiment(lab, recorder, clock, before)


@pytest.fixture
def privacy(caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]) -> Privacy:
    return Privacy(caplog, capsys)


def assert_trace(lab: ReadLab, phase: str, offset: int = 0) -> None:
    expected = [fixture("ddb_delete")["requests"][name] for name, _ in fixture()[phase]]
    assert len(lab.process.calls) == fixture()["counts"][phase] == len(expected)
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
            "7",
            "--cli-read-timeout",
            "7",
        ]
        actual = json.loads(options["input"])
        assert actual == payload
        assert {key: type(value) for key, value in actual.items()} == {
            key: type(value) for key, value in payload.items()
        }
        assert {key: value for key, value in options.items() if key not in {"input", "env"}} == {
            "timeout": 7,
            "shell": False,
            "capture_output": True,
            "text": True,
            "check": False,
        }
        assert_environment(options["env"], region)
    assert lab.process.observed_debits == list(range(offset + 1, offset + len(expected) + 1))
    assert lab.process.outcomes == []
    budget = lab.context.budget
    assert budget.used == offset + len(expected)
    assert budget.work_used == 0
    assert budget.cleanup_deadline == fixture()["cleanup_deadline"]
    assert budget.deadline == 1060
    assert asdict(budget.limits) == fixture()["limits"]
    assert lab.services.spec.probe == "all"


def cut(experiment: Experiment) -> None:
    lab, recorder = experiment.lab, experiment.recorder
    queue(lab, "cutoff")
    with pytest.raises(ProtectionInterruption):
        janitor(lab.services.spec, ports(lab))
    assert_trace(lab, "cutoff")
    assert [snapshot.operation for snapshot in recorder.snapshots] == ["update-table"]
    assert recorder.update_reply == fixture("ddb_delete")["replies"]["update"]
    snapshot = recorder.snapshots[0]
    assert snapshot.data == experiment.before, "oracle=protection_created_premature_receipt"
    assert json.loads(snapshot.data) == fixture()["states"]["initial"]
    assert "dynamodb_delete_accepted" not in json.loads(snapshot.data)
    assert snapshot.events == ()
    assert Journal(lab.services.spec).path.read_bytes() == experiment.before


def restart(experiment: Experiment, monkeypatch: pytest.MonkeyPatch) -> tuple[ReadLab, Recorder]:
    previous = experiment.lab
    budget = previous.context.budget
    experiment.clock[0] = 1001
    old = cast("AwsCommands", previous.services.ports.commands)
    commands = AwsCommands("us-east-2", dict(old.environment), 7)
    spec = replace(previous.services.spec)
    services = AwsServices(spec, ServicePorts(commands, budget.now))
    lab = ReadLab(
        services,
        ProcessScript(budget=budget),
        RequestContext(budget, "cleanup"),
        previous.directory,
    )
    journal = Journal(spec)
    journal.load()
    assert json.loads(journal.path.read_bytes()) == fixture()["states"]["initial"]
    assert [asdict(root) for root in journal.created] == fixture()["states"]["initial"]["created"]
    assert journal.dynamodb_delete_accepted == []
    assert services is not previous.services
    assert commands is not old
    assert lab.process is not previous.process
    assert lab.context.budget is previous.context.budget
    recorder = Recorder(lab, experiment.recorder.durability)
    monkeypatch.setattr(subprocess, "run", recorder.run)
    return lab, recorder


def assert_receipt_durable(snapshot: Snapshot, lab: ReadLab) -> None:
    assert json.loads(snapshot.data) == fixture()["states"]["accepted"]
    path = Journal(lab.services.spec).path
    positions = [
        (index, event) for index, event in enumerate(snapshot.events) if event[0] == "replace"
    ]
    assert len(positions) == 1
    index, replacement = positions[0]
    assert replacement[1] == snapshot.inode
    assert replacement[3] == path
    assert replacement[2].parent == path.parent
    files = [event for event in snapshot.events[:index] if event[0] == "file"]
    assert len(files) == 1
    assert files[0][1:3] == replacement[1:3]
    assert files[0][3] == snapshot.data
    directories = [event for event in snapshot.events[index + 1 :] if event[0] == "directory"]
    assert len(directories) == 1
    assert directories[0][1:] == (path.parent.stat().st_ino, path.parent)


def assert_final(lab: ReadLab, state: str) -> None:
    journal = Journal(lab.services.spec)
    journal.load()
    assert json.loads(journal.path.read_bytes()) == fixture()["states"][state]
    assert [asdict(root) for root in journal.created] == fixture()["states"][state]["created"]
    assert journal.dynamodb_delete_accepted == []
    assert journal.path.stat().st_mode & 0o777 == 0o600
    assert lab.directory.stat().st_mode & 0o777 == 0o700


def test_interrupcao_apos_desproteger_retoma_sem_repetir_update(
    interruption_lab: Experiment, monkeypatch: pytest.MonkeyPatch, privacy: Privacy
) -> None:
    cut(interruption_lab)
    privacy.check(ProtectionInterruption(), interruption_lab.lab)
    lab, recorder = restart(interruption_lab, monkeypatch)
    queue(lab, "positive")
    outcome = janitor(lab.services.spec, ports(lab))
    assert_trace(lab, "positive", 16)
    assert (outcome.status, outcome.reason, outcome.remaining) == ("succeeded", "complete", ())
    assert outcome.evidence == fixture()["evidence"]
    assert lab.context.budget.used == fixture()["accumulated"]["positive"]
    assert [snapshot.operation for snapshot in recorder.snapshots] == [
        "delete-table",
        "describe-table",
        "describe-table",
    ]
    assert recorder.snapshots[0].data == interruption_lab.before
    assert recorder.snapshots[0].events == ()
    assert_receipt_durable(recorder.snapshots[1], lab)
    assert_receipt_durable(recorder.snapshots[2], lab)
    assert_final(lab, "complete")
    privacy.check(ProtectionInterruption(repr(outcome)), lab)


def test_interrupcao_apos_desproteger_nao_gera_recibo_quando_root_sumiu(
    interruption_lab: Experiment, monkeypatch: pytest.MonkeyPatch, privacy: Privacy
) -> None:
    cut(interruption_lab)
    privacy.check(ProtectionInterruption(), interruption_lab.lab)
    lab, recorder = restart(interruption_lab, monkeypatch)
    queue(lab, "negative")
    outcome = janitor(lab.services.spec, ports(lab))
    assert_trace(lab, "negative", 16)
    assert (outcome.status, outcome.reason, outcome.remaining) == (
        "incomplete",
        "creation_uncertain",
        (),
    )
    assert outcome.evidence == fixture()["evidence"]
    assert lab.context.budget.used == fixture()["accumulated"]["negative"]
    assert [snapshot.operation for snapshot in recorder.snapshots] == ["describe-table"]
    assert recorder.snapshots[0].data == interruption_lab.before
    assert recorder.snapshots[0].events == ()
    assert Journal(lab.services.spec).path.read_bytes() == interruption_lab.before
    assert_final(lab, "initial")
    privacy.check(ProtectionInterruption(repr(outcome)), lab)
