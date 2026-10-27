"""Token DynamoDB exige replay idempotente, deleção e leituras fortes."""

import copy
import inspect
import json
import logging
import subprocess
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import pytest

from tests.integration.aws_capability._service_ddb_prepare_testing import Privacy, _data
from tests.integration.aws_capability._service_read_assertions import (
    FIXED_FLAGS,
    assert_environment,
)
from tests.integration.aws_capability._service_read_testing import (
    CANARY,
    ReadLab,
    make_read_lab,
    respond,
)
from tests.integration.aws_capability.cli import Cli, Request
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Limits,
    Resource,
    ServiceFailure,
)

ROOT = Resource("dynamodb", "cnesdata-cap-a1b2c3d4", "cnesdata-cap-a1b2c3d4")
Step = tuple[str, Any]
TOKEN_KEY = {"pk": {"S": "cap#a1b2c3d4"}, "sk": {"S": "token"}}
TOKEN_I1 = {**TOKEN_KEY, "version": {"N": "1"}}
TOKEN_I2 = {**TOKEN_KEY, "version": {"N": "2"}}


@dataclass
class Clock:
    current: float = 1000
    ticks: bool = False
    queued: tuple[Any, ...] = field(default=(), repr=False)

    def now(self) -> float:
        return self.current


@dataclass
class TokenLab:
    read: ReadLab
    clock: Clock
    privacy: Privacy
    original: Any
    original_budget: Any
    sleeps: list[float]


def _assert_no_sleeps(sleeps: Sequence[float]) -> None:
    assert list(sleeps) == []


def _fixture(name: str = "ddb_token") -> Any:
    path = Path(__file__).parent / "fixtures" / "cli" / "b4" / f"{name}.json"
    return json.loads(path.read_text())


@pytest.fixture
def token_lab(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> TokenLab:
    caplog.set_level(logging.DEBUG)
    lab = make_read_lab(monkeypatch, tmp_path)
    clock = Clock()
    lab.services = replace(lab.services, spec=replace(lab.services.spec, probe="ddb-conditions"))
    lab.services = replace(lab.services, ports=replace(lab.services.ports, now=clock.now))
    lab.context.budget.now = clock.now
    lab.context.budget.limits = Limits(256, 8, 2, 60, 128)
    original = lab.context
    original_budget = lab.context.budget
    sleeps: list[float] = []

    def record_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    def run(argv: Sequence[str], **options: Any) -> subprocess.CompletedProcess[str]:
        frame = inspect.currentframe()
        assert frame is not None
        assert frame.f_back is not None
        cli = frame.f_back.f_locals["self"]
        assert isinstance(cli, Cli)
        assert cli.context is original
        assert cli.context.budget is original_budget
        assert cli.context.lane == "work"
        completed = lab.process.run(argv, **options)
        if clock.ticks:
            clock.current += 0.5
        return completed

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(time, "sleep", record_sleep)
    return TokenLab(lab, clock, Privacy(caplog, capsys), original, original_budget, sleeps)


@pytest.fixture(autouse=True)
def private_surface(
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    token_lab: TokenLab,
) -> Iterator[None]:
    caplog.set_level(logging.DEBUG)
    yield
    captured = capsys.readouterr()
    assert all(secret not in caplog.text + captured.out + captured.err for secret in (CANARY,))
    assert list(tmp_path.rglob("*")) == []
    _assert_no_sleeps(token_lab.sleeps)


def _outcome(reply: Any) -> tuple[int, str, str] | BaseException:
    if "timeout" in reply:
        return subprocess.TimeoutExpired("aws", 7, output=reply["timeout"], stderr=reply["timeout"])
    return (
        reply["exit"],
        json.dumps(reply["data"]) if "data" in reply else "",
        json.dumps(reply["error"]) if "error" in reply else "",
    )


def _queue(lab: TokenLab, case: str = "") -> None:
    prep = _data()
    for data in (prep["active"], prep["tags"], prep["ttl"]["enabled"]):
        respond(lab.read, data)
    for name in ("ddb_cas", "ddb_cancel"):
        for step in _fixture(name)["steps"]:
            lab.read.process.outcomes.append(_outcome(step["reply"]))
    fixture = _fixture()
    override = fixture["overrides"].get(case, {})
    for index, step in enumerate(fixture["steps"]):
        reply = override["reply"] if index == override.get("index") else step["reply"]
        lab.read.process.outcomes.append(_outcome(reply))
    lab.clock.queued = tuple(lab.read.process.outcomes)


def _expected() -> list[Any]:
    prep = _data()["requests"]
    requests = [prep["describe"], prep["tags"], prep["ttl"]]
    for name in ("ddb_cas", "ddb_cancel", "ddb_token"):
        requests.extend(step["request"] for step in _fixture(name)["steps"])
    return requests


def _assert_call(call: Any, expected: Any, timeout: float) -> None:
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
    assert json.loads(options["input"]) == payload
    assert options["timeout"] == timeout
    assert options["shell"] is False
    assert options["check"] is False
    assert options["capture_output"] is True
    assert options["text"] is True
    assert_environment(options["env"], region)


def _trace(lab: TokenLab, count: int, start: int = 0) -> None:
    read = lab.read
    actual = len(read.process.calls)
    expected = _expected()
    for index, (call, request) in enumerate(
        zip(read.process.calls, expected[:actual], strict=True)
    ):
        timeout = 7
        if lab.clock.ticks:
            timeout = min(7, max(0, 1060 - (1049.5 + index * 0.5)))
        _assert_call(call, request, timeout)
    budget = read.context.budget
    assert read.context is lab.original
    assert read.context.lane == "work"
    assert budget is lab.original.budget is read.process.budget
    assert budget.used == budget.work_used == start + actual
    assert read.process.observed_debits == list(range(start + 1, start + actual + 1))
    assert budget.limits == Limits(256, 8, 2, 60, 128)
    assert budget.deadline == read.services.spec.deadline == 1060
    assert budget.cleanup_deadline is None
    assert budget.limits.calls - budget.limits.cleanup_reserve - budget.work_used == 128 - (
        start + actual
    )
    assert len(read.process.outcomes) == 24 - actual
    assert read.process.outcomes == list(lab.clock.queued[actual:])
    _assert_no_sleeps(lab.sleeps)
    assert actual == count, (
        f"probe=ddb_token_semantic_trace_missing calls={actual} expected={count}"
    )


def _probe(lab: TokenLab) -> BaseException:
    with pytest.raises((ServiceFailure, BudgetExhausted)) as error:
        lab.read.services.probe([ROOT], replace(lab.read.services.spec), lab.read.context)
    lab.privacy.check(error.value, lab.read)
    return error.value


def _assert_token_shape(lab: TokenLab) -> None:
    calls = lab.read.process.calls[16:24]
    assert len(calls) == 8
    assert calls[0][1]["input"] == calls[4][1]["input"]
    first = json.loads(calls[0][1]["input"])
    replay = json.loads(calls[4][1]["input"])
    mismatch = json.loads(calls[6][1]["input"])
    assert first == replay
    assert first["ClientRequestToken"] == "a1b2c3d4-token"
    assert first["TransactItems"][0]["Put"]["Item"] == TOKEN_I1
    changed = copy.deepcopy(first)
    changed["TransactItems"][0]["Put"]["Item"]["version"] = {"N": "2"}
    assert mismatch == changed
    assert mismatch["TransactItems"][0]["Put"]["Item"] == TOKEN_I2


def _assert_transport_replies(replies: Sequence[Any]) -> None:
    assert len(replies) == 24
    assert replies[16].data == replies[19].data == replies[20].data == replies[23].data == {}
    item = replies[17].data["Item"]
    attributes = replies[18].data["Attributes"]
    assert item == TOKEN_I1
    assert attributes == TOKEN_I1
    error = replies[22].error
    assert error is not None
    assert (error.origin, error.code) == ("service", "IdempotentParameterMismatchException")


def test_token_nominal_exige_oito_chamadas_sem_concluir_perfil(token_lab: TokenLab) -> None:
    _queue(token_lab)
    error = _probe(token_lab)
    _trace(token_lab, 24)
    _assert_token_shape(token_lab)
    assert type(error) is ServiceFailure
    assert error.args == ("probe=not_implemented",)


@pytest.mark.parametrize(
    ("case", "count", "reason"),
    [
        ("transacao_remota", 17, "service=request_failed"),
        ("transacao_timeout", 17, "service=request_failed"),
        ("leitura_inicial_ausente", 18, "probe=ddb_token_readback_invalid"),
        ("leitura_inicial_estrangeira", 18, "probe=ddb_token_readback_invalid"),
        ("leitura_inicial_dois", 18, "probe=ddb_token_readback_invalid"),
        ("delete_failed", 19, "service=request_failed"),
        ("delete_missing_old", 19, "probe=ddb_token_response_invalid"),
        ("delete_wrong_old", 19, "probe=ddb_token_response_invalid"),
        ("ausencia_item", 20, "probe=ddb_token_readback_invalid"),
        ("ausencia_null", 20, "probe=ddb_token_readback_invalid"),
        ("ausencia_vazia", 20, "probe=ddb_token_readback_invalid"),
        ("replay_remoto", 21, "service=request_failed"),
        ("replay_timeout", 21, "service=request_failed"),
        ("pos_replay_recriado", 22, "probe=ddb_token_readback_invalid"),
        ("pos_replay_estrangeiro", 22, "probe=ddb_token_readback_invalid"),
        ("mismatch_sucesso", 23, "probe=ddb_token_rejection_invalid"),
        ("mismatch_local", 23, "probe=ddb_token_rejection_invalid"),
        ("mismatch_remoto_errado", 23, "probe=ddb_token_rejection_invalid"),
        ("mismatch_timeout", 23, "probe=ddb_token_rejection_invalid"),
        ("leitura_final_dois", 24, "probe=ddb_token_readback_invalid"),
        ("timeout_18", 18, "service=request_failed"),
        ("timeout_20", 20, "service=request_failed"),
        ("timeout_22", 22, "service=request_failed"),
        ("timeout_24", 24, "service=request_failed"),
    ],
)
def test_token_recusa_sem_repetir_nem_vazar(
    token_lab: TokenLab, case: str, count: int, reason: str
) -> None:
    _queue(token_lab, case)
    error = _probe(token_lab)
    _trace(token_lab, count)
    assert type(error) is ServiceFailure
    assert error.args == (reason,)


def test_token_preserva_reserva_apos_vinte_chamadas(token_lab: TokenLab) -> None:
    budget = token_lab.read.context.budget
    for _ in range(108):
        budget.acquire("work")
    _queue(token_lab)
    error = _probe(token_lab)
    _trace(token_lab, 20, start=108)
    assert type(error) is BudgetExhausted
    assert error.args == ("budget=requests",)
    assert budget.used == budget.work_used == 128
    assert budget.limits.calls - budget.used == budget.limits.cleanup_reserve == 128


def test_token_respeita_deadline_antes_do_get_pos_replay(token_lab: TokenLab) -> None:
    token_lab.clock.current = 1049.5
    token_lab.clock.ticks = True
    _queue(token_lab)
    error = _probe(token_lab)
    _trace(token_lab, 21)
    assert type(error) is BudgetExhausted
    assert error.args == ("budget=deadline",)
    assert token_lab.clock.current == 1060
    assert token_lab.read.services.spec.deadline == 1060


def test_fronteira_real_consumindo_trace_literal_de_24_respostas(token_lab: TokenLab) -> None:
    _queue(token_lab)
    lab = token_lab.read
    replies = [
        lab.services.ports.commands.call(Request(service, operation, payload), lab.context)
        for service, operation, _, payload in _expected()
    ]
    assert [reply.ok for reply in replies] == [
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        False,
        True,
        True,
        True,
        True,
        False,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        True,
        False,
        True,
    ]
    _assert_transport_replies(replies)
    _assert_token_shape(token_lab)
    assert CANARY not in repr(replies)
    _trace(token_lab, 24)
    token_lab.privacy.check(ServiceFailure("control=boundary"), lab)
