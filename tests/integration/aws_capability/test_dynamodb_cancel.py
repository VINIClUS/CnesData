"""Cancelamento central exige razões ordenadas e três readbacks físicos independentes."""

import inspect
import json
import logging
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import pytest

from tests.integration.aws_capability._service_ddb_prepare_testing import (
    CONDITIONS_EVIDENCE,
    Privacy,
    _data,
)
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
    RequestContext,
    Resource,
    ServiceFailure,
)

ROOT = Resource("dynamodb", "cnesdata-cap-a1b2c3d4", "cnesdata-cap-a1b2c3d4")


@dataclass
class Clock:
    current: float = 1000
    ticks: bool = False
    queued: tuple[Any, ...] = field(default=(), repr=False)

    def now(self) -> float:
        return self.current


@dataclass
class CancelLab:
    read: ReadLab
    clock: Clock
    privacy: Privacy
    original: RequestContext


def _fixture(name: str = "ddb_cancel") -> Any:
    path = Path(__file__).parent / "fixtures" / "cli" / "b4" / f"{name}.json"
    return json.loads(path.read_text())


@pytest.fixture
def cancel_lab(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> CancelLab:
    caplog.set_level(logging.DEBUG)
    lab = make_read_lab(monkeypatch, tmp_path)
    clock = Clock()
    lab.services = replace(lab.services, spec=replace(lab.services.spec, probe="ddb-conditions"))
    lab.services = replace(lab.services, ports=replace(lab.services.ports, now=clock.now))
    lab.context.budget.now = clock.now
    lab.context.budget.limits = Limits(256, 8, 2, 60, 128)
    original = lab.context

    def run(argv: Sequence[str], **options: Any) -> subprocess.CompletedProcess[str]:
        frame = inspect.currentframe()
        assert frame is not None
        assert frame.f_back is not None
        cli = frame.f_back.f_locals["self"]
        assert isinstance(cli, Cli)
        assert cli.context is original
        assert cli.context.budget is lab.process.budget
        assert cli.context.lane == "work"
        completed = lab.process.run(argv, **options)
        if clock.ticks:
            clock.current += 0.5
        return completed

    monkeypatch.setattr(subprocess, "run", run)
    return CancelLab(lab, clock, Privacy(caplog, capsys), original)


def _outcome(reply: Any) -> tuple[int, str, str] | BaseException:
    if "timeout" in reply:
        return subprocess.TimeoutExpired("aws", 7, output=reply["timeout"], stderr=reply["timeout"])
    return (
        reply["exit"],
        json.dumps(reply["data"]) if "data" in reply else "",
        json.dumps(reply["error"]) if "error" in reply else "",
    )


def _queue(cancel: CancelLab, case: str = "") -> None:
    prep = _data()
    for data in (prep["active"], prep["tags"], prep["ttl"]["enabled"]):
        respond(cancel.read, data)
    for step in _fixture("ddb_cas")["steps"]:
        cancel.read.process.outcomes.append(_outcome(step["reply"]))
    fixture = _fixture()
    override = fixture["overrides"].get(case, {})
    for index, step in enumerate(fixture["steps"]):
        reply = override["reply"] if index == override.get("index") else step["reply"]
        cancel.read.process.outcomes.append(_outcome(reply))
    cancel.clock.queued = tuple(cancel.read.process.outcomes)


def _expected() -> list[Any]:
    prep = _data()["requests"]
    return [prep["describe"], prep["tags"], prep["ttl"]] + [
        step["request"] for step in _fixture("ddb_cas")["steps"] + _fixture()["steps"]
    ]


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
    actual = json.loads(options["input"])
    assert actual == payload
    assert json.dumps(actual, sort_keys=True) == json.dumps(payload, sort_keys=True)
    assert options["timeout"] == timeout
    assert options["shell"] is False
    assert options["check"] is False
    assert options["capture_output"] is True
    assert options["text"] is True
    assert_environment(options["env"], region)


def _trace(cancel: CancelLab, count: int, start: int = 0, tail: Sequence[Any] = ()) -> None:
    lab = cancel.read
    actual = len(lab.process.calls)
    timeouts = (
        [6.5 - index * 0.5 for index in range(actual)] if cancel.clock.ticks else [7] * actual
    )
    for call, expected, timeout in zip(
        lab.process.calls, (_expected() + list(tail))[:actual], timeouts, strict=True
    ):
        _assert_call(call, expected, timeout)
    budget = lab.context.budget
    assert lab.context is cancel.original
    assert lab.context.lane == "work"
    assert budget is cancel.original.budget is lab.process.budget
    assert budget.used == budget.work_used == start + actual
    assert lab.process.observed_debits == list(range(start + 1, start + actual + 1))
    assert budget.limits == Limits(256, 8, 2, 60, 128)
    assert budget.deadline == lab.services.spec.deadline == 1060
    assert budget.cleanup_deadline is None
    assert budget.limits.calls - budget.limits.cleanup_reserve - budget.work_used == 128 - (
        start + actual
    )
    assert len(lab.process.outcomes) == 16 + len(tail) - actual
    assert lab.process.outcomes == list(cancel.clock.queued[actual:])
    assert actual == count, (
        f"probe=ddb_cancel_semantic_trace_missing calls={actual} expected={count}"
    )


def _probe(cancel: CancelLab) -> BaseException:
    lab = cancel.read
    with pytest.raises((ServiceFailure, BudgetExhausted)) as error:
        lab.services.probe([ROOT], replace(lab.services.spec), lab.context)
    cancel.privacy.check(error.value, lab)
    return error.value


def _probe_ok(cancel: CancelLab) -> Any:
    lab = cancel.read
    result = lab.services.probe([ROOT], replace(lab.services.spec), lab.context)
    cancel.privacy.check_value(result, lab)
    return result


def test_cancelamento_preserva_tres_itens_e_devolve_observacoes_do_perfil(
    cancel_lab: CancelLab,
) -> None:
    _queue(cancel_lab)
    path = Path(__file__).parent / "fixtures" / "cli" / "b4" / "ddb_token.json"
    token_tail = json.loads(path.read_text())["steps"]
    for step in token_tail:
        cancel_lab.read.process.outcomes.append(_outcome(step["reply"]))
    cancel_lab.clock.queued = tuple(cancel_lab.read.process.outcomes)
    evidence = _probe_ok(cancel_lab)
    _trace(cancel_lab, 24, tail=[step["request"] for step in token_tail])
    assert evidence == CONDITIONS_EVIDENCE


@pytest.mark.parametrize(
    ("case", "count", "reason"),
    [
        ("colisao_seed_1", 10, "service=request_failed"),
        ("colisao_seed_2", 11, "service=request_failed"),
        ("colisao_seed_3", 12, "service=request_failed"),
        ("atributos_seed_1", 10, "probe=ddb_cancel_response_invalid"),
        ("atributos_seed_2", 11, "probe=ddb_cancel_response_invalid"),
        ("atributos_seed_3", 12, "probe=ddb_cancel_response_invalid"),
        ("transacao_sucesso_vazio", 13, "probe=ddb_cancel_rejection_invalid"),
        ("cancelamento_local", 13, "probe=ddb_cancel_rejection_invalid"),
        ("acesso_negado", 13, "probe=ddb_cancel_rejection_invalid"),
        ("reasons_ausentes", 13, "probe=ddb_cancel_reasons_invalid"),
        ("reasons_mapping", 13, "probe=ddb_cancel_reasons_invalid"),
        ("reasons_duas", 13, "probe=ddb_cancel_reasons_invalid"),
        ("reasons_quatro", 13, "probe=ddb_cancel_reasons_invalid"),
        ("reason_null", 13, "probe=ddb_cancel_reasons_invalid"),
        ("code_null", 13, "probe=ddb_cancel_reasons_invalid"),
        ("codes_reordenados", 13, "probe=ddb_cancel_reasons_invalid"),
        ("code_ausente", 13, "probe=ddb_cancel_reasons_invalid"),
        ("reason_string", 13, "probe=ddb_cancel_reasons_invalid"),
        ("primeiro_ausente", 14, "probe=ddb_cancel_readback_invalid"),
        ("segundo_dois", 15, "probe=ddb_cancel_readback_invalid"),
        ("terceiro_estrangeiro", 16, "probe=ddb_cancel_readback_invalid"),
        ("terceiro_numero", 16, "probe=ddb_cancel_readback_invalid"),
        ("timeout_transacao", 13, "probe=ddb_cancel_rejection_invalid"),
        ("timeout_primeiro_get", 14, "service=request_failed"),
    ],
)
def test_cancelamento_recusa_sem_continuar(
    cancel_lab: CancelLab, case: str, count: int, reason: str
) -> None:
    _queue(cancel_lab, case)
    error = _probe(cancel_lab)
    _trace(cancel_lab, count)
    assert type(error) is ServiceFailure
    assert error.args == (reason,)


def test_cancelamento_preserva_reserva_antes_do_primeiro_read(cancel_lab: CancelLab) -> None:
    budget = cancel_lab.read.context.budget
    for _ in range(115):
        budget.acquire("work")
    _queue(cancel_lab)
    error = _probe(cancel_lab)
    _trace(cancel_lab, 13, start=115)
    assert budget.used == budget.work_used == 128
    assert budget.limits.calls - budget.used == budget.limits.cleanup_reserve == 128
    assert type(error) is BudgetExhausted
    assert error.args == ("budget=requests",)


def test_cancelamento_respeita_deadline_antes_do_primeiro_read(cancel_lab: CancelLab) -> None:
    cancel_lab.clock.current = 1053.5
    cancel_lab.clock.ticks = True
    _queue(cancel_lab)
    error = _probe(cancel_lab)
    _trace(cancel_lab, 13)
    assert cancel_lab.clock.current == 1060
    assert type(error) is BudgetExhausted
    assert error.args == ("budget=deadline",)


def test_fronteira_real_preserva_cancelamento_modelado_e_dezesseis_calls(
    cancel_lab: CancelLab,
) -> None:
    _queue(cancel_lab)
    lab = cancel_lab.read
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
    ]
    assert [reply.data for reply in replies[9:12]] == [
        step["reply"]["data"] for step in _fixture()["steps"][:3]
    ]
    assert [reply.data for reply in replies[13:16]] == [
        step["reply"]["data"] for step in _fixture()["steps"][4:7]
    ]
    remote = replies[12].error
    assert remote is not None
    assert remote.origin == "service"
    assert remote.code == "TransactionCanceledException"
    reasons = remote.details["CancellationReasons"]
    assert isinstance(reasons, list)
    assert [reason["Code"] for reason in reasons] == [
        "None",
        "ConditionalCheckFailed",
        "None",
    ]
    assert CANARY not in repr(replies)
    _trace(cancel_lab, 16)
    cancel_lab.privacy.check(ServiceFailure("control=boundary"), lab)
