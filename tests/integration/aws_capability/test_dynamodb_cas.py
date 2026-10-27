"""CAS DynamoDB parcial exige escrita condicional e readbacks fortes independentes."""

import json
import logging
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass, replace
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
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Limits,
    Resource,
    ServiceFailure,
)

ROOT = Resource("dynamodb", "cnesdata-cap-a1b2c3d4", "cnesdata-cap-a1b2c3d4")


@dataclass
class Clock:
    current: float = 1000
    ticks: bool = False

    def now(self) -> float:
        return self.current


@dataclass
class CasLab:
    read: ReadLab
    clock: Clock
    privacy: Privacy


def _cas_data() -> Any:
    path = Path(__file__).parent / "fixtures" / "cli" / "b4" / "ddb_cas.json"
    return json.loads(path.read_text())


@pytest.fixture
def cas_lab(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> CasLab:
    caplog.set_level(logging.DEBUG)
    lab = make_read_lab(monkeypatch, tmp_path)
    clock = Clock()
    lab.services = replace(lab.services, spec=replace(lab.services.spec, probe="ddb-conditions"))
    lab.services = replace(lab.services, ports=replace(lab.services.ports, now=clock.now))
    lab.context.budget.now = clock.now
    lab.context.budget.limits = Limits(256, 8, 2, 60, 128)

    def run(argv: Sequence[str], **options: Any) -> subprocess.CompletedProcess[str]:
        completed = lab.process.run(argv, **options)
        if clock.ticks:
            clock.current += 0.5
            if len(lab.process.calls) == 8:
                clock.current = 1060
        return completed

    monkeypatch.setattr(subprocess, "run", run)
    return CasLab(lab, clock, Privacy(caplog, capsys))


def _outcome(reply: Any) -> tuple[int, str, str] | BaseException:
    if "timeout" in reply:
        return subprocess.TimeoutExpired("aws", 7, output=reply["timeout"], stderr=reply["timeout"])
    return (
        reply["exit"],
        json.dumps(reply["data"]) if "data" in reply else "",
        json.dumps(reply["error"]) if "error" in reply else "",
    )


def _queue(cas: CasLab, case: str = "") -> None:
    prep = _data()
    for data in (prep["active"], prep["tags"], prep["ttl"]["enabled"]):
        respond(cas.read, data)
    fixture = _cas_data()
    override = fixture["overrides"].get(case, {})
    for index, step in enumerate(fixture["steps"]):
        reply = override["reply"] if index == override.get("index") else step["reply"]
        cas.read.process.outcomes.append(_outcome(reply))


def _expected() -> list[Any]:
    prep = _data()["requests"]
    return [prep["describe"], prep["tags"], prep["ttl"]] + [
        step["request"] for step in _cas_data()["steps"]
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


def _trace(cas: CasLab, count: int, start: int = 0, tail: Sequence[Any] = ()) -> None:
    lab = cas.read
    actual = len(lab.process.calls)
    timeouts = [6.5, 6, 5.5, 5, 4.5, 4, 3.5, 3] if cas.clock.ticks else [7] * actual
    expected_calls = _expected() + list(tail)
    for call, expected, timeout in zip(
        lab.process.calls, expected_calls[:actual], timeouts[:actual], strict=True
    ):
        _assert_call(call, expected, timeout)
    budget = lab.context.budget
    assert lab.process.budget is budget
    assert budget.used == budget.work_used == start + actual
    assert lab.process.observed_debits == list(range(start + 1, start + actual + 1))
    assert budget.limits == Limits(256, 8, 2, 60, 128)
    assert budget.deadline == lab.services.spec.deadline == 1060
    assert budget.cleanup_deadline is None
    assert budget.limits.calls - budget.limits.cleanup_reserve - budget.work_used == 128 - (
        start + actual
    )
    assert len(lab.process.outcomes) == 9 + len(tail) - actual
    assert actual == count, f"probe=ddb_cas_semantic_trace_missing calls={actual} expected={count}"


def _probe(cas: CasLab) -> BaseException:
    lab = cas.read
    with pytest.raises((ServiceFailure, BudgetExhausted)) as error:
        lab.services.probe([ROOT], replace(lab.services.spec), lab.context)
    cas.privacy.check(error.value, lab)
    return error.value


def test_cas_confirma_um_dois_e_rejeicao_remota_sem_concluir_perfil(cas_lab: CasLab) -> None:
    _queue(cas_lab)
    path = Path(__file__).parent / "fixtures" / "cli" / "b4" / "ddb_cancel.json"
    tail = json.loads(path.read_text())["steps"]
    for step in tail:
        cas_lab.read.process.outcomes.append(_outcome(step["reply"]))
    token_path = path.with_name("ddb_token.json")
    token_tail = json.loads(token_path.read_text())["steps"]
    for step in token_tail:
        cas_lab.read.process.outcomes.append(_outcome(step["reply"]))
    error = _probe(cas_lab)
    assert type(error) is ServiceFailure
    assert error.args == ("probe=not_implemented",)
    _trace(cas_lab, 24, tail=[step["request"] for step in [*tail, *token_tail]])


@pytest.mark.parametrize(
    ("case", "count", "reason"),
    [
        ("colisao_seed", 4, "service=request_failed"),
        ("item_inicial_ausente", 5, "probe=ddb_cas_readback_invalid"),
        ("echo_dois_readback_um", 7, "probe=ddb_cas_readback_invalid"),
        ("echo_cas_tres", 6, "probe=ddb_cas_response_invalid"),
        ("chave_estrangeira", 7, "probe=ddb_cas_readback_invalid"),
        ("stale_aceito", 8, "probe=ddb_cas_rejection_invalid"),
        ("conditional_local", 8, "probe=ddb_cas_rejection_invalid"),
        ("stale_denied", 8, "probe=ddb_cas_rejection_invalid"),
        ("ultimo_read_tres", 9, "probe=ddb_cas_readback_invalid"),
        ("ultimo_read_numero", 9, "probe=ddb_cas_readback_invalid"),
        ("timeout_cas", 6, "service=request_failed"),
    ],
)
def test_cas_recusa_sem_adotar_estado_nem_continuar(
    cas_lab: CasLab, case: str, count: int, reason: str
) -> None:
    _queue(cas_lab, case)
    error = _probe(cas_lab)
    assert type(error) is ServiceFailure
    assert error.args == (reason,)
    _trace(cas_lab, count)


def test_cas_preserva_reserva_ao_esgotar_work_apos_preparo(cas_lab: CasLab) -> None:
    budget = cas_lab.read.context.budget
    for _ in range(125):
        budget.acquire("work")
    _queue(cas_lab)
    error = _probe(cas_lab)
    _trace(cas_lab, 3, start=125)
    assert budget.limits.calls - budget.used == budget.limits.cleanup_reserve == 128
    assert type(error) is BudgetExhausted
    assert error.args == ("budget=requests",)


def test_cas_respeita_deadline_original_antes_do_ultimo_read(cas_lab: CasLab) -> None:
    cas_lab.clock.current = 1053.5
    cas_lab.clock.ticks = True
    _queue(cas_lab)
    error = _probe(cas_lab)
    _trace(cas_lab, 8)
    assert cas_lab.clock.current == 1060
    assert type(error) is BudgetExhausted
    assert error.args == ("budget=deadline",)


def test_fronteira_real_aceita_fixtures_e_distingue_conditional_remoto(cas_lab: CasLab) -> None:
    _queue(cas_lab)
    lab = cas_lab.read
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
    ]
    assert [reply.data for reply in replies[3:7]] == [
        step["reply"]["data"] for step in _cas_data()["steps"][:4]
    ]
    assert replies[-1].data == _cas_data()["steps"][-1]["reply"]["data"]
    assert CANARY not in repr(replies)
    remote = replies[-2].error
    assert remote is not None
    assert remote.origin == "service"
    assert remote.code == "ConditionalCheckFailedException"
    _trace(cas_lab, 9)
    cas_lab.privacy.check(ServiceFailure("control=boundary"), lab)
