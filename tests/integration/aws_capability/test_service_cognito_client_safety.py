"""Orçamento e canais públicos conservam limites e material privado do client."""

import json
import logging
import subprocess
import traceback
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_cognito_testing import make_read_lab
from tests.integration.aws_capability._service_read_assertions import (
    FIXED_FLAGS,
    assert_environment,
    assert_private,
    assert_process,
)
from tests.integration.aws_capability._service_read_testing import CANARY, ReadLab
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Limits,
    Resource,
    ServiceFailure,
)
from tests.integration.aws_capability.test_service_cognito_client_lookup import (
    CLIENT,
    DATA,
    TRACE,
    assert_owner,
    enqueue,
    enqueue_error,
)

PRIVATE_IDS = (
    "us-east-2_R1PRIVATECANARY",
    "r1privatecanary",
    "arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_R1PRIVATECANARY",
)


def _channels(caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]) -> str:
    logs = ""
    for phase in ("setup", "call", "teardown"):
        for record in caplog.get_records(phase):
            logs += record.getMessage() + (record.exc_text or "") + (record.stack_info or "")
            if record.exc_info is not None:
                logs += "".join(traceback.format_exception(*record.exc_info))
    captured = capsys.readouterr()
    return logs + captured.out + captured.err


def _assert_channels(error: BaseException | None, lab: ReadLab, captured: str) -> None:
    public = captured
    if error is not None:
        assert_private(error, lab, captured)
        public += "".join(traceback.format_exception(error)) + repr(error)
    public += repr(lab.services) + repr(lab.services.ports) + repr(lab.services.ports.commands)
    for path in lab.directory.rglob("*"):
        public += str(path.relative_to(lab.directory))
        if path.is_file() and not path.is_symlink():
            public += path.read_text(errors="replace")
    assert CANARY not in public
    assert all(value not in public for value in PRIVATE_IDS)
    assert list(lab.directory.iterdir()) == []


def _capture_channels(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)


@pytest.fixture
def read_lab(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> ReadLab:
    _capture_channels(caplog)
    return make_read_lab(monkeypatch, tmp_path)


@pytest.mark.parametrize("failure", [key for key in DATA["errors"] if key != "not_found"])
def test_falha_ambigua_do_client_nao_vira_ausencia(
    read_lab: ReadLab,
    failure: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    enqueue(read_lab, "root", "tags")
    enqueue_error(read_lab, failure)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(CLIENT, read_lab.context)
    assert error.value.args == (
        "service=describe_invalid" if failure == "empty" else "service=describe_failed",
    )
    _assert_channels(error.value, read_lab, _channels(caplog, capsys))
    assert_process(read_lab, TRACE)


@pytest.mark.parametrize("failure", ["timeout", "oserror"])
def test_transporte_do_client_falha_sem_retry_e_sem_causa_privada(
    read_lab: ReadLab,
    failure: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    enqueue(read_lab, "root", "tags")
    outcome = (
        subprocess.TimeoutExpired("aws", 7, output=CANARY, stderr=CANARY)
        if failure == "timeout"
        else OSError(CANARY)
    )
    read_lab.process.outcomes.append(outcome)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(CLIENT, read_lab.context)
    assert error.value.args == ("service=describe_failed",)
    _assert_channels(error.value, read_lab, _channels(caplog, capsys))
    assert_process(read_lab, TRACE)


@pytest.mark.parametrize("cut", [0, 1, 2])
def test_deadline_interrompe_antes_do_proximo_readback(read_lab: ReadLab, cut: int) -> None:
    enqueue(read_lab, *("root", "tags")[:cut])
    read_lab.context.budget.now = lambda: 1060 if len(read_lab.process.calls) >= cut else 1000
    with pytest.raises(BudgetExhausted) as error:
        read_lab.services.lookup(CLIENT, read_lab.context)
    assert error.value.args == ("budget=deadline",)
    assert_private(error.value, read_lab, "")
    assert_process(read_lab, TRACE[:cut])
    assert read_lab.context.budget.limits == Limits()


def _assert_reserved_trace(lab: ReadLab, cut: int) -> None:
    assert len(lab.process.calls) == cut
    for (argv, options), (service, operation, region, payload) in zip(
        lab.process.calls, TRACE[:cut], strict=True
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
        assert json.loads(options["input"]) == payload
        assert options["timeout"] == 7
        assert options["shell"] is False
        assert options["capture_output"] is True
        assert options["text"] is True
        assert options["check"] is False
        assert_environment(options["env"], "us-east-2")
    assert lab.context.budget.used == 40
    assert lab.context.budget.work_used == 40
    assert lab.process.observed_debits == list(range(41 - cut, 41))
    assert lab.process.outcomes == []


@pytest.mark.parametrize("cut", [0, 1, 2])
def test_reserva_interrompe_antes_do_proximo_readback(read_lab: ReadLab, cut: int) -> None:
    for _ in range(40 - cut):
        read_lab.context.budget.acquire("work")
    enqueue(read_lab, *("root", "tags")[:cut])
    with pytest.raises(BudgetExhausted) as error:
        read_lab.services.lookup(CLIENT, read_lab.context)
    assert error.value.args == ("budget=requests",)
    assert_private(error.value, read_lab, "")
    _assert_reserved_trace(read_lab, cut)
    assert read_lab.context.budget.limits == Limits()


def _private_client(lab: ReadLab) -> tuple[Resource, list[tuple]]:
    lab.process.outcomes.append((0, json.dumps(DATA["private_ids"]["root"]), ""))
    resource = Resource(**DATA["private_ids"]["resource"])
    return resource, [tuple(call) for call in DATA["private_ids"]["trace"]]


@pytest.mark.parametrize("phase", ["tags", "client", "message"])
def test_readback_client_nao_publica_material_privado(
    read_lab: ReadLab,
    phase: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    resource, trace = _private_client(read_lab)
    tags = dict(DATA["replies"]["tags"]["Tags"])
    tags["Run" if phase == "tags" else "Private"] = CANARY
    read_lab.process.outcomes.append((0, json.dumps({"Tags": tags}), ""))
    if phase == "message":
        enqueue_error(read_lab, "denied")
    elif phase == "client":
        read_lab.process.outcomes.append(
            (
                0,
                json.dumps(
                    {
                        "UserPoolClient": {
                            "ClientId": "otherclient",
                            "UserPoolId": "us-east-2_R1PRIVATECANARY",
                            "ClientSecret": CANARY,
                        }
                    }
                ),
                "",
            )
        )
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource, read_lab.context)
    expected = {
        "tags": "service=not_owned",
        "client": "service=describe_invalid",
        "message": "service=describe_failed",
    }
    assert error.value.args == (expected[phase],)
    _assert_channels(error.value, read_lab, _channels(caplog, capsys))
    assert_process(read_lab, trace[: 2 if phase == "tags" else 3])


def test_client_secret_presente_nao_e_requisito_nem_canal_publico(
    read_lab: ReadLab, caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    enqueue(read_lab, "root", "tags")
    client = dict(DATA["replies"]["client"]["UserPoolClient"], ClientSecret=CANARY)
    read_lab.process.outcomes.append((0, json.dumps({"UserPoolClient": client}), ""))
    assert_owner(read_lab.services.lookup(CLIENT, read_lab.context))
    _assert_channels(None, read_lab, _channels(caplog, capsys))
    assert_process(read_lab, TRACE)
