"""Sucesso vazio exige par DynamoDB exato, orçamento e fronteira CLI preservados."""

import json
import logging
import os
import subprocess
import traceback
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.integration.aws_capability._cli_testing import PRIVATE_INPUT
from tests.integration.aws_capability._service_contracts import Reply
from tests.integration.aws_capability._service_testing import (
    CANARIES,
    CliLab,
    ProcessScript,
    make_cli_lab,
)
from tests.integration.aws_capability.cli import CliFailure, Request
from tests.integration.aws_capability.contracts import BudgetExhausted, Limits

TRANSACAO = (
    '{"TransactItems": [{"Put": {"TableName": "cnesdata-cap-a1b2c3d4", '
    '"Item": {"pk": {"S": "cap#a1b2c3d4"}, "sk": {"S": "token"}, '
    '"version": {"N": "1"}}, "ConditionExpression": '
    '"attribute_not_exists(#pk) AND attribute_not_exists(#sk)", '
    '"ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk"}}}], '
    '"ClientRequestToken": "a1b2c3d4-token"}'
)
LEITURA = (
    '{"TableName": "cnesdata-cap-a1b2c3d4", "Key": {"pk": {"S": "cap#a1b2c3d4"}, '
    '"sk": {"S": "token"}}, "ConsistentRead": true}'
)


@pytest.fixture(autouse=True)
def privacidade(
    caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> Iterator[None]:
    caplog.set_level(logging.DEBUG)
    yield
    captured = capsys.readouterr()
    assert all(secret not in caplog.text + captured.out + captured.err for secret in CANARIES)
    assert list(tmp_path.rglob("*")) == []


@pytest.fixture
def cli_lab(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, request: pytest.FixtureRequest
) -> Iterator[CliLab]:
    lab = make_cli_lab(tmp_path)
    if getattr(request, "param", "") == "expirado":
        lab.cli.context.budget.now = lambda: 1060
    original = lab.cli.context
    budget = original.budget
    environment = dict(lab.cli.environment)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(subprocess, "run", lab.process.run)
    yield lab
    assert lab.cli.context is original
    assert lab.cli.context.budget is budget is lab.process.budget
    assert original.lane == "work"
    assert budget.deadline == 1060
    assert budget.limits == Limits(80, 8, 2, 30, 40)
    assert budget.cleanup_deadline is None
    assert budget.limits.calls - budget.used >= budget.limits.cleanup_reserve == 40
    assert lab.cli.environment == environment
    assert lab.cli.timeout == 7


@pytest.fixture(params=["transact-write-items", "get-item"], ids=["transacao", "leitura"])
def requisicao(request: pytest.FixtureRequest) -> Request:
    payload = TRANSACAO if request.param == "transact-write-items" else LEITURA
    return Request("dynamodb", request.param, json.loads(payload))


def _ambiente(lab: CliLab) -> None:
    assert lab.process.calls[0][1]["env"] == {
        "AWS_ACCESS_KEY_ID": "dummy",
        "AWS_SECRET_ACCESS_KEY": "dummy",
        "AWS_SESSION_TOKEN": "dummy",
        "AWS_CLI_ERROR_FORMAT": "legacy",
        "AWS_MAX_ATTEMPTS": "1",
        "AWS_REGION": "us-east-2",
        "AWS_DEFAULT_REGION": "us-east-2",
        "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "true",
        "AWS_PAGER": "",
        "AWS_CLI_AUTO_PROMPT": "off",
        "AWS_CONFIG_FILE": os.devnull,
        "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
        "AWS_EC2_METADATA_DISABLED": "true",
    }


def _rastro(lab: CliLab, request: Request) -> None:
    assert len(lab.process.calls) == 1
    argv, options = lab.process.calls[0]
    assert argv == [
        "aws",
        request.service,
        request.operation,
        "--region",
        "us-east-2",
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
        "7",
        "--cli-read-timeout",
        "7",
    ]
    expected = {
        ("dynamodb", "transact-write-items"): TRANSACAO,
        ("dynamodb", "get-item"): LEITURA,
    }.get((request.service, request.operation), "{}")
    assert options["input"] == expected
    assert json.loads(options["input"]) == request.payload
    assert options["timeout"] == 7
    assert options["shell"] is False
    assert options["check"] is False
    assert options["capture_output"] is True
    assert options["text"] is True
    _ambiente(lab)
    assert lab.cli.context.budget.used == lab.cli.context.budget.work_used == 1
    assert lab.process.observed_debits == [1]
    assert lab.process.outcomes == []


def _publico(value: Reply | BaseException) -> None:
    evidence = str(value) + repr(value)
    if isinstance(value, BaseException):
        evidence += "".join(traceback.format_exception(value))
    else:
        evidence += repr(value.error)
    assert all(secret not in evidence for secret in CANARIES)


def _recusa(reply: Reply, code: str, origin: str = "local") -> None:
    _publico(reply)
    assert reply.ok is False
    assert reply.data == {}
    assert reply.error is not None
    assert (reply.error.code, reply.error.origin) == (code, origin)


def _sem_io(lab: CliLab, used: int) -> None:
    assert lab.process.calls == []
    assert lab.process.observed_debits == []
    assert lab.process.outcomes == [(0, "", "")]
    assert lab.cli.context.budget.used == lab.cli.context.budget.work_used == used


@pytest.mark.parametrize("saida", ["", " \t\r\n", "{}"], ids=["vazio", "brancos", "objeto"])
@pytest.mark.parametrize("metodo", ["call", "invoke"])
def test_sucesso_vazio_admite_apenas_os_pares_dynamodb(
    cli_lab: CliLab, requisicao: Request, metodo: str, saida: str
) -> None:
    cli_lab.respond((0, saida, ""))
    try:
        result = getattr(cli_lab.cli, metodo)(requisicao)
    except CliFailure as error:
        result = error
        _publico(error)
    _rastro(cli_lab, requisicao)
    if metodo == "call":
        assert isinstance(result, Reply)
        _publico(result)
        assert (result.ok, result.data, result.error) == (True, {}, None), (
            result.error.code if result.error is not None else None
        )
    else:
        assert result == {}


@pytest.mark.parametrize(
    ("servico", "operacao"),
    [
        ("dynamodb", "transact-get-items"),
        ("s3api", "transact-write-items"),
        ("sts", "get-caller-identity"),
    ],
)
def test_stdout_vazio_recusa_outros_pares(cli_lab: CliLab, servico: str, operacao: str) -> None:
    request = Request(servico, operacao, {})
    cli_lab.respond((0, "", ""))
    reply = cli_lab.cli.call(request)
    _rastro(cli_lab, request)
    _recusa(reply, "operation_failed")


@pytest.mark.parametrize(
    ("saida", "codigo"),
    [
        ("[]", "invalid_json"),
        ("null", "invalid_json"),
        ("not-json-" + CANARIES[0], "operation_failed"),
    ],
    ids=["array", "null", "malformado_privado"],
)
def test_json_nao_objeto_ou_malformado_permanece_falha(
    cli_lab: CliLab, requisicao: Request, saida: str, codigo: str
) -> None:
    cli_lab.respond((0, saida, CANARIES[2]))
    reply = cli_lab.cli.call(requisicao)
    _rastro(cli_lab, requisicao)
    _recusa(reply, codigo)
    assert reply.error is not None
    assert reply.error.details == {}


@pytest.mark.parametrize(("status", "origem"), [(252, "local"), (254, "service")])
def test_status_nao_zero_preserva_erro_modelado_e_origem(
    cli_lab: CliLab, requisicao: Request, status: int, origem: str
) -> None:
    details = {"Code": "IdempotentParameterMismatchException", "Message": CANARIES[2]}
    cli_lab.respond((status, "", json.dumps(details)))
    reply = cli_lab.cli.call(requisicao)
    _rastro(cli_lab, requisicao)
    _recusa(reply, "IdempotentParameterMismatchException", origem)
    assert reply.error is not None
    assert reply.error.details == {
        "Code": "IdempotentParameterMismatchException",
        "Message": "B1_STDERR_CANARY",
    }


def test_timeout_com_corpos_privados_permanece_falha(cli_lab: CliLab, requisicao: Request) -> None:
    cli_lab.respond(subprocess.TimeoutExpired("aws", 7, CANARIES[0], CANARIES[2]))
    reply = cli_lab.cli.call(requisicao)
    _rastro(cli_lab, requisicao)
    _recusa(reply, "operation_failed")


def test_teto_work_precarregado_preserva_reserva_sem_io(
    cli_lab: CliLab, requisicao: Request
) -> None:
    budget = cli_lab.cli.context.budget
    for _ in range(40):
        budget.acquire("work")
    cli_lab.respond((0, "", ""))
    with pytest.raises(BudgetExhausted, match=r"^budget=requests$") as caught:
        cli_lab.cli.call(requisicao)
    _publico(caught.value)
    _sem_io(cli_lab, 40)
    assert budget.limits.calls - budget.used == budget.limits.cleanup_reserve == 40


@pytest.mark.parametrize("cli_lab", ["expirado"], indirect=True)
def test_deadline_original_expirado_recusa_antes_de_io(
    cli_lab: CliLab, requisicao: Request
) -> None:
    budget = cli_lab.cli.context.budget
    assert budget.deadline == 1060
    assert budget.remaining_seconds("work") == 0
    cli_lab.respond((0, "", ""))
    with pytest.raises(BudgetExhausted, match=r"^budget=deadline$") as caught:
        cli_lab.cli.call(requisicao)
    _publico(caught.value)
    _sem_io(cli_lab, 0)


@pytest.mark.parametrize("saida", ["", " \t\r\n"], ids=["vazio", "brancos"])
def test_controle_gravador_preserva_bytes_vazios_sem_fabricar_json(saida: str) -> None:
    process = ProcessScript(outcomes=[(0, saida, "")])
    result = process.run(["aws", "dynamodb", "get-item"], check=False)
    assert (result.returncode, result.stdout, result.stderr) == (0, saida, "")
    assert result.stdout.encode("utf-8") == saida.encode("utf-8")
    assert process.outcomes == []
    assert process.observed_debits == []
    assert len(process.calls) == 1
