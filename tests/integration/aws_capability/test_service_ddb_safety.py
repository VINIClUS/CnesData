"""Orçamento e sanitização da leitura AWS sem mutações ou retries ocultos."""

import json
import subprocess
import traceback
from dataclasses import replace
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_read_assertions import (
    ExpectedCall,
    assert_private,
    assert_process,
    assert_rejected,
)
from tests.integration.aws_capability._service_read_testing import (
    CANARY,
    ROOT,
    ReadLab,
    fixture_data,
    make_read_lab,
    respond,
    respond_error,
)
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    RequestContext,
    ServiceFailure,
)

TABLE_ARN = "arn:aws:dynamodb:us-east-2:111111111111:table/cnesdata-cap-a1b2c3d4"
DESCRIBE = ("dynamodb", "describe-table", "us-east-2", {"TableName": "cnesdata-cap-a1b2c3d4"})
TAGS = ("dynamodb", "list-tags-of-resource", "us-east-2", {"ResourceArn": TABLE_ARN})


@pytest.fixture
def read_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ReadLab:
    return make_read_lab(monkeypatch, tmp_path)


def test_reserva_impede_terceira_request_de_ownership(read_lab: ReadLab) -> None:
    read_lab.context.budget.limits = replace(
        read_lab.context.budget.limits, calls=4, cleanup_reserve=2
    )
    respond(read_lab, fixture_data("ownership")["describe"])
    respond(read_lab, fixture_data("ownership")["tags_first"])
    with pytest.raises(BudgetExhausted):
        read_lab.services.lookup(ROOT, read_lab.context)
    assert_process(read_lab, [DESCRIBE, TAGS])
    assert read_lab.context.budget.work_used == 2


def test_deadline_expirado_impede_primeiro_processo(read_lab: ReadLab) -> None:
    read_lab.context.budget.deadline = 1000
    with pytest.raises(BudgetExhausted):
        read_lab.services.identity(read_lab.context)
    assert_process(read_lab, [])


def test_timeout_do_cli_e_limitado_pelo_deadline_compartilhado(read_lab: ReadLab) -> None:
    read_lab.context.budget.deadline = 1002
    respond(read_lab, {"TableNames": []})
    page = read_lab.services.inventory("dynamodb", None, read_lab.context)
    assert page.resources == ()
    assert page.cursor is None
    assert_process(read_lab, [("dynamodb", "list-tables", "us-east-2", {"Limit": 100})], 2)


def test_contexto_cleanup_nao_debita_lane_de_trabalho(read_lab: ReadLab) -> None:
    read_lab.context = RequestContext(read_lab.context.budget, "cleanup")
    respond(read_lab, {"TableNames": []})
    page = read_lab.services.inventory("dynamodb", None, read_lab.context)
    assert page.resources == ()
    assert page.cursor is None
    assert_process(read_lab, [("dynamodb", "list-tables", "us-east-2", {"Limit": 100})])
    assert read_lab.context.budget.work_used == 0


def test_limite_de_paginas_de_tags_interrompe_antes_de_nova_request(read_lab: ReadLab) -> None:
    read_lab.context.budget.limits = replace(read_lab.context.budget.limits, pages=2)
    respond(read_lab, fixture_data("ownership")["describe"])
    respond(read_lab, fixture_data("ownership")["tags_first"])
    respond(read_lab, {"Tags": [], "NextToken": "tags-after"})
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(ROOT, read_lab.context)
    assert_rejected(error.value)
    assert_process(
        read_lab,
        [
            DESCRIBE,
            TAGS,
            (
                "dynamodb",
                "list-tags-of-resource",
                "us-east-2",
                {
                    "ResourceArn": TABLE_ARN,
                    "NextToken": "tags-next",
                },
            ),
        ],
    )


@pytest.mark.parametrize("failure", ["remote_denied", "bad_json", "bad_stderr"])
def test_falhas_nao_expoem_material_privado_nos_canais_publicos(
    read_lab: ReadLab,
    failure: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respond_error(read_lab, failure)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert_rejected(error.value)
    public = "".join(traceback.format_exception(error.value)) + caplog.text
    captured = capsys.readouterr()
    public += captured.out + captured.err
    public += repr(read_lab.services) + repr(read_lab.services.ports)
    public += repr(read_lab.services.ports.commands)
    assert CANARY not in public
    assert TABLE_ARN not in public
    assert_private(error.value, read_lab, public)
    assert list(read_lab.directory.iterdir()) == []
    assert_process(read_lab, [DESCRIBE])


@pytest.mark.parametrize("failure", ["timeout", "oserror"])
def test_transporte_sanitiza_causa_sem_retry_oculto(
    read_lab: ReadLab,
    failure: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    outcome = (
        subprocess.TimeoutExpired("aws", 7, output=CANARY, stderr=CANARY)
        if failure == "timeout"
        else OSError(CANARY)
    )
    read_lab.process.outcomes.append(outcome)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert_rejected(error.value)
    assert CANARY not in "".join(traceback.format_exception(error.value))
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [DESCRIBE])


@pytest.mark.parametrize("case", ["extra_field", "other_marker", "extra_marker"])
def test_cursor_local_com_canary_e_recusado_sem_io_ou_vazamento(
    read_lab: ReadLab,
    case: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cursor = json.dumps(fixture_data("cursors")["invalid"][case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("dynamodb", cursor, read_lab.context)
    assert_rejected(error.value)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [])


@pytest.mark.parametrize("case", ["private_arn", "private_name", "private_tag"])
def test_reply_invalida_com_canary_e_recusada_sem_vazamento(
    read_lab: ReadLab,
    case: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    expected: list[ExpectedCall] = [DESCRIBE]
    if case == "private_tag":
        respond(read_lab, fixture_data("ownership")["describe"])
        expected.append(TAGS)
    respond(read_lab, fixture_data("ownership")[case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(ROOT, read_lab.context)
    assert_rejected(error.value)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, expected)
