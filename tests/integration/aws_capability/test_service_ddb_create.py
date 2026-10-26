"""CreateTable devolve identidade aceita sem antecipar prontidão ou recuperação."""

import json
import logging
import subprocess
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

from tests.integration.aws_capability._service_read_assertions import (
    assert_private,
    assert_process,
    assert_rejected,
)
from tests.integration.aws_capability._service_read_testing import (
    CANARY,
    ReadLab,
    make_read_lab,
    respond,
)
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Creation,
    RequestContext,
    Resource,
    ServiceFailure,
)

ROOT = Resource("dynamodb", "cnesdata-cap-a1b2c3d4", "cnesdata-cap-a1b2c3d4")


@pytest.fixture
def read_lab(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> ReadLab:
    caplog.set_level(logging.DEBUG)
    return make_read_lab(monkeypatch, tmp_path)


def _fixture() -> Any:
    path = Path(__file__).parent / "fixtures" / "cli" / "b3" / "ddb_create.json"
    return json.loads(path.read_text())


def _creation(lab: ReadLab) -> Creation:
    return Creation(
        lab.services.spec,
        "cnesdata-cap-a1b2c3d4",
        {
            "Project": "CnesData",
            "Purpose": "aws-capability",
            "Run": "a1b2c3d4",
            "ExpiresAt": "1060",
        },
    )


def _assert_create(lab: ReadLab, payload: Mapping[str, object], timeout: float = 7) -> None:
    assert_process(lab, [("dynamodb", "create-table", "us-east-2", payload)], timeout)
    assert set(lab.process.calls[0][1]) == {
        "input",
        "capture_output",
        "text",
        "timeout",
        "check",
        "shell",
        "env",
    }
    assert Path.cwd() == lab.directory
    assert list(lab.directory.iterdir()) == []


def test_cria_root_com_payload_literal_e_retorna_identidade_creating(read_lab: ReadLab) -> None:
    creation = _creation(read_lab)
    assert creation.spec is read_lab.services.spec
    respond(read_lab, _fixture()["creating"])
    resource = read_lab.services.create("dynamodb", creation, read_lab.context)
    _assert_create(read_lab, _fixture()["request"])
    assert resource == ROOT
    assert read_lab.context.budget.work_used == 1


def test_aceita_active_sem_configurar_ou_esperar(read_lab: ReadLab) -> None:
    creation = replace(_creation(read_lab), spec=replace(read_lab.services.spec))
    assert creation.spec == read_lab.services.spec
    assert creation.spec is not read_lab.services.spec
    respond(read_lab, _fixture()["active"])
    resource = read_lab.services.create("dynamodb", creation, read_lab.context)
    _assert_create(read_lab, _fixture()["request"])
    assert resource == ROOT
    assert resource.resource_id != "synthetic-table-id"
    assert read_lab.context.budget.work_used == 1


def test_preserva_deadline_fracionario_nas_tags(read_lab: ReadLab) -> None:
    read_lab.services = replace(
        read_lab.services, spec=replace(read_lab.services.spec, deadline=1060.25)
    )
    creation = _creation(read_lab)
    creation = replace(creation, tags={**creation.tags, "ExpiresAt": "1060.25"})
    expected = _fixture()["request"]
    expected["Tags"] = [
        {"Key": "Project", "Value": "CnesData"},
        {"Key": "Purpose", "Value": "aws-capability"},
        {"Key": "Run", "Value": "a1b2c3d4"},
        {"Key": "ExpiresAt", "Value": "1060.25"},
    ]
    respond(read_lab, _fixture()["creating"])
    resource = read_lab.services.create("dynamodb", creation, read_lab.context)
    _assert_create(read_lab, expected)
    assert resource == ROOT
    assert read_lab.context.budget.work_used == 1


def _divergent(creation: Creation, case: str) -> Creation:
    changes: dict[str, object] = {
        "account": "222222222222",
        "region": "us-west-1",
        "run_id": "b1c2d3e4",
        "deadline": 1060.25,
        "journal_dir": creation.spec.journal_dir / "other",
        "retention": "COMPLIANCE",
        "allow_governance_bypass": True,
        "probe": "none",
    }
    if case in changes:
        return replace(creation, spec=replace(creation.spec, **cast("Any", {case: changes[case]})))
    if case == "nome_sufixo":
        return replace(creation, name="cnesdata-cap-a1b2c3d4-suffix")
    tags = dict(creation.tags)
    if case == "tag_ausente":
        del tags["Project"]
    elif case == "tag_extra":
        tags["Extra"] = CANARY
    elif case == "tag_tipo":
        tags["ExpiresAt"] = cast("str", 1060)
    else:
        tags["Run"] = CANARY
    return replace(creation, tags=tags)


@pytest.mark.parametrize(
    "case",
    [
        "account",
        "region",
        "run_id",
        "deadline",
        "journal_dir",
        "retention",
        "allow_governance_bypass",
        "probe",
        "nome_sufixo",
        "tag_ausente",
        "tag_extra",
        "tag_tipo",
        "tag_valor",
    ],
)
def test_rejeita_creation_divergente_antes_de_io(
    read_lab: ReadLab,
    case: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    creation = _divergent(_creation(read_lab), case)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.create("dynamodb", creation, read_lab.context)
    assert_process(read_lab, [])
    assert read_lab.context.budget.work_used == 0
    assert_rejected(error.value)
    assert error.value.args == ("service=creation_invalid",)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)


def _ambiguous_reply(case: str) -> object:
    if case == "table_description_ausente":
        return {}
    if case == "table_description_lista":
        return {"TableDescription": []}
    if case == "table_description_null":
        return {"TableDescription": None}
    table = _fixture()["creating"]["TableDescription"]
    changes = {
        "nome": ("TableName", "cnesdata-cap-ffffffff"),
        "arn_servico": (
            "TableArn",
            "arn:aws:s3:us-east-2:111111111111:table/cnesdata-cap-a1b2c3d4",
        ),
        "arn_conta": (
            "TableArn",
            "arn:aws:dynamodb:us-east-2:222222222222:table/cnesdata-cap-a1b2c3d4",
        ),
        "arn_regiao": (
            "TableArn",
            "arn:aws:dynamodb:us-west-1:111111111111:table/cnesdata-cap-a1b2c3d4",
        ),
        "arn_nome": (
            "TableArn",
            "arn:aws:dynamodb:us-east-2:111111111111:table/cnesdata-cap-ffffffff",
        ),
        "status_deleting": ("TableStatus", "DELETING"),
        "status_updating": ("TableStatus", "UPDATING"),
        "status_desconhecido": ("TableStatus", CANARY),
    }
    if case == "status_ausente":
        del table["TableStatus"]
    else:
        key, value = changes[case]
        table[key] = value
    return {"TableDescription": table}


@pytest.mark.parametrize(
    "case",
    [
        "table_description_ausente",
        "table_description_lista",
        "table_description_null",
        "nome",
        "arn_servico",
        "arn_conta",
        "arn_regiao",
        "arn_nome",
        "status_ausente",
        "status_deleting",
        "status_updating",
        "status_desconhecido",
    ],
)
def test_resposta_ambigua_nao_fabrica_root_nem_repete_create(
    read_lab: ReadLab,
    case: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respond(read_lab, _ambiguous_reply(case))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.create("dynamodb", _creation(read_lab), read_lab.context)
    _assert_create(read_lab, _fixture()["request"])
    assert_rejected(error.value)
    assert error.value.args == ("service=create_invalid",)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)


@pytest.mark.parametrize(
    "case",
    [
        "timeout",
        "oserror",
        "access_denied",
        "resource_in_use",
        "internal_error",
        "invalid_json",
        "empty_stdout",
    ],
)
def test_erro_remoto_ou_resposta_perdida_preserva_incerteza(
    read_lab: ReadLab,
    case: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    outcome: tuple[int, str, str] | BaseException
    if case == "timeout":
        outcome = subprocess.TimeoutExpired("aws", 7, output=CANARY, stderr=CANARY)
    elif case == "oserror":
        outcome = OSError(CANARY)
    else:
        code, stdout, stderr = _fixture()["failures"][case]
        outcome = (code, stdout, stderr)
    read_lab.process.outcomes.append(outcome)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.create("dynamodb", _creation(read_lab), read_lab.context)
    _assert_create(read_lab, _fixture()["request"])
    assert_rejected(error.value)
    assert error.value.args == ("service=request_failed",)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)


@pytest.mark.parametrize("case", ["reserva_work", "deadline_esgotado"])
def test_orcamento_impede_primeiro_create_sem_debito(read_lab: ReadLab, case: str) -> None:
    budget = read_lab.context.budget
    if case == "reserva_work":
        budget.limits = replace(budget.limits, calls=2, cleanup_reserve=2)
    else:
        budget.deadline = 1000
    with pytest.raises(BudgetExhausted) as error:
        read_lab.services.create("dynamodb", _creation(read_lab), read_lab.context)
    assert_process(read_lab, [])
    assert budget.work_used == 0
    assert error.value.args == ("budget=requests" if case == "reserva_work" else "budget=deadline",)


def test_timeout_do_create_limitado_pelo_saldo_work(read_lab: ReadLab) -> None:
    read_lab.context.budget.deadline = 1002
    respond(read_lab, _fixture()["creating"])
    resource = read_lab.services.create("dynamodb", _creation(read_lab), read_lab.context)
    _assert_create(read_lab, _fixture()["request"], 2)
    assert resource == ROOT
    assert read_lab.context.budget.work_used == 1


def test_create_cleanup_usa_deadline_proprio_sem_debitar_work(read_lab: ReadLab) -> None:
    budget = read_lab.context.budget
    budget.deadline = 999
    budget.limits = replace(budget.limits, cleanup_seconds=2)
    read_lab.context = RequestContext(budget, "cleanup")
    respond(read_lab, _fixture()["creating"])
    resource = read_lab.services.create("dynamodb", _creation(read_lab), read_lab.context)
    _assert_create(read_lab, _fixture()["request"], 2)
    assert resource == ROOT
    assert budget.work_used == 0
    assert budget.cleanup_deadline == 1002
    assert budget.deadline == 999


def test_create_cognito_preserva_stub_sem_consumir_resposta_dynamodb(read_lab: ReadLab) -> None:
    creation = _creation(read_lab)
    respond(read_lab, _fixture()["creating"])
    queued = list(read_lab.process.outcomes)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.create("cognito", creation, read_lab.context)
    assert error.value.args == ("service=not_implemented",)
    assert read_lab.process.calls == []
    assert read_lab.process.observed_debits == []
    assert read_lab.context.budget.used == 0
    assert read_lab.context.budget.work_used == 0
    assert read_lab.process.outcomes == queued
    assert len(read_lab.process.outcomes) == 1
