"""DeleteTable exige autorização fresca e aceitação causal sem repetir mutações."""

import json
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._service_ddb_prepare_testing import Privacy
from tests.integration.aws_capability._service_read_assertions import assert_private, assert_process
from tests.integration.aws_capability._service_read_testing import ReadLab, make_read_lab, respond
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    RequestContext,
    Resource,
    ServiceFailure,
)

ROOT = Resource("dynamodb", "cnesdata-cap-a1b2c3d4", "cnesdata-cap-a1b2c3d4")
CANARY = "R1_PRIVATE_CANARY"

LEGACY = (
    b'{"schema_version": 1, "account": "111111111111", "region": "us-east-2", '
    b'"run_id": "a1b2c3d4", "deadline": 1060, "phase": "cleanup_requested", '
    b'"baseline": [], "created": [{"service": "dynamodb", '
    b'"resource_id": "cnesdata-cap-a1b2c3d4", "name": "cnesdata-cap-a1b2c3d4"}], '
    b'"pending_service": null, "operation_state": "confirmed"}'
)
PROFILE = (
    b', "probe": "ddb-transactions", "profile_version": 1, "limits": '
    b'{"calls": 192, "pages": 8, "attempts": 2, "cleanup_seconds": 60, "cleanup_reserve": 96}}'
)


def fixture() -> Any:
    path = Path(__file__).parent / "fixtures" / "cli" / "b3" / "ddb_delete.json"
    return json.loads(path.read_text())


@pytest.fixture
def delete_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ReadLab:
    directory = tmp_path / "private"
    prepare_directory(directory)
    lab = make_read_lab(monkeypatch, directory)
    lab.context = RequestContext(lab.context.budget, "cleanup")
    return lab


@pytest.fixture
def privacy(caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]) -> Privacy:
    return Privacy(caplog, capsys)


def queue(lab: ReadLab, steps: list[list[str]]) -> None:
    data = fixture()
    for _, reply in steps:
        if reply in data["failures"]:
            code, stdout, stderr = data["failures"][reply]
            lab.process.outcomes.append((code, stdout, stderr))
        else:
            respond(lab, data["replies"][reply])


def trace(lab: ReadLab, names: list[str], tail: int = 0) -> None:
    pending = list(lab.process.outcomes)
    assert len(pending) == tail
    lab.process.outcomes.clear()
    assert_process(lab, [tuple(fixture()["requests"][name]) for name in names])
    lab.process.outcomes.extend(pending)
    assert lab.context.budget.work_used == 0


def adapter(lab: ReadLab, protected: bool = True) -> list[str]:
    steps = fixture()["adapter_protected" if protected else "adapter_unprotected"]
    queue(lab, steps)
    return [name for name, _ in steps]


def test_aceita_delete_protegido_com_seis_requests_literais(delete_lab: ReadLab) -> None:
    names = adapter(delete_lab)
    assert delete_lab.services.delete(ROOT, False, delete_lab.context) is None
    trace(delete_lab, names)
    assert delete_lab.context.budget.used == 6
    payload = json.loads(delete_lab.process.calls[2][1]["input"])
    assert payload["DeletionProtectionEnabled"] is False
    assert type(payload["DeletionProtectionEnabled"]) is bool
    assert list(delete_lab.directory.iterdir()) == []


@pytest.mark.parametrize("bypass", [False, True], ids=["false", "true"])
def test_aceita_delete_desprotegido_sem_update(delete_lab: ReadLab, bypass: bool) -> None:
    names = adapter(delete_lab, False)
    assert delete_lab.services.delete(ROOT, bypass, delete_lab.context) is None
    trace(delete_lab, names)
    assert delete_lab.context.budget.used == 3


@pytest.mark.parametrize("case", ["lane", "nome", "id", "bypass_zero", "bypass_um"])
def test_rejeita_entrada_local_sem_io_ou_debito(delete_lab: ReadLab, case: str) -> None:
    root, context = ROOT, delete_lab.context
    bypass: Any = False
    if case == "lane":
        context = RequestContext(context.budget, "work")
    elif case == "nome":
        root = Resource("dynamodb", "cnesdata-cap-ffffffff", "cnesdata-cap-ffffffff")
    elif case == "id":
        root = Resource("dynamodb", "foreign-id", "cnesdata-cap-a1b2c3d4")
    else:
        bypass = json.loads("0" if case == "bypass_zero" else "1")
        assert type(bypass) is int
    adapter(delete_lab)
    with pytest.raises(ServiceFailure) as error:
        delete_lab.services.delete(root, cast("bool", bypass), context)
    assert error.value.args == ("service=ddb_delete_invalid",)
    trace(delete_lab, [], 6)


@pytest.mark.parametrize("service", ["cognito", "foreign"])
def test_preserva_stub_de_outros_servicos_sem_io(delete_lab: ReadLab, service: str) -> None:
    adapter(delete_lab)
    with pytest.raises(ServiceFailure) as error:
        delete_lab.services.delete(replace(ROOT, service=service), True, delete_lab.context)
    assert error.value.args == ("service=not_implemented",)
    trace(delete_lab, [], 6)


@pytest.mark.parametrize(
    "case", ["nome", "arn", "zero", "um", "creating", "updating", "deleting", "ausente"]
)
def test_rejeita_identidade_estado_e_booleano_inicial(
    delete_lab: ReadLab, case: str, privacy: Privacy
) -> None:
    data = fixture()["replies"]["protected"]
    changes = {
        "nome": ("TableName", CANARY),
        "arn": ("TableArn", CANARY),
        "zero": ("DeletionProtectionEnabled", json.loads("0")),
        "um": ("DeletionProtectionEnabled", json.loads("1")),
        "creating": ("TableStatus", "CREATING"),
        "updating": ("TableStatus", "UPDATING"),
        "deleting": ("TableStatus", "DELETING"),
    }
    if case == "ausente":
        del data["Table"]["DeletionProtectionEnabled"]
    else:
        key, value = changes[case]
        data["Table"][key] = value
    respond(delete_lab, data)
    adapter(delete_lab)
    with pytest.raises(ServiceFailure) as error:
        delete_lab.services.delete(ROOT, False, delete_lab.context)
    assert error.value.args == ("service=ddb_delete_state",)
    trace(delete_lab, ["D"], 6)
    privacy.check(error.value, delete_lab)


@pytest.mark.parametrize("stage", ["update", "readback"])
@pytest.mark.parametrize("case", ["zero", "true", "nome", "estado"])
def test_rejeita_echo_e_readback_sem_booleano_false_estrito(
    delete_lab: ReadLab, stage: str, case: str
) -> None:
    queue(delete_lab, [["D", "protected"], ["T", "owner"]])
    if stage == "readback":
        queue(delete_lab, [["U", "update"]])
    data = fixture()["replies"]["update" if stage == "update" else "unprotected"]
    table = data["TableDescription" if stage == "update" else "Table"]
    changes = {
        "zero": ("DeletionProtectionEnabled", json.loads("0")),
        "true": ("DeletionProtectionEnabled", True),
        "nome": ("TableName", CANARY),
        "estado": ("TableStatus", "DELETING" if stage == "update" else "UPDATING"),
    }
    key, value = changes[case]
    table[key] = value
    respond(delete_lab, data)
    adapter(delete_lab)
    with pytest.raises(ServiceFailure) as error:
        delete_lab.services.delete(ROOT, True, delete_lab.context)
    assert error.value.args == ("service=ddb_delete_state",)
    trace(delete_lab, ["D", "T", "U"] + (["D"] if stage == "readback" else []), 6)


@pytest.mark.parametrize("case", ["drift", "duplicada", "malformada", "pagina_falha"])
def test_ownership_fresco_apos_update_impede_delete(delete_lab: ReadLab, case: str) -> None:
    queue(delete_lab, fixture()["adapter_protected"][:4])
    owner = fixture()["replies"]["owner"]
    if case == "drift":
        owner["Tags"][2]["Value"] = CANARY
        respond(delete_lab, owner)
        names, expected = ["D", "T", "U", "D", "T"], "ddb_delete_owner"
    else:
        queue(delete_lab, [["T", "first_page"]])
        data = fixture()["replies"]["second_page"]
        if case == "duplicada":
            data["Tags"].append({"Key": "Project", "Value": "CnesData"})
            respond(delete_lab, data)
            expected = "tags_ambiguous"
        elif case == "malformada":
            respond(delete_lab, {"Tags": [{"Key": "Run", "Value": 1}]})
            expected = "tags_invalid"
        else:
            queue(delete_lab, [["P", "denied"]])
            expected = "request_failed"
        names = ["D", "T", "U", "D", "T", "P"]
    adapter(delete_lab)
    with pytest.raises(ServiceFailure) as error:
        delete_lab.services.delete(ROOT, False, delete_lab.context)
    assert error.value.args == ("service=" + expected,)
    trace(delete_lab, names, 6)


def test_paginas_frescas_e_expiracao_fracionaria_antes_de_delete(delete_lab: ReadLab) -> None:
    delete_lab.services = replace(
        delete_lab.services, spec=replace(delete_lab.services.spec, deadline=1060.25)
    )
    for reply in [
        "protected",
        "first_page",
        "second_page",
        "update",
        "unprotected",
        "first_page",
        "second_page",
        "delete",
    ]:
        data = fixture()["replies"][reply]
        if reply == "second_page":
            data["Tags"][1]["Value"] = "1060.25"
        respond(delete_lab, data)
    assert delete_lab.services.delete(ROOT, False, delete_lab.context) is None
    trace(delete_lab, ["D", "T", "P", "U", "D", "T", "P", "X"])


@pytest.mark.parametrize(
    ("stage", "failure"),
    [
        ("describe", "rnf"),
        ("describe", "in_use"),
        ("update", "timeout"),
        ("update", "object"),
        ("delete", "timeout"),
        ("delete", "oserror"),
        ("delete", "rnf"),
        ("delete", "in_use"),
        ("delete", "empty"),
        ("delete", "object"),
    ],
)
def test_falha_ambigua_para_sem_retry_com_cola_positiva(
    delete_lab: ReadLab, stage: str, failure: str, privacy: Privacy
) -> None:
    prefix = {
        "describe": [],
        "update": [["D", "protected"], ["T", "owner"]],
        "delete": [["D", "unprotected"], ["T", "owner"]],
    }[stage]
    queue(delete_lab, prefix)
    if failure == "timeout":
        delete_lab.process.outcomes.append(
            subprocess.TimeoutExpired("aws", 7, output=CANARY, stderr=CANARY)
        )
    elif failure == "oserror":
        delete_lab.process.outcomes.append(OSError(CANARY))
    else:
        queue(delete_lab, [["D", failure]])
    adapter(delete_lab)
    with pytest.raises(ServiceFailure) as error:
        delete_lab.services.delete(ROOT, False, delete_lab.context)
    expected = "request_failed"
    if (
        failure == "object"
        or (stage == "delete" and failure == "empty")
        or (stage == "describe" and failure == "rnf")
    ):
        expected = "ddb_delete_state"
    elif stage == "describe" and failure == "in_use":
        expected = "describe_failed"
    assert error.value.args == ("service=" + expected,)
    names = [name for name, _ in prefix] + [{"describe": "D", "update": "U", "delete": "X"}[stage]]
    trace(delete_lab, names, 6)
    privacy.check(error.value, delete_lab)


@pytest.mark.parametrize("case", ["nome", "arn", "active", "ausente"])
def test_delete_so_aceita_table_description_deleting_vinculado(
    delete_lab: ReadLab, case: str, privacy: Privacy
) -> None:
    queue(delete_lab, fixture()["adapter_unprotected"][:2])
    data = fixture()["replies"]["delete"]
    table = data["TableDescription"]
    if case == "ausente":
        del table["TableStatus"]
    else:
        key, value = {
            "nome": ("TableName", CANARY),
            "arn": ("TableArn", CANARY),
            "active": ("TableStatus", "ACTIVE"),
        }[case]
        table[key] = value
    respond(delete_lab, data)
    adapter(delete_lab)
    with pytest.raises(ServiceFailure) as error:
        delete_lab.services.delete(ROOT, False, delete_lab.context)
    assert error.value.args == ("service=ddb_delete_state",)
    trace(delete_lab, ["D", "T", "X"], 6)
    privacy.check(error.value, delete_lab)


@pytest.mark.parametrize("cut", [0, 3, 5])
def test_saldo_original_interrompe_antes_de_request_sem_debito_extra(
    delete_lab: ReadLab, cut: int
) -> None:
    names = adapter(delete_lab)
    budget = delete_lab.context.budget
    budget.limits = replace(budget.limits, calls=max(1, cut), cleanup_reserve=0)
    if cut == 0:
        budget.used = 1
    with pytest.raises(BudgetExhausted) as error:
        delete_lab.services.delete(ROOT, False, delete_lab.context)
    assert error.value.args == ("budget=requests",)
    assert budget.used == max(1, cut)
    assert budget.work_used == 0
    assert len(delete_lab.process.calls) == cut
    assert delete_lab.process.observed_debits == list(range(1, cut + 1))
    assert len(delete_lab.process.outcomes) == 6 - cut
    assert [call[0][2] for call in delete_lab.process.calls] == [
        fixture()["requests"][name][1] for name in names[:cut]
    ]


def test_deadline_cleanup_esgotado_impede_primeiro_delete(delete_lab: ReadLab) -> None:
    adapter(delete_lab)
    delete_lab.context.budget.cleanup_deadline = 1000
    with pytest.raises(BudgetExhausted) as error:
        delete_lab.services.delete(ROOT, False, delete_lab.context)
    assert error.value.args == ("budget=deadline",)
    trace(delete_lab, [], 6)


def test_privacidade_de_erro_remoto_e_campos_invalidos(
    delete_lab: ReadLab, caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    queue(delete_lab, [["D", "unprotected"], ["T", "owner"], ["X", "denied"]])
    adapter(delete_lab)
    with pytest.raises(ServiceFailure) as error:
        delete_lab.services.delete(ROOT, False, delete_lab.context)
    assert error.value.args == ("service=request_failed",)
    trace(delete_lab, ["D", "T", "X"], 6)
    assert error.value.__cause__ is None
    captured = capsys.readouterr()
    assert_private(error.value, delete_lab, caplog.text + captured.out + captured.err)


def test_controle_json_preserva_tipos_distintos_na_fronteira_real(delete_lab: ReadLab) -> None:
    data = fixture()["replies"]["unprotected"]
    for value in [False, 0, True, 1]:
        data["Table"]["DeletionProtectionEnabled"] = value
        respond(delete_lab, data)
        reply = delete_lab.services.ports.commands.call(
            Request("dynamodb", "describe-table", {"TableName": "cnesdata-cap-a1b2c3d4"}),
            delete_lab.context,
        )
        assert reply.data is not None
        assert type(cast("Any", reply.data["Table"])["DeletionProtectionEnabled"]) is type(value)
    trace(delete_lab, ["D", "D", "D", "D"])


@pytest.mark.parametrize("profile", [False, True], ids=["schema1", "schema2"])
def test_recibo_opcional_preserva_schema_e_limpa_evidencia_ao_load_legado(
    delete_lab: ReadLab, profile: bool
) -> None:
    spec = delete_lab.services.spec
    data = fixture()["journal"]
    if profile:
        spec = replace(spec, probe="ddb-transactions")
        data.update(schema_version=2, **fixture()["profile"])
    journal = Journal(spec)
    expected = {**data, "dynamodb_delete_accepted": [fixture()["root"]]}
    journal.path.write_text(json.dumps(expected))
    journal.path.chmod(0o600)
    journal.load()
    assert json.loads(journal.content()) == expected
    journal.path.write_text(json.dumps(data))
    journal.load()
    assert json.loads(journal.content()) == data
    journal.path.write_text(json.dumps({**data, "dynamodb_delete_accepted": []}))
    journal.load()
    assert json.loads(journal.content()) == data
    assert delete_lab.process.calls == []


@pytest.mark.parametrize("profile", [False, True], ids=["schema1", "schema2"])
def test_journal_legado_preserva_todos_os_bytes(delete_lab: ReadLab, profile: bool) -> None:
    spec = delete_lab.services.spec
    expected = LEGACY
    if profile:
        spec = replace(spec, probe="ddb-transactions")
        expected = LEGACY.replace(b'"schema_version": 1', b'"schema_version": 2')[:-1] + PROFILE
    journal = Journal(spec)
    journal.path.write_bytes(expected)
    journal.path.chmod(0o600)
    journal.load()
    assert journal.content() == expected
    journal.save()
    assert journal.path.read_bytes() == expected
    assert delete_lab.process.calls == []


@pytest.mark.parametrize(
    "case",
    [
        "tipo",
        "duplicado",
        "identidade_duplicada",
        "orfao",
        "nome",
        "servico",
        "baseline",
        "unknown",
        "complete",
        "malformado",
    ],
)
def test_recibo_corrompido_falha_antes_de_mutacao(delete_lab: ReadLab, case: str) -> None:
    journal = Journal(delete_lab.services.spec)
    data = fixture()["journal"]
    data["dynamodb_delete_accepted"] = [fixture()["root"]]
    rows = data["dynamodb_delete_accepted"]
    if case == "tipo":
        data["dynamodb_delete_accepted"] = True
    elif case == "duplicado":
        rows.append(dict(rows[0]))
    elif case == "identidade_duplicada":
        rows.append({**rows[0], "name": "alias"})
        data["created"].append(dict(rows[1]))
    elif case == "orfao":
        data["created"] = []
    elif case == "nome":
        rows[0]["name"] = "different-full-resource"
    elif case == "servico":
        rows[0]["service"] = "s3"
        data["created"] = [dict(rows[0])]
    elif case == "baseline":
        data["baseline"] = [{**rows[0], "name": "different-baseline-name"}]
    elif case == "unknown":
        data.update(operation_state="unknown", pending_service="dynamodb")
    elif case == "complete":
        data["phase"] = "complete"
    else:
        rows[0]["resource_id"] = 1
    journal.path.write_text(json.dumps(data))
    journal.path.chmod(0o600)
    with pytest.raises(SafetyFailure) as error:
        journal.load()
    assert error.value.args == ("lifecycle=journal_corrupt",)
    assert delete_lab.process.calls == []
    assert delete_lab.context.budget.used == 0
