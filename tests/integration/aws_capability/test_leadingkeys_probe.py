"""Probe iam-leadingkeys: cada negação gravada fica ao lado de um sucesso da mesma ação."""

import json
from pathlib import Path

import pytest

from tests.integration.aws_capability import _matrix_policies as policies
from tests.integration.aws_capability._b1_testing import NAME
from tests.integration.aws_capability._leadingkeys_probe import message_category
from tests.integration.aws_capability._matrix_testing import Call, covered, dims, record
from tests.integration.aws_capability._probe_testing import ProbeLab, make_probe_lab
from tests.integration.aws_capability.contracts import ServiceFailure

TABLE_ARN = "arn:aws:dynamodb:us-east-2:111111111111:table/" + NAME
AUDIT, API = f"{NAME}-audit", f"{NAME}-api"
DENIED = "AccessDeniedException"
SEED_ITEMS = [
    {"pk": "TENANT#_GLOBAL", "sk": "meta"},
    {"pk": "OUTBOX#CURSOR", "sk": "meta", "gsi6pk": "OUTBOX#PENDING", "gsi6sk": "cursor"},
    {"pk": "TENANT#x", "sk": "meta", "gsi1pk": "USER#1", "gsi1sk": "user"},
    {"pk": "ENV#prod#ATTEMPTS", "sk": "meta", "gsi6pk": "OUTBOX#OTHER", "gsi6sk": "attempts"},
    {"pk": "OTHER#1", "sk": "meta", "gsi1pk": "ADMIN#1", "gsi1sk": "admin"},
]
SECONDS = {
    "assume_audit_seconds", "audit_get_ready_seconds", "audit_update_ready_seconds",
    "audit_query_ready_seconds", "assume_api_seconds", "api_get_ready_seconds",
    "api_query_ready_seconds",
}
EXPECTED = {
    "audit_get_other": DENIED,
    "audit_update_tenant": DENIED,
    "audit_update_attempts": DENIED,
    "audit_query_other_key": DENIED,
    "audit_query_gsi1": DENIED,
    "audit_query_table": DENIED,
    "audit_put_item_ungranted": DENIED,
    "audit_delete_item_ungranted": DENIED,
    "audit_scan_ungranted": DENIED,
    "tx_allowed_code": "ok",
    "tx_allowed_category": "none",
    "tx_mixed_code": DENIED,
    "tx_mixed_category": "not_authorized",
    "api_get_other": DENIED,
    "api_query_admin": DENIED,
    "api_query_gsi6": DENIED,
}


@pytest.fixture
def lab(tmp_path: Path) -> ProbeLab:
    return make_probe_lab(tmp_path, "iam-leadingkeys")


def run(lab: ProbeLab) -> dict:
    return dict(lab.services.probe(lab.resources, lab.spec, lab.context))


def row(call: Call) -> tuple:
    payload, operation = call.payload, call.operation
    if operation == "assume-role":
        return call.who, operation, str(payload["RoleArn"]).rsplit("-", 1)[1]
    if operation == "put-role-policy":
        role = str(payload["RoleName"]).rsplit("-", 1)[1]
        return call.who, operation, role, payload["PolicyName"]
    if operation == "transact-write-items":
        keys = tuple(i["Update"]["Key"]["pk"]["S"] for i in payload["TransactItems"])  # type: ignore[index]
        return call.who, operation, keys
    if operation in {"describe-table", "scan"}:
        return call.who, operation
    found = dims(call)
    return call.who, operation, found["key"], found.get("index")


STEPS = [
    ("operator", "describe-table"),
    *[("operator", "put-item", item["pk"], None) for item in SEED_ITEMS],
    ("operator", "assume-role", "audit"),
    ("operator", "put-role-policy", "audit", "cap-audit-ddb"),
    ("role-audit", "get-item", "TENANT#_GLOBAL", None),
    ("role-audit", "get-item", "OTHER#1", None),
    ("role-audit", "update-item", "OUTBOX#CURSOR", None),
    ("role-audit", "update-item", "TENANT#x", None),
    ("role-audit", "update-item", "ENV#prod#ATTEMPTS", None),
    ("role-audit", "query", "OUTBOX#PENDING", "gsi6"),
    ("role-audit", "query", "OUTBOX#OTHER", "gsi6"),
    ("role-audit", "query", "OUTBOX#PENDING", "gsi1"),
    ("role-audit", "query", "OUTBOX#PENDING", "table"),
    ("role-audit", "get-item", "TENANT#_GLOBAL", None),
    ("role-audit", "put-item", "TENANT#_GLOBAL", None),
    ("role-audit", "delete-item", "TENANT#_GLOBAL", None),
    ("role-audit", "scan"),
    ("role-audit", "transact-write-items", ("TENANT#_GLOBAL", "OUTBOX#CURSOR")),
    ("role-audit", "transact-write-items", ("TENANT#_GLOBAL", "OUTBOX#CURSOR", "OTHER#1")),
    ("operator", "assume-role", "api"),
    ("operator", "put-role-policy", "api", "cap-api-ddb"),
    ("role-api", "get-item", "TENANT#x", None),
    ("role-api", "get-item", "OTHER#1", None),
    ("role-api", "query", "USER#1", "gsi1"),
    ("role-api", "query", "ADMIN#1", "gsi1"),
    ("role-api", "query", "USER#1", "gsi6"),
]


def test_observa_codigos_categorias_e_segundos(lab: ProbeLab) -> None:
    observed = run(lab)
    assert {k: v for k, v in observed.items() if k not in SECONDS} == EXPECTED
    assert set(observed) == set(EXPECTED) | SECONDS
    assert all(isinstance(observed[key], float) and observed[key] >= 0 for key in SECONDS)


def test_ordem_chave_e_indice_de_cada_passo_com_controle_antes_da_negacao(lab: ProbeLab) -> None:
    calls = record(lab)
    run(lab)
    assert [row(call) for call in calls] == STEPS


def test_sementes_sao_gravadas_pelo_operador_com_sk_e_atributos_de_gsi(lab: ProbeLab) -> None:
    calls = record(lab)
    run(lab)
    puts = [c for c in calls if c.operation == "put-item" and c.who == "operator"]
    assert [dict(c.payload) for c in puts] == [
        {"TableName": NAME, "Item": {k: {"S": v} for k, v in item.items()}} for item in SEED_ITEMS
    ]


def test_cada_negacao_de_condicao_ou_recurso_tem_controle_anterior_com_uma_unica_troca(
    lab: ProbeLab,
) -> None:
    calls = record(lab)
    run(lab)
    denials = [
        i for i, c in enumerate(calls)
        if not c.ok and c.who != "operator" and c.operation != "transact-write-items"
    ]
    ungranted = {"put-item", "delete-item", "scan"}
    checked = [i for i in denials if calls[i].operation not in ungranted]
    assert len(checked) == 9
    assert [i for i in checked if not covered(calls, i)] == []


def test_acoes_nao_concedidas_vem_logo_depois_de_um_get_item_ok_da_mesma_sessao(
    lab: ProbeLab,
) -> None:
    calls = record(lab)
    observed = run(lab)
    assert {k for k in observed if k.endswith("_ungranted")} == {
        "audit_put_item_ungranted", "audit_delete_item_ungranted", "audit_scan_ungranted",
    }
    first = next(
        i for i, c in enumerate(calls) if c.operation == "put-item" and c.who != "operator"
    )
    control = calls[first - 1]
    assert (control.who, control.operation, control.ok) == ("role-audit", "get-item", True)
    assert dims(control) == {"key": "TENANT#_GLOBAL"}
    assert [c.operation for c in calls[first:first + 3]] == ["put-item", "delete-item", "scan"]
    assert [dims(c).get("key") for c in calls[first:first + 2]] == ["TENANT#_GLOBAL"] * 2


def test_formatos_exatos_de_cada_operacao(lab: ProbeLab) -> None:
    calls = record(lab)
    run(lab)
    by = {}
    for call in calls:
        by.setdefault((call.who, call.operation), call)
    key = {"pk": {"S": "TENANT#_GLOBAL"}, "sk": {"S": "meta"}}
    assert dict(by[("role-audit", "get-item")].payload) == {"TableName": NAME, "Key": key}
    update = by[("role-audit", "update-item")].payload
    assert update["UpdateExpression"] == "SET touched = :v"
    assert update["ExpressionAttributeValues"] == {":v": {"N": "2"}}
    assert dict(by[("role-audit", "query")].payload) == {
        "TableName": NAME, "IndexName": "gsi6", "Select": "COUNT",
        "KeyConditionExpression": "gsi6pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "OUTBOX#PENDING"}},
    }
    assert dict(by[("role-audit", "scan")].payload) == {
        "TableName": NAME, "Select": "COUNT", "ConsistentRead": True,
    }
    assert dict(by[("role-audit", "delete-item")].payload) == {"TableName": NAME, "Key": key}


def test_consulta_na_tabela_base_usa_pk_sem_indice(lab: ProbeLab) -> None:
    calls = record(lab)
    run(lab)
    (base,) = [
        c for c in calls if c.operation == "query" and "IndexName" not in c.payload
    ]
    assert dict(base.payload) == {
        "TableName": NAME, "Select": "COUNT",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {":pk": {"S": "OUTBOX#PENDING"}},
    }


def test_transacoes_usam_updates_com_a_mesma_expressao(lab: ProbeLab) -> None:
    calls = record(lab)
    run(lab)
    first, second = [c for c in calls if c.operation == "transact-write-items"]
    item = {
        "Update": {
            "TableName": NAME,
            "Key": {"pk": {"S": "TENANT#_GLOBAL"}, "sk": {"S": "meta"}},
            "UpdateExpression": "SET touched = :v",
            "ExpressionAttributeValues": {":v": {"N": "2"}},
        }
    }
    assert first.payload["TransactItems"][0] == item  # type: ignore[index]
    assert len(first.payload["TransactItems"]) == 2  # type: ignore[arg-type]
    assert len(second.payload["TransactItems"]) == 3  # type: ignore[arg-type]


def test_policies_inline_espelham_task_roles_com_os_arns_do_run(lab: ProbeLab) -> None:
    calls = record(lab)
    run(lab)
    puts = {c.payload["RoleName"]: c.payload for c in calls if c.operation == "put-role-policy"}
    assert json.loads(puts[AUDIT]["PolicyDocument"]) == {  # type: ignore[arg-type]
        "Version": "2012-10-17", "Statement": policies.audit_dynamodb(TABLE_ARN),
    }
    assert json.loads(puts[API]["PolicyDocument"]) == {  # type: ignore[arg-type]
        "Version": "2012-10-17", "Statement": policies.api_dynamodb(TABLE_ARN),
    }


def test_so_o_operador_e_as_duas_sessoes_chamam(lab: ProbeLab) -> None:
    run(lab)
    assert {who for _, _, who in lab.aws.calls} == {"operator", "role-audit", "role-api"}


def test_sementes_so_depois_de_tabela_e_indices_active(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-leadingkeys", ddb_ready_describes=4)
    lab.aws.tables[NAME].status = "CREATING"
    lab.aws.tables[NAME].ready_in = 4
    calls = record(lab)
    run(lab)
    operations = [c.operation for c in calls]
    assert operations[:5] == ["describe-table"] * 5
    assert operations[5] == "put-item"
    assert len(lab.pauses) >= 4


@pytest.mark.parametrize("option", [{"policy_lag": 10**6}])
def test_controle_que_nunca_fica_verde_falha_sem_gravar_negacao(
    tmp_path: Path, option: dict
) -> None:
    lab = make_probe_lab(tmp_path, "iam-leadingkeys", **option)
    calls = record(lab)
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=control_failed step=audit_get",)
    gets = [c for c in calls if c.who == "role-audit"]
    assert {dims(c)["key"] for c in gets} == {"TENANT#_GLOBAL"}
    assert len(gets) == 12


@pytest.mark.parametrize(
    ("text", "category"),
    [
        ("User: x is not authorized to perform: dynamodb:UpdateItem", "not_authorized"),
        ("Transaction cancelled, please refer cancellation reasons", "transaction_canceled"),
        ("Transaction canceled", "transaction_canceled"),
        ("something else", "other"),
        ("", "other"),
    ],
)
def test_categoria_da_mensagem_nunca_expoe_o_texto(text: str, category: str) -> None:
    assert message_category(text) == category


def test_recursos_divergentes_sao_recusados(lab: ProbeLab) -> None:
    with pytest.raises(ServiceFailure) as error:
        lab.services.probe(list(reversed(lab.resources)), lab.spec, lab.context)
    assert error.value.args == ("probe=leadingkeys_invalid",)
    assert lab.aws.calls == []
