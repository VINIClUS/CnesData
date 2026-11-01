"""Plano de dados DynamoDB da fronteira com estado: LeadingKeys, índice e ações não concedidas."""

import json
from pathlib import Path

import pytest

from tests.integration.aws_capability import _matrix_policies as policies
from tests.integration.aws_capability._aws_fake_testing import FakeSession
from tests.integration.aws_capability._aws_fake_types_testing import Reply
from tests.integration.aws_capability._b1_testing import NAME
from tests.integration.aws_capability._probe_testing import ProbeLab, make_probe_lab
from tests.integration.aws_capability.cli import Request

TABLE_ARN = "arn:aws:dynamodb:us-east-2:111111111111:table/" + NAME
SEEDS = [
    {"pk": "TENANT#_GLOBAL", "sk": "m"},
    {"pk": "OUTBOX#CURSOR", "sk": "m", "gsi6pk": "OUTBOX#PENDING", "gsi6sk": "c"},
    {"pk": "TENANT#x", "sk": "m", "gsi1pk": "USER#1", "gsi1sk": "u"},
    {"pk": "OTHER#1", "sk": "m", "gsi1pk": "ADMIN#1", "gsi1sk": "a"},
]


def item(row: dict[str, str]) -> dict[str, dict[str, str]]:
    return {key: {"S": value} for key, value in row.items()}


@pytest.fixture
def lab(tmp_path: Path) -> ProbeLab:
    made = make_probe_lab(tmp_path, "iam-leadingkeys")
    for row in SEEDS:
        request = Request("dynamodb", "put-item", {"TableName": NAME, "Item": item(row)})
        assert made.aws.call(request, made.context).ok
    for role, statements in (
        (f"{NAME}-audit", policies.audit_dynamodb(TABLE_ARN)),
        (f"{NAME}-api", policies.api_dynamodb(TABLE_ARN)),
    ):
        document = json.dumps({"Version": "2012-10-17", "Statement": statements})
        payload = {"RoleName": role, "PolicyName": "p", "PolicyDocument": document}
        assert made.aws.call(Request("iam", "put-role-policy", payload), made.context).ok
    return made


def call(lab: ProbeLab, who: str, operation: str, payload: dict) -> Reply:
    request = Request("dynamodb", operation, {"TableName": NAME, **payload})
    return FakeSession(lab.aws, who).call(request, lab.context)


def code(reply: Reply) -> str:
    return "ok" if reply.ok else reply.error.code  # type: ignore[union-attr]


def key(pk: str) -> dict:
    return {"Key": {"pk": {"S": pk}, "sk": {"S": "m"}}}


def query(index: str | None, pk: str) -> dict:
    attribute = f"{index}pk" if index else "pk"
    payload: dict = {
        "KeyConditionExpression": f"{attribute} = :pk",
        "ExpressionAttributeValues": {":pk": {"S": pk}},
        "Select": "COUNT",
    }
    if index:
        payload["IndexName"] = index
    return payload


def test_get_item_do_audit_respeita_leading_keys(lab: ProbeLab) -> None:
    assert code(call(lab, "role-audit", "get-item", key("TENANT#_GLOBAL"))) == "ok"
    assert code(call(lab, "role-audit", "get-item", key("OUTBOX#CURSOR"))) == "ok"
    assert code(call(lab, "role-audit", "get-item", key("OTHER#1"))) == "AccessDeniedException"


def test_get_item_ausente_com_chave_permitida_e_ok_e_vazio(lab: ProbeLab) -> None:
    reply = call(lab, "role-audit", "get-item", key("TENANT#_GLOBAL"))
    assert reply.ok
    assert "Item" in reply.data
    lab.aws.tables[NAME].rows.clear()
    assert dict(call(lab, "role-audit", "get-item", key("TENANT#_GLOBAL")).data) == {}


def update(pk: str) -> dict:
    return {
        **key(pk),
        "UpdateExpression": "SET touched = :v",
        "ExpressionAttributeValues": {":v": {"N": "2"}},
    }


def test_update_item_do_audit_so_nas_duas_chaves(lab: ProbeLab) -> None:
    assert code(call(lab, "role-audit", "update-item", update("OUTBOX#CURSOR"))) == "ok"
    denied = call(lab, "role-audit", "update-item", update("TENANT#x"))
    assert code(denied) == "AccessDeniedException"
    row = lab.aws.tables[NAME].rows[("OUTBOX#CURSOR", "m")]
    assert row["touched"] == {"N": "2"}


def test_query_do_audit_so_no_gsi6_com_pending(lab: ProbeLab) -> None:
    ok = call(lab, "role-audit", "query", query("gsi6", "OUTBOX#PENDING"))
    assert dict(ok.data) == {"Count": 1, "ScannedCount": 1}
    assert code(call(lab, "role-audit", "query", query("gsi6", "OUTBOX#OTHER"))) == (
        "AccessDeniedException"
    )
    assert code(call(lab, "role-audit", "query", query("gsi1", "OUTBOX#PENDING"))) == (
        "AccessDeniedException"
    )
    assert code(call(lab, "role-audit", "query", query(None, "OUTBOX#PENDING"))) == (
        "AccessDeniedException"
    )


def test_query_do_api_so_no_gsi1_com_user(lab: ProbeLab) -> None:
    assert dict(call(lab, "role-api", "query", query("gsi1", "USER#1")).data)["Count"] == 1
    assert code(call(lab, "role-api", "query", query("gsi1", "ADMIN#1"))) == (
        "AccessDeniedException"
    )
    assert code(call(lab, "role-api", "query", query("gsi6", "USER#1"))) == (
        "AccessDeniedException"
    )


def test_acoes_nao_concedidas_ao_audit_sao_negadas_mesmo_em_chave_permitida(lab: ProbeLab) -> None:
    put = {"Item": item({"pk": "TENANT#_GLOBAL", "sk": "m"})}
    scan = {"Select": "COUNT", "ConsistentRead": True}
    assert code(call(lab, "role-audit", "put-item", put)) == "AccessDeniedException"
    assert code(call(lab, "role-audit", "delete-item", key("TENANT#_GLOBAL"))) == (
        "AccessDeniedException"
    )
    assert code(call(lab, "role-audit", "scan", scan)) == "AccessDeniedException"
    assert ("TENANT#_GLOBAL", "m") in lab.aws.tables[NAME].rows


def test_operador_semeia_e_escaneia_sem_policy(lab: ProbeLab) -> None:
    scan = Request(
        "dynamodb", "scan", {"TableName": NAME, "Select": "COUNT", "ConsistentRead": True}
    )
    assert lab.aws.call(scan, lab.context).ok


def transaction(*keys: str) -> dict:
    return {"TransactItems": [{"Update": update(pk) | {"TableName": NAME}} for pk in keys]}


def test_transacao_so_com_updates_permitidos_passa(lab: ProbeLab) -> None:
    reply = FakeSession(lab.aws, "role-audit").call(
        Request("dynamodb", "transact-write-items", transaction("TENANT#_GLOBAL", "OUTBOX#CURSOR")),
        lab.context,
    )
    assert reply.ok


def test_transacao_com_um_update_negado_falha_com_mensagem_de_autorizacao(lab: ProbeLab) -> None:
    reply = FakeSession(lab.aws, "role-audit").call(
        Request(
            "dynamodb", "transact-write-items",
            transaction("TENANT#_GLOBAL", "OUTBOX#CURSOR", "OTHER#1"),
        ),
        lab.context,
    )
    assert code(reply) == "AccessDeniedException"
    assert "not authorized to perform: dynamodb:UpdateItem" in reply.error.details["Message"]  # type: ignore[union-attr]
    assert "touched" not in lab.aws.tables[NAME].rows[("OUTBOX#CURSOR", "m")]


def test_escrita_antes_de_active_e_erro_do_teste(lab: ProbeLab) -> None:
    lab.aws.tables[NAME].status = "CREATING"
    with pytest.raises(AssertionError, match="write_before_active"):
        call(lab, "role-audit", "get-item", key("TENANT#_GLOBAL"))


def test_forma_do_query_e_validada_pelo_fake(lab: ProbeLab) -> None:
    bad = {**query("gsi6", "OUTBOX#PENDING"), "KeyConditionExpression": "gsi1pk = :pk"}
    with pytest.raises(AssertionError):
        call(lab, "role-audit", "query", bad)
