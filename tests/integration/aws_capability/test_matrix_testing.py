"""Auxiliares da trilha: a cobertura exige mesma sessão, mesma ação e exatamente uma troca."""

from tests.integration.aws_capability._matrix_testing import Call, covered, dims, single_flip
from tests.integration.aws_capability.cli import Request


def get(who: str, pk: str, ok: bool, table: str = "t") -> Call:
    payload = {"TableName": table, "Key": {"pk": {"S": pk}, "sk": {"S": "m"}}}
    return Call(who, Request("dynamodb", "get-item", payload), ok, "ok" if ok else "Denied")


def query(who: str, index: str | None, pk: str, ok: bool) -> Call:
    attribute = f"{index}pk" if index else "pk"
    payload: dict = {
        "TableName": "t", "Select": "COUNT",
        "KeyConditionExpression": f"{attribute} = :pk",
        "ExpressionAttributeValues": {":pk": {"S": pk}},
    }
    if index:
        payload["IndexName"] = index
    return Call(who, Request("dynamodb", "query", payload), ok, "ok" if ok else "Denied")


def test_controle_com_chave_trocada_cobre_a_negacao() -> None:
    assert covered([get("a", "K1", True), get("a", "K2", False)], 1)


def test_sem_controle_anterior_a_negacao_nao_e_coberta() -> None:
    assert not covered([get("a", "K2", False)], 0)
    assert not covered([get("a", "K2", False), get("a", "K1", True)], 0)


def test_controle_que_falhou_nao_cobre() -> None:
    assert not covered([get("a", "K1", False), get("a", "K2", False)], 1)


def test_outra_sessao_ou_outra_acao_nao_cobre() -> None:
    assert not covered([get("b", "K1", True), get("a", "K2", False)], 1)
    other = Call("a", Request("dynamodb", "delete-item", get("a", "K1", True).payload), True, "ok")
    assert not covered([other, get("a", "K2", False)], 1)


def test_troca_dupla_de_indice_e_chave_nao_e_cobertura() -> None:
    control = query("a", "gsi6", "PENDING", True)
    assert single_flip(control, query("a", "gsi1", "PENDING", False))
    assert single_flip(control, query("a", None, "PENDING", False))
    assert single_flip(control, query("a", "gsi6", "OTHER", False))
    assert not single_flip(control, query("a", "gsi1", "OTHER", False))
    assert not single_flip(control, query("a", "gsi6", "PENDING", False))


def test_payload_diferente_fora_da_chave_nao_e_cobertura() -> None:
    control = get("a", "K1", True)
    assert not single_flip(control, get("a", "K2", False, table="outra"))
    assert dims(control) == {"key": "K1"}
