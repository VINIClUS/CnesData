"""Incerteza CAS, corrupção e reconhecimento sem fabricar sucesso."""

from typing import Any

import pytest

from packages.cnes_infra.tests.auth.dynamodb_refresh_cas_support import (
    TABLE,
    CasCase,
    assert_atomic_reads,
    assert_pair,
    changed,
    failure,
    keys,
    mutation,
    pair,
    pure_failure,
    seed,
)
from packages.cnes_infra.tests.auth.dynamodb_refresh_support import (
    EXPIRES,
    ISSUED,
    TOKEN,
    requests_for,
)

pytest_plugins = (
    "packages.cnes_infra.tests.auth.dynamodb_refresh_support",
    "packages.cnes_infra.tests.auth.dynamodb_refresh_cas_support",
)


@pytest.mark.parametrize(
    "envelope",
    [
        None,
        {},
        {
            "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "ok"},
            "Item": {"invalid": {"S": "not-a-record"}},
        },
    ],
    ids=["none", "vazio", "item-invalido"],
)
def test_descoberta_malformada_nao_e_ausencia(cas_case: CasCase, envelope: Any) -> None:
    boundary: Any = cas_case.boundary
    seed(boundary, pair())
    boundary.response_overrides["get_item"] = envelope
    cas_case.unavailable()
    assert len(requests_for(boundary, "get_item")) == 1
    assert requests_for(boundary, "transact_get_items") == []
    assert requests_for(boundary, "transact_write_items") == []
    assert_pair(boundary, pair())


@pytest.mark.parametrize(
    "case",
    ["digest-ausente", "agent-ausente", "ambos-ausentes", "campo-invalido", "discordante"],
)
def test_snapshot_parcial_ou_discordante_precede_guardas(cas_case: CasCase, case: str) -> None:
    boundary: Any = cas_case.boundary
    seed(boundary, pair())
    original = boundary.transact_get_items
    expected = pair()

    def snapshot(**kwargs: Any) -> Any:
        for index, projection in enumerate(("digest", "agent")):
            if case in {f"{projection}-ausente", "ambos-ausentes"}:
                boundary.client.delete_item(TableName=TABLE, Key=keys()[index])
                expected[projection] = None
        if case in {"campo-invalido", "discordante"}:
            expected["agent"].update(
                {
                    "schema_version" if case == "campo-invalido" else "machine_fingerprint": {
                        "N": "2"
                    }
                    if case == "campo-invalido"
                    else {"S": "different-fingerprint"},
                }
            )
            boundary.client.put_item(TableName=TABLE, Item=expected["agent"])
        return original(**kwargs)

    boundary.transact_get_items = snapshot
    cas_case.unavailable(context=mutation(expected_version=6))
    assert_atomic_reads(boundary)
    assert requests_for(boundary, "transact_write_items") == []
    assert_pair(boundary, expected)


@pytest.mark.parametrize("case", ["schema-expirado", "fingerprint-revogado", "max-futuro"])
def test_corrupcao_precede_inatividade(cas_case: CasCase, case: str) -> None:
    before = pair("revoked8" if case == "fingerprint-revogado" else "base7")
    if case == "schema-expirado":
        before = changed(before, schema_version={"N": "2"})
    elif case == "fingerprint-revogado":
        for item in before.values():
            del item["machine_fingerprint"]
    else:
        before = changed(before, version={"N": "9223372036854775808"})
    seed(cas_case.boundary, before)
    context = mutation(now=EXPIRES if case == "schema-expirado" else ISSUED)
    if case == "max-futuro":
        context = mutation(now=ISSUED.replace(microsecond=123455))
    cas_case.unavailable(context=context)
    assert requests_for(cas_case.boundary, "transact_write_items") == []
    assert_pair(cas_case.boundary, before)


@pytest.mark.parametrize(
    "case",
    [
        "access-denied",
        "internal",
        "capacity",
        "transaction-conflict",
        "conditional-mixed",
        "reasons-missing",
        "reasons-malformed",
        "reasons-all-none",
    ],
)
def test_sdk_ambiguo_nao_vira_false(cas_case: CasCase, case: str) -> None:
    boundary: Any = cas_case.boundary
    seed(boundary, pair())
    codes = {
        "access-denied": "AccessDeniedException",
        "internal": "InternalServerError",
        "capacity": "ProvisionedThroughputExceededException",
        "transaction-conflict": "TransactionConflictException",
    }
    reasons = {
        "conditional-mixed": [{"Code": "ConditionalCheckFailed"}, {"Code": "TransactionConflict"}],
        "reasons-malformed": [{"Code": "ConditionalCheckFailed"}],
        "reasons-all-none": [{"Code": "None"}, {"Code": "None"}],
    }.get(case)
    boundary.fail_operation = "transact_write_items"
    boundary.fail_error = failure(codes.get(case, "TransactionCanceledException"), reasons)
    cas_case.unavailable()
    assert len(requests_for(boundary, "transact_write_items")) == 1
    assert_atomic_reads(boundary)
    assert boundary.forwarded_errors == [("transact_write_items", boundary.fail_error)]
    assert_pair(boundary, pair())


@pytest.mark.parametrize(
    "envelope",
    [None, {"ResponseMetadata": {"HTTPStatusCode": 200}}],
    ids=["none", "sem-request-id"],
)
def test_resposta_de_escrita_invalida_nao_confirma_cas(cas_case: CasCase, envelope: Any) -> None:
    boundary: Any = cas_case.boundary
    seed(boundary, pair())
    boundary.response_overrides["transact_write_items"] = envelope
    cas_case.unavailable()
    assert len(requests_for(boundary, "transact_write_items")) == 1
    assert_atomic_reads(boundary)
    assert_pair(boundary, pair())


@pytest.mark.parametrize(
    ("operation", "mode"),
    [("mark_used", "timeout"), ("revoke", "malformada")],
    ids=["timeout-apos-commit", "envelope-apos-commit"],
)
def test_resposta_perdida_apos_commit_nao_inventa_resultado(
    cas_case: CasCase,
    operation: str,
    mode: str,
) -> None:
    boundary: Any = cas_case.boundary
    seed(boundary, pair())
    original = boundary.transact_write_items
    calls: list[Any] = []

    def uncertain(**kwargs: Any) -> Any:
        calls.append(kwargs)
        original(**kwargs)
        if mode == "timeout":
            raise TimeoutError(str(failure()))
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    boundary.transact_write_items = uncertain
    cas_case.unavailable(operation)
    assert len(calls) == 1
    assert_atomic_reads(boundary)
    expected = pair("used8" if operation == "mark_used" else "revoked8")
    assert_pair(boundary, expected)
    boundary.transact_write_items = original
    assert getattr(cas_case.store(), operation)(TOKEN, mutation()) is False
    assert len(calls) == 1
    assert len(requests_for(boundary, "transact_write_items")) == 1
    assert_pair(boundary, expected)


@pytest.mark.parametrize(
    "case",
    [
        "versao-maior",
        "mesma-versao",
        "versao-menor",
        "parcial",
        "leitura-falha",
        "imutavel-alterado",
    ],
)
def test_cancelamento_condicional_exige_prova_atual(cas_case: CasCase, case: str) -> None:
    boundary: Any = cas_case.boundary
    seed(boundary, pair())
    original = boundary.transact_write_items
    proof = pair("used8")
    if case == "versao-maior":
        proof = changed(proof, version={"N": "10"})
    if case in {"mesma-versao", "versao-menor"}:
        proof = changed(pair(), version={"N": "7" if case == "mesma-versao" else "6"})
    if case == "imutavel-alterado":
        proof = changed(proof, machine_fingerprint={"S": "new-immutable-fingerprint"})
    boundary.fail_operation = "transact_write_items"
    error = pure_failure()
    boundary.fail_error = error

    def cancel(**kwargs: Any) -> Any:
        seed(boundary, proof)
        if case == "parcial":
            boundary.client.delete_item(TableName=TABLE, Key=keys()[1])
        if case == "leitura-falha":
            boundary.transact_get_items = _failed_proof(boundary)
        return original(**kwargs)

    boundary.transact_write_items = cancel
    if case == "versao-maior":
        assert cas_case.store().mark_used(TOKEN, mutation()) is False
    else:
        cas_case.unavailable()
    assert len(requests_for(boundary, "transact_write_items")) == 1
    assert_atomic_reads(boundary, count=2)
    assert error.response.get("CancellationReasons", []) == [
        {"Code": "None"},
        {"Code": "ConditionalCheckFailed"},
    ]
    expected = dict(proof, agent=None) if case == "parcial" else proof
    assert_pair(boundary, expected)


def _failed_proof(boundary: Any) -> Any:
    def fail(**kwargs: Any) -> Any:
        boundary.requests.append(("transact_get_items", kwargs))
        raise failure("InternalServerError")

    return fail
