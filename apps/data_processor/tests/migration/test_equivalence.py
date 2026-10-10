"""Comparacao exata: regras fechadas, ausencias, formas do legado e presenca por linha."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from data_processor.migration.equivalence import (
    ComparisonStatus,
    EquivalenceContract,
    MetricComparison,
    compare_shadow_run,
    parse_contract,
)
from data_processor.migration.flatten import flatten_payload

_ROOT = Path(__file__).resolve().parents[4]
_CONTRACT = _ROOT / "docs" / "fixtures" / "migration" / "equivalence-contract-v1.json"
_MATCH, _EXPLAINED, _MISMATCH = (
    ComparisonStatus.MATCH, ComparisonStatus.EXPLAINED, ComparisonStatus.MISMATCH,
)


def _rule(rule_id: str = "R-1", **overrides: Any) -> dict[str, Any]:
    rule: dict[str, Any] = {
        "rule_id": rule_id, "metrics": ["d::x"], "check": "candidate_equals_context",
        "context": "key", "absent_ok": False, "rationale": "teste", **overrides,
    }
    needs_form = rule["check"] == "candidate_equals_context" and not rule["absent_ok"]
    return {**rule, "legacy_form": rule.get("legacy_form", "text" if needs_form else None)}


def _document(rules: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "contract_version": 1, "normalizations": ["date_iso8601"],
        "clock": "2026-10-10T12:00:00Z", "datasets": {}, "rules": rules,
    }


def _contract(*rules: dict[str, Any]) -> EquivalenceContract:
    return EquivalenceContract.model_validate_json(json.dumps(_document(list(rules))))


def _compare(
    contract: EquivalenceContract, legacy: dict[str, Any], candidate: dict[str, Any],
    context: dict[str, str] | None = None,
) -> tuple[MetricComparison, ...]:
    return compare_shadow_run(
        contract=contract, legacy=legacy, candidate=candidate, context=context,
    )


def test_compara_valores_identicos_como_match() -> None:
    legacy = {"d::x": 7, "d::y": "a", "d::z": None, "d::w": True}

    result = _compare(_contract(), legacy, dict(legacy))

    assert {item.status for item in result} == {_MATCH}
    assert all(item.rule_id is None and item.absent is None for item in result)


def test_valores_iguais_sao_match_mesmo_quando_a_regra_casa_com_a_metrica() -> None:
    result = _compare(_contract(_rule()), {"d::x": "v"}, {"d::x": "v"}, {"key": "v"})

    assert result == (MetricComparison("d::x", "v", "v", _MATCH, None),)


def test_regra_aprovada_marca_explained() -> None:
    result = _compare(_contract(_rule("R-CLOCK")), {"d::x": "old"}, {"d::x": "new"}, {"key": "new"})

    assert result == (MetricComparison("d::x", "old", "new", _EXPLAINED, "R-CLOCK"),)


def test_rejeita_regra_quando_o_predicado_falha() -> None:
    result = _compare(_contract(_rule()), {"d::x": "old"}, {"d::x": "new"}, {"key": "outro"})

    assert [(item.status, item.rule_id) for item in result] == [(_MISMATCH, None)]


def test_rejeita_regra_quando_a_chave_de_contexto_nao_existe() -> None:
    result = _compare(_contract(_rule()), {"d::x": "old"}, {"d::x": "new"})

    assert result[0].status is _MISMATCH


def test_diferenca_sem_regra_e_mismatch() -> None:
    result = _compare(_contract(), {"d::x": 1}, {"d::x": 2})

    assert result == (MetricComparison("d::x", 1, 2, _MISMATCH, None),)


def test_regra_fora_do_escopo_da_metrica_permanece_mismatch() -> None:
    contract = _contract(_rule(metrics=["d::generated_at"], context="clock"))
    legacy = {"d::generated_at": "old", "d::created_at": "old"}
    candidate = {"d::generated_at": "now", "d::created_at": "now"}

    result = _compare(contract, legacy, candidate, {"clock": "now"})

    assert {item.metric: item.status for item in result} == {
        "d::created_at": _MISMATCH, "d::generated_at": _EXPLAINED,
    }


def test_metrica_ausente_e_distinta_de_nulo() -> None:
    contract = _contract()

    missing_candidate = _compare(contract, {"d::x": None}, {})
    missing_legacy = _compare(contract, {}, {"d::x": None})
    both_null = _compare(contract, {"d::x": None}, {"d::x": None})

    expected_candidate = MetricComparison("d::x", None, None, _MISMATCH, None, "candidate")
    expected_legacy = MetricComparison("d::x", None, None, _MISMATCH, None, "legacy")
    assert missing_candidate == (expected_candidate,)
    assert missing_legacy == (expected_legacy,)
    assert both_null[0].status is _MATCH


def test_regra_absent_ok_explica_somente_ausencia_no_legado() -> None:
    contract = _contract(_rule("R-SHA", absent_ok=True, metrics=["d::[*].x"]))
    context = {"key": "sha"}

    absent_legacy = _compare(contract, {}, {"d::[1].x": "sha"}, context)
    other_value = _compare(contract, {"d::[1].x": "old"}, {"d::[1].x": "sha"}, context)
    absent_candidate = _compare(contract, {"d::[1].x": "sha"}, {}, context)

    assert [(item.status, item.rule_id, item.absent) for item in absent_legacy] == [
        (_EXPLAINED, "R-SHA", "legacy"),
    ]
    assert other_value[0].status is _MISMATCH
    assert absent_candidate[0].status is _MISMATCH


def test_regra_sem_absent_ok_nao_explica_ausencia() -> None:
    result = _compare(_contract(_rule()), {}, {"d::x": "sha"}, {"key": "sha"})

    assert result[0].status is _MISMATCH


def test_regra_por_valor_legado_mapeia_para_o_contexto() -> None:
    rule = _rule(
        "R-IDS", check="candidate_equals_context_by_legacy",
        context={"fixture-local": "id/local", "fixture-nacional": "id/nacional"},
    )
    legacy = {"d::x": "fixture-local", "d::y": "fixture-nacional", "d::z": "desconhecido"}
    candidate = {"d::x": "norm-1", "d::y": "norm-1", "d::z": "norm-1"}
    contract = _contract({**rule, "metrics": ["d::*"]})

    result = _compare(contract, legacy, candidate, {"id/local": "norm-1", "id/nacional": "norm-2"})

    assert {item.metric: item.status for item in result} == {
        "d::x": _EXPLAINED, "d::y": _MISMATCH, "d::z": _MISMATCH,
    }


@pytest.mark.parametrize(("legacy", "candidate", "status"), [
    (40, "40", _EXPLAINED),
    (40, "41", _MISMATCH),
    (True, "True", _MISMATCH),
    ("40", "40 ", _MISMATCH),
    (40, 40, _MATCH),
])
def test_regra_do_texto_do_inteiro_legado_e_exata(legacy: Any, candidate: Any, status: Any) -> None:
    rule = _rule("R-TXT", check="candidate_is_text_of_legacy_int", context=None)

    result = _compare(_contract(rule), {"d::x": legacy}, {"d::x": candidate})

    assert result[0].status is status


def test_regra_do_texto_do_inteiro_nao_cobre_outra_metrica() -> None:
    rule = _rule("R-TXT", metrics=["d::[*].local_value"], check="candidate_is_text_of_legacy_int",
                 context=None)

    result = _compare(_contract(rule), {"d::[k].count": 40}, {"d::[k].count": "40"})

    assert result[0].status is _MISMATCH


def test_bool_nao_equivale_a_inteiro() -> None:
    result = _compare(_contract(), {"d::x": True, "d::y": 1}, {"d::x": 1, "d::y": True})

    assert {item.status for item in result} == {_MISMATCH}


def test_glob_trata_colchetes_literalmente() -> None:
    rule = _rule(metrics=["d::[A|B].col"], context="key")
    legacy = {"d::[A|B].col": "o", "d::A.col": "o", "d::B.col": "o"}
    candidate = {"d::[A|B].col": "n", "d::A.col": "n", "d::B.col": "n"}

    result = _compare(_contract(rule), legacy, candidate, {"key": "n"})

    assert {item.metric: item.status for item in result} == {
        "d::[A|B].col": _EXPLAINED, "d::A.col": _MISMATCH, "d::B.col": _MISMATCH,
    }


def test_glob_asterisco_cobre_chaves_da_linha() -> None:
    rule = _rule(metrics=["d::[*]._normalized_at"], context="key")

    result = _compare(
        _contract(rule), {"d::[1|a]._normalized_at": "o"}, {"d::[1|a]._normalized_at": "n"},
        {"key": "n"},
    )

    assert result[0].status is _EXPLAINED


def _flat_compare(
    legacy: Any, candidate: Any, keys: dict[str, tuple[str, ...]] | None = None
) -> tuple[MetricComparison, ...]:
    schema = keys or {}
    return _compare(
        _contract(), flatten_payload("d", legacy, schema), flatten_payload("d", candidate, schema)
    )


@pytest.mark.parametrize(("legacy", "candidate"), [
    ({"total": 3, "missing_sources": []}, {"total": 3}),
    ({"total": 3, "missing_sources": []}, {"total": 3, "missing_sources": {}}),
    ({"total": 3, "missing_sources": {}}, {"total": 3, "missing_sources": []}),
    ({"total": 3, "missing_sources": []}, {"total": 3, "missing_sources": None}),
    ({"total": 3, "missing_sources": []}, {"total": 3, "missing_sources": ["CNES"]}),
    ({"total": 3}, {"total": 3, "missing_sources": []}),
    ({"a": {}}, {"a": {"b": 1}}),
])
def test_container_vazio_so_equivale_a_container_vazio_do_mesmo_tipo(
    legacy: Any, candidate: Any
) -> None:
    result = _flat_compare(legacy, candidate)

    assert _MISMATCH in {item.status for item in result}


@pytest.mark.parametrize("value", [{"a": []}, {"a": {}}, {"a": {"b": []}}])
def test_container_vazio_identico_nos_dois_lados_e_match(value: Any) -> None:
    result = _flat_compare(value, value)

    assert result
    assert {item.status for item in result} == {_MATCH}


@pytest.mark.parametrize("rows", [[{"k": "a"}], [{"k": "a"}, {"k": "b"}]])
def test_oraculo_com_lista_chaveada_vazia_exige_candidato_vazio(rows: Any) -> None:
    keys = {"": ("k",)}

    divergent = _flat_compare([], rows, keys)
    reverse = _flat_compare(rows, [], keys)
    same = _flat_compare([], [], keys)

    assert _MISMATCH in {item.status for item in divergent}
    assert _MISMATCH in {item.status for item in reverse}
    assert same
    assert {item.status for item in same} == {_MATCH}


def test_lista_chaveada_aninhada_vazia_exige_candidato_vazio() -> None:
    keys = {"por_cnes": ("cnes",)}

    result = _flat_compare({"por_cnes": []}, {"por_cnes": [{"cnes": "1"}]}, keys)

    assert _MISMATCH in {item.status for item in result}


_VOLATILE_CONTEXT = {
    "version_id": "mig010-sihd-2026-01",
    "clock_z": "2026-10-10T12:00:00Z",
    "clock_offset": "2026-10-10T12:00:00+00:00",
}
_VOLATILE = {
    "MIG010-RUN-ID": ("sihd-serving-overview::run_id", "version_id"),
    "MIG010-GENERATED-AT": ("sihd-serving-overview::generated_at", "clock_z"),
    "MIG010-NORMALIZED-AT": ("sihd-normalized-internacoes::[K1]._normalized_at", "clock_offset"),
}
_NOT_TEXT = (None, 42, True, "")
_NOT_AN_INSTANT = (
    *_NOT_TEXT, "not-a-timestamp", "2026-01-15", "2026-01-15T12:00:00",
    "2026-01-15T12:00:00-03:00",
)
_BAD_LEGACY = [
    *(("MIG010-RUN-ID", value) for value in _NOT_TEXT),
    *(("MIG010-GENERATED-AT", value) for value in (
        *_NOT_AN_INSTANT, "2026-01-15T12:00:00+00:00", "2026-13-45T00:00:00Z",
    )),
    *(("MIG010-NORMALIZED-AT", value) for value in (
        *_NOT_AN_INSTANT, "2026-01-15T12:00:00Z", "2026-13-45T00:00:00+00:00",
    )),
]


def _volatile(rule_id: str, legacy: Any) -> tuple[ComparisonStatus, str | None]:
    metric, key = _VOLATILE[rule_id]
    item = compare_shadow_run(
        contract=parse_contract(_CONTRACT.read_bytes()), legacy={metric: legacy},
        candidate={metric: _VOLATILE_CONTEXT[key]}, context=_VOLATILE_CONTEXT,
    )[0]
    return item.status, item.rule_id


@pytest.mark.parametrize(("rule_id", "legacy"), _BAD_LEGACY)
def test_regra_volatil_nao_explica_legado_fora_da_forma(rule_id: str, legacy: Any) -> None:
    assert _volatile(rule_id, legacy) == (_MISMATCH, None)


@pytest.mark.parametrize(("rule_id", "legacy"), [
    ("MIG010-RUN-ID", "fixture-run-v1"),
    ("MIG010-GENERATED-AT", "2026-01-31T23:59:59Z"),
    ("MIG010-NORMALIZED-AT", "2026-01-15T12:00:00+00:00"),
])
def test_regra_volatil_explica_legado_na_forma_esperada(rule_id: str, legacy: Any) -> None:
    assert _volatile(rule_id, legacy) == (_EXPLAINED, rule_id)


_SIA_DOC = "sia-normalized-sia-apa"
_SIA_KEYS = {"": ("_source_row",)}
_RAW_SHA = "ab" * 32


def _sia_run(candidate_rows: list[dict[str, Any]]) -> tuple[MetricComparison, ...]:
    legacy_rows = [{"_source_row": 1, "v": "a"}, {"_source_row": 2, "v": "b"}]
    return compare_shadow_run(
        contract=parse_contract(_CONTRACT.read_bytes()),
        legacy=flatten_payload(_SIA_DOC, legacy_rows, _SIA_KEYS),
        candidate=flatten_payload(_SIA_DOC, candidate_rows, _SIA_KEYS),
        context={f"raw_manifest_sha256/{_SIA_DOC}": _RAW_SHA},
    )


def _row(number: int, **extra: Any) -> dict[str, Any]:
    return {"_source_row": number, "v": "ab"[number - 1], **extra}


def test_candidato_com_o_hash_do_manifest_raw_em_toda_linha_nao_tem_mismatch() -> None:
    rows = [_row(1, _source_manifest_sha256=_RAW_SHA), _row(2, _source_manifest_sha256=_RAW_SHA)]

    result = _sia_run(rows)

    assert [item.status for item in result].count(_EXPLAINED) == 2
    assert _MISMATCH not in {item.status for item in result}


@pytest.mark.parametrize(("rows", "missing"), [
    ([_row(1, _source_manifest_sha256=_RAW_SHA), _row(2)], [2]),
    ([_row(1), _row(2)], [1, 2]),
])
def test_linha_do_candidato_sem_o_hash_do_manifest_raw_vira_mismatch(
    rows: list[dict[str, Any]], missing: list[int]
) -> None:
    result = _sia_run(rows)

    mismatches = [item for item in result if item.status is _MISMATCH]
    assert [item.metric for item in mismatches] == [
        f"{_SIA_DOC}::[{number}]._source_manifest_sha256" for number in missing
    ]
    assert {(item.absent, item.rule_id) for item in mismatches} == {("candidate", None)}


def test_hash_do_manifest_raw_divergente_gera_um_unico_mismatch_por_linha() -> None:
    rows = [_row(1, _source_manifest_sha256="00" * 32), _row(2, _source_manifest_sha256=_RAW_SHA)]

    result = _sia_run(rows)

    assert [item.metric for item in result if item.status is _MISMATCH] == [
        f"{_SIA_DOC}::[1]._source_manifest_sha256",
    ]


def test_glob_de_colchetes_curinga_cobre_um_unico_segmento() -> None:
    rule = _rule(metrics=["d::[*].x"], context="key")
    legacy = {"d::[A].x": "o", "d::[A].y[B].x": "o", "d::[A].y[B].z[C].x": "o"}
    candidate = {"d::[A].x": "n", "d::[A].y[B].x": "n", "d::[A].y[B].z[C].x": "n"}

    result = _compare(_contract(rule), legacy, candidate, {"key": "n"})

    assert {item.metric: item.status for item in result} == {
        "d::[A].x": _EXPLAINED, "d::[A].y[B].x": _MISMATCH, "d::[A].y[B].z[C].x": _MISMATCH,
    }
