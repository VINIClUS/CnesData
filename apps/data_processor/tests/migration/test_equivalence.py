"""Comparacao exata de equivalencia: contrato, achatamento, regras aprovadas e serializacao."""
from __future__ import annotations

import json
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from cnes_domain.orchestration.source_catalog import build_source_catalog
from data_processor.migration.equivalence import (
    ComparisonStatus,
    ContractInvalid,
    EquivalenceContract,
    MetricComparison,
    SourceEquivalenceReport,
    aggregate_bytes,
    canonical_rows,
    compare_shadow_run,
    flatten_payload,
    load_contract,
    report_bytes,
    sha256_hex,
)

_ROOT = Path(__file__).resolve().parents[4]
_CONTRACT = _ROOT / "docs" / "fixtures" / "migration" / "equivalence-contract-v1.json"
_APPROVED = {
    "MIG010-RUN-ID", "MIG010-GENERATED-AT", "MIG010-NORMALIZED-AT",
    "MIG010-CNES-NORMALIZED-IDS", "MIG010-CNES-DIVERGENCE-TEXT",
    "MIG010-SIA-RAW-MANIFEST-SHA256",
}
_MATCH, _EXPLAINED, _MISMATCH = (
    ComparisonStatus.MATCH, ComparisonStatus.EXPLAINED, ComparisonStatus.MISMATCH,
)


def _rule(rule_id: str = "R-1", **overrides: Any) -> dict[str, Any]:
    return {
        "rule_id": rule_id, "metrics": ["d::x"], "check": "candidate_equals_context",
        "context": "key", "absent_ok": False, "rationale": "teste", **overrides,
    }


def _document(rules: list[dict[str, Any]]) -> dict[str, Any]:
    return {"contract_version": 1, "clock": "2026-10-10T12:00:00Z", "datasets": {}, "rules": rules}


def _contract(*rules: dict[str, Any]) -> EquivalenceContract:
    return EquivalenceContract.model_validate_json(json.dumps(_document(list(rules))))


def _write(tmp_path: Path, document: dict[str, Any]) -> Path:
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def _compare(
    contract: EquivalenceContract, legacy: dict[str, Any], candidate: dict[str, Any],
    context: dict[str, str] | None = None,
) -> tuple[MetricComparison, ...]:
    return compare_shadow_run(
        contract=contract, legacy=legacy, candidate=candidate, context=context,
    )


@pytest.mark.parametrize("name", ["tolerance", "percent", "epsilon", "max_percent", "Epsilon"])
def test_rejeita_campos_de_tolerancia_estatistica(tmp_path: Path, name: str) -> None:
    document = _document([{**_rule(), name: 0}])

    with pytest.raises(ContractInvalid, match=f"forbidden_field key={name}"):
        load_contract(_write(tmp_path, document))


def test_rejeita_campo_de_tolerancia_aninhado_fora_das_regras(tmp_path: Path) -> None:
    document = _document([])
    document["datasets"] = {"cnes": {"provenance": {"tolerance_rows": 1}}}

    with pytest.raises(ContractInvalid, match="forbidden_field key=tolerance_rows"):
        load_contract(_write(tmp_path, document))


def test_rejeita_rule_id_duplicado(tmp_path: Path) -> None:
    with pytest.raises(ContractInvalid, match="contract_invalid"):
        load_contract(_write(tmp_path, _document([_rule("R-1"), _rule("R-1")])))


def test_rejeita_predicado_fora_do_conjunto_fechado(tmp_path: Path) -> None:
    document = _document([_rule(check="candidate_within_percentage")])

    with pytest.raises(ContractInvalid, match="contract_invalid"):
        load_contract(_write(tmp_path, document))


def _real_document() -> dict[str, Any]:
    return json.loads(_CONTRACT.read_text(encoding="utf-8"))


def _drop(mapping: dict[str, Any], key: str) -> None:
    mapping.pop(key)


@pytest.mark.parametrize(("mutate", "code"), [
    (lambda d: _drop(d["datasets"]["sihd"]["raw_inputs"][0], "rows"), "raw_input_body_required"),
    (lambda d: d["datasets"]["sihd"]["raw_inputs"][0]["manifest"].update(competencia="2026-02"),
     "raw_manifest_competencia_mismatch"),
    (lambda d: _drop(d["datasets"]["sihd"]["oracle_files"], "expected_serving.json"),
     "file_not_pinned file=expected_serving.json"),
    (lambda d: d["rules"][0].update(context={"a": "b"}), "rule_context_invalid"),
    (lambda d: d.update(clock="2026-10-10T12:00:00-03:00"), "clock_utc_required"),
    (lambda d: d["datasets"]["sihd"]["documents"].append(d["datasets"]["sihd"]["documents"][0]),
     "duplicate_doc_id"),
    (lambda d: d["datasets"]["sihd"]["documents"][0].update(doc_id="bpa-outro"),
     "doc_id_prefix_invalid dataset=sihd"),
])
def test_rejeita_contrato_com_dataset_inconsistente(
    tmp_path: Path, mutate: Any, code: str
) -> None:
    document = _real_document()
    mutate(document)

    with pytest.raises(ContractInvalid, match=code):
        load_contract(_write(tmp_path, document))


def test_rejeita_contrato_que_nao_e_json(tmp_path: Path) -> None:
    path = tmp_path / "contract.json"
    path.write_text("{nao e json", encoding="utf-8")

    with pytest.raises(ContractInvalid, match=r"contract_unreadable path=contract\.json"):
        load_contract(path)


def test_contrato_expoe_relogio_nas_duas_formas_e_chaves_por_documento() -> None:
    contract = load_contract(_CONTRACT)
    documents = {item.doc_id: item for item in contract.datasets["sihd"].documents}

    assert contract.clock_z == "2026-10-10T12:00:00Z"
    assert contract.clock_offset == "2026-10-10T12:00:00+00:00"
    assert documents["sihd-serving-overview"].key_paths == {
        "por_cnes": ("cnes",), "por_procedimento": ("procedimento",),
    }
    assert documents["sihd-normalized-internacoes"].key_paths == {"": ("SIHD_KEY",)}


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
    contract = _contract(_rule("R-SHA", absent_ok=True))
    context = {"key": "sha"}

    absent_legacy = _compare(contract, {}, {"d::x": "sha"}, context)
    other_value = _compare(contract, {"d::x": "old"}, {"d::x": "sha"}, context)
    absent_candidate = _compare(contract, {"d::x": "sha"}, {}, context)

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


def test_achata_objetos_com_chaves_pontuadas() -> None:
    value = {"kpis": {"linhas": 3, "ok": True}, "nota": None, "lista": []}

    flat = flatten_payload("serving", value, {})

    assert flat == {"serving::kpis.linhas": 3, "serving::kpis.ok": True, "serving::nota": None}


def test_achata_linhas_chaveadas_sem_depender_da_ordem() -> None:
    rows = [{"cnes": "B", "n": 2}, {"cnes": "A", "n": 1}]

    forward = flatten_payload("gold", rows, {"": ("cnes",)})
    backward = flatten_payload("gold", rows[::-1], {"": ("cnes",)})

    assert forward == backward == {
        "gold::[A].cnes": "A", "gold::[A].n": 1, "gold::[B].cnes": "B", "gold::[B].n": 2,
    }


def test_achata_chave_composta_com_caminho_pontuado_e_nulo() -> None:
    rows = [{"natural_key": {"identity": "I"}, "field": None, "v": 1}]

    flat = flatten_payload("div", rows, {"": ("natural_key.identity", "field")})

    assert flat["div::[I|].natural_key.identity"] == "I"
    assert flat["div::[I|].v"] == 1


def test_achata_listas_aninhadas_chaveadas_e_posicionais() -> None:
    value = {"estabelecimentos": [{"cnes": "X", "n": 1}], "missing_sources": ["a", "b"]}

    flat = flatten_payload("d", value, {"estabelecimentos": ("cnes",)})

    assert flat == {
        "d::estabelecimentos[X].cnes": "X", "d::estabelecimentos[X].n": 1,
        "d::missing_sources[0]": "a", "d::missing_sources[1]": "b",
    }


def test_achata_datas_como_iso() -> None:
    value = {"dia": date(2026, 1, 3), "instante": datetime(2026, 1, 3, 12, tzinfo=UTC)}

    flat = flatten_payload("d", value, {})

    assert flat == {"d::dia": "2026-01-03", "d::instante": "2026-01-03T12:00:00+00:00"}


@pytest.mark.parametrize("value", [{"x": 1.5}, [{"k": "a", "x": 0.0}]])
def test_rejeita_float(value: Any) -> None:
    with pytest.raises(ValueError, match="float_not_allowed"):
        flatten_payload("d", value, {"": ("k",)})


def test_rejeita_metricas_com_o_mesmo_nome() -> None:
    with pytest.raises(ValueError, match=r"duplicate_metric metric=d::a\.b"):
        flatten_payload("d", {"a.b": 1, "a": {"b": 2}}, {})


def test_rejeita_valor_de_tipo_nao_suportado() -> None:
    with pytest.raises(ValueError, match="unsupported_value"):
        flatten_payload("d", {"x": b"bytes"}, {})


def test_rejeita_chave_duplicada() -> None:
    rows = [{"k": "a", "v": 1}, {"k": "a", "v": 2}]

    with pytest.raises(ValueError, match=r"duplicate_key key=a"):
        canonical_rows(rows, ("k",))
    with pytest.raises(ValueError, match="duplicate_key"):
        flatten_payload("d", rows, {"": ("k",)})


def test_rejeita_linha_sem_a_coluna_da_chave() -> None:
    with pytest.raises(ValueError, match="key_missing column=k"):
        canonical_rows([{"outra": 1}], ("k",))


def test_linhas_canonicas_independem_da_ordem() -> None:
    rows = [{"k": "b", "v": 2}, {"k": "a", "v": 1}, {"k": "c", "v": 3}]

    forward = canonical_rows(rows, ("k",))
    backward = canonical_rows(rows[::-1], ("k",))

    assert list(forward) == list(backward) == ["a", "b", "c"]
    assert forward == backward


def _report(*comparisons: MetricComparison) -> SourceEquivalenceReport:
    return SourceEquivalenceReport(
        tenant_id="354130", dataset="sihd", source_types=("SIHD",), competencia="2026-01",
        legacy_sha256="a" * 64, candidate_version_id="run-1", comparisons=comparisons,
    )


def test_relatorio_vazio_ou_com_mismatch_nao_e_aceito() -> None:
    match = MetricComparison("d::x", 1, 1, _MATCH, None)
    explained = MetricComparison("d::y", "a", "b", _EXPLAINED, "R-1")
    mismatch = MetricComparison("d::z", 1, 2, _MISMATCH, None)

    assert _report().accepted is False
    assert _report(match, mismatch).accepted is False
    assert _report(match, explained).accepted is True


def test_serializa_relatorio_e_agregado_com_sha256_estavel() -> None:
    comparisons = (
        MetricComparison("d::x", 1, 1, _MATCH, None),
        MetricComparison("d::y", "a", "b", _EXPLAINED, "R-1"),
        MetricComparison("d::z", None, "c", _MISMATCH, None, "legacy"),
    )
    evidence: dict[str, object] = {"run_manifest_sha256": "b" * 64, "outputs": [{"asserted": True}]}

    first = report_bytes(_report(*comparisons), evidence)
    second = report_bytes(_report(*comparisons), dict(reversed(list(evidence.items()))))
    payload = json.loads(first)

    assert first == second
    assert list(payload) == sorted(payload)
    assert payload["accepted"] is False
    assert payload["summary"] == {"EXPLAINED": 1, "MATCH": 1, "MISMATCH": 1}
    assert payload["comparisons"][2]["absent"] == "legacy"
    assert sha256_hex(first) == sha256(first).hexdigest()
    shuffled = {"reports": {"b": "2", "a": "1"}, "commit": "abc"}
    ordered = {"commit": "abc", "reports": {"a": "1", "b": "2"}}
    assert aggregate_bytes(shuffled) == aggregate_bytes(ordered)


def _digest(path: Path) -> str:
    data = path.read_bytes()
    if path.suffix == ".json":
        data = data.replace(b"\r\n", b"\n")
    return sha256(data).hexdigest()


def _catalog_leaves(dataset: str) -> dict[str, set[str]]:
    layout = build_source_catalog().for_pipeline(dataset).layout
    return {
        "normalized": {n for item in layout.normalized for n in item.normalized_filenames},
        "reconciliation": {layout.reconciliation_filename, layout.divergence_filename},
        "serving": {f"{name}.json" for name in layout.serving_documents},
    }


def test_contrato_versionado_fixa_hashes_e_layout_do_catalogo() -> None:
    contract = load_contract(_CONTRACT)

    assert set(contract.datasets) == {"cnes", "sihd", "bpa", "sia"}
    assert {rule.rule_id for rule in contract.rules} == _APPROVED
    for name, spec in contract.datasets.items():
        pinned = {file: _digest(_ROOT / spec.oracle_dir / file) for file in spec.oracle_files}
        assert pinned == spec.oracle_files
        leaves = _catalog_leaves(name)
        for document in spec.documents:
            assert document.candidate.leaf in leaves[document.candidate.layer]
            assert document.oracle.file in spec.oracle_files


def test_contrato_versionado_cobre_as_dependencias_requeridas_e_a_proveniencia() -> None:
    contract = load_contract(_CONTRACT)

    for name, spec in contract.datasets.items():
        seeded = {(i.manifest["source_type"], i.manifest["file_subtype"]) for i in spec.raw_inputs}
        required = {
            (dep.source_type, dep.file_subtype)
            for dep in build_source_catalog().for_pipeline(name).dependencies if dep.required
        }
        assert required <= seeded
        assert spec.provenance.data_nature == "synthetic"
    kinds = {name: spec.provenance.kind for name, spec in contract.datasets.items()}
    assert kinds == {
        "cnes": "independent_frozen", "sihd": "reproduction", "bpa": "reproduction",
        "sia": "reproduction",
    }
