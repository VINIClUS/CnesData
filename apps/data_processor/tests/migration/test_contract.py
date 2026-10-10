"""Contrato de equivalencia: schema, regras fechadas, normalizacoes e versao fixada."""
from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from cnes_domain.orchestration.source_catalog import build_source_catalog
from data_processor.migration.equivalence import ContractInvalid, load_contract

_ROOT = Path(__file__).resolve().parents[4]
_CONTRACT = _ROOT / "docs" / "fixtures" / "migration" / "equivalence-contract-v1.json"
_APPROVED = {
    "MIG010-RUN-ID", "MIG010-GENERATED-AT", "MIG010-NORMALIZED-AT",
    "MIG010-CNES-NORMALIZED-IDS", "MIG010-CNES-DIVERGENCE-TEXT",
    "MIG010-SIA-RAW-MANIFEST-SHA256",
}


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


def _write(tmp_path: Path, document: dict[str, Any]) -> Path:
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


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


@pytest.mark.parametrize("metrics", [
    ["d::x"], ["d::[*]"], ["d::[*].x*"], ["d::[*].x[*]"], ["d::[*].x", "d::y"],
])
def test_rejeita_regra_absent_ok_que_nao_declara_campo_por_linha(
    tmp_path: Path, metrics: list[str]
) -> None:
    document = _document([_rule(absent_ok=True, metrics=metrics)])

    with pytest.raises(ContractInvalid, match="rule_absent_ok_pattern_invalid rule_id=R-1"):
        load_contract(_write(tmp_path, document))


def test_contrato_versionado_declara_as_normalizacoes_aplicadas() -> None:
    assert load_contract(_CONTRACT).normalizations == ("date_iso8601",)


def test_rejeita_contrato_sem_normalizacoes_declaradas(tmp_path: Path) -> None:
    document = _real_document()
    document.pop("normalizations", None)

    with pytest.raises(ContractInvalid, match=r"loc=normalizations msg=Field required"):
        load_contract(_write(tmp_path, document))


@pytest.mark.parametrize("declared", [[], ["trim"], ["date_iso8601", "trim"], ["date_iso8601"] * 2])
def test_rejeita_normalizacoes_diferentes_das_aplicadas(
    tmp_path: Path, declared: list[str]
) -> None:
    document = _real_document()
    document["normalizations"] = declared

    with pytest.raises(ContractInvalid, match="normalizations_invalid"):
        load_contract(_write(tmp_path, document))


@pytest.mark.parametrize("overrides", [
    {"legacy_form": None},
    {"absent_ok": True, "metrics": ["d::[*].x"]},
    {"check": "candidate_is_text_of_legacy_int", "context": None},
])
def test_rejeita_forma_do_legado_incoerente_com_o_predicado(
    tmp_path: Path, overrides: dict[str, Any]
) -> None:
    document = _document([_rule(**{"legacy_form": "text", **overrides})])

    with pytest.raises(ContractInvalid, match="rule_legacy_form_invalid rule_id=R-1"):
        load_contract(_write(tmp_path, document))


def test_rejeita_forma_do_legado_fora_do_conjunto_fechado(tmp_path: Path) -> None:
    document = _document([_rule(legacy_form="iso")])

    with pytest.raises(ContractInvalid, match="Input should be"):
        load_contract(_write(tmp_path, document))
