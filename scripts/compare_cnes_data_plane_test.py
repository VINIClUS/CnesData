"""Testes do gate de comparação golden/shadow do vertical slice CNES."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import polars as pl
import pytest

from scripts.compare_cnes_data_plane import (
    APPROVED_RULES,
    DIVERGENCE_FILE,
    GOLD_FILE,
    KPIS_FILE,
    MANIFEST_FILE,
    NORMALIZED_IDS_FILE,
    SERVING_FILE,
    ComparisonDifference,
    UnexplainedDifference,
    compare_outputs,
    require_explained,
    volatile_normalized,
)

VERSIONED_ROOT = Path(__file__).parents[1] / "docs" / "fixtures" / "data-plane"

_KPIS = {
    "match_count": 3, "local_only_count": 2, "national_only_count": 2,
    "conflict_count": 1, "reconciled_row_count": 7, "active_professional_count": 6,
}
_DIVERGENCES = [
    {
        "natural_key": {
            "identity": "999000000000002", "CNES": "1234567",
            "CBO": "225125", "COMPETENCIA": "2026-01",
        },
        "field": "CH_TOTAL",
        "local_value": 40,
        "national_value": 30,
        "selected_value": 40,
        "selected_source": "LOCAL",
        "source_manifest_ids": ["fixture-cnes-local-v1", "fixture-cnes-nacional-v1"],
    },
]
_SERVING = {
    "schema_version": "cnes-serving-v1",
    "tenant_id": "354130",
    "run_id": "fixture-cnes-run-v1",
    "generated_at": "2026-01-31T23:59:59Z",
    "competencia": "2026-01",
    "kpis": _KPIS,
    "divergence_counts": {"CH_TOTAL": 1},
    "missing_sources": [],
}
_VOLATILE_FIELDS = ["run_id", "generated_at"]


def _gold_frame() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "CPF": "90000000001", "CNS": "999000000000001",
                "NOME_PROFISSIONAL": "PROFISSIONAL TESTE 001", "NOME_SOCIAL": None,
                "SEXO": "F", "CBO": "225125", "CNES": "1234567",
                "TIPO_VINCULO": "01", "SUS": "S", "CH_TOTAL": 40,
                "CH_AMBULATORIAL": 20, "CH_OUTRAS": 0, "CH_HOSPITALAR": 20,
                "COMPETENCIA": "2026-01",
                "_source_manifest_ids": ["fixture-cnes-local-v1", "fixture-cnes-nacional-v1"],
            },
        ],
        schema={
            "CPF": pl.String, "CNS": pl.String, "NOME_PROFISSIONAL": pl.String,
            "NOME_SOCIAL": pl.String, "SEXO": pl.String, "CBO": pl.String,
            "CNES": pl.String, "TIPO_VINCULO": pl.String, "SUS": pl.String,
            "CH_TOTAL": pl.Int64, "CH_AMBULATORIAL": pl.Int64, "CH_OUTRAS": pl.Int64,
            "CH_HOSPITALAR": pl.Int64, "COMPETENCIA": pl.String,
            "_source_manifest_ids": pl.List(pl.String),
        },
    )


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_directory(
    root: Path,
    *,
    gold: pl.DataFrame | None = None,
    divergences: list[dict] | None = None,
    serving: dict | None = None,
    kpis: dict | None = None,
    volatile_fields: list[str] | None = None,
    run_ids: dict | None = None,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (gold if gold is not None else _gold_frame()).write_parquet(root / GOLD_FILE)
    _write_json(root / DIVERGENCE_FILE, divergences if divergences is not None else _DIVERGENCES)
    _write_json(root / SERVING_FILE, serving if serving is not None else _SERVING)
    _write_json(root / KPIS_FILE, kpis if kpis is not None else _KPIS)
    _write_json(
        root / MANIFEST_FILE,
        {"volatile_fields": volatile_fields if volatile_fields is not None else _VOLATILE_FIELDS},
    )
    if run_ids is not None:
        _write_json(root / NORMALIZED_IDS_FILE, run_ids)
    return root


def test_compare_outputs_detecta_toda_diferenca_fora_dos_campos_volateis(tmp_path):
    expected = _write_directory(tmp_path / "expected")
    changed_gold = _gold_frame().with_columns(pl.lit(41, dtype=pl.Int64).alias("CH_TOTAL"))
    actual = _write_directory(tmp_path / "actual", gold=changed_gold)

    differences = compare_outputs(expected, actual)

    matches = [d for d in differences if d.layer == "gold" and d.field == "CH_TOTAL"]
    assert len(matches) == 1
    assert matches[0].expected == 40
    assert matches[0].actual == 41
    assert matches[0].rule == ""


def test_campos_volateis_run_id_e_generated_at_sao_ignorados(tmp_path):
    expected = _write_directory(tmp_path / "expected")
    other_serving = {**_SERVING, "run_id": "some-other-run", "generated_at": "2099-01-01T00:00:00Z"}
    actual = _write_directory(tmp_path / "actual", serving=other_serving)

    differences = compare_outputs(expected, actual)

    assert differences == ()


def test_require_explained_levanta_para_diferenca_nao_aprovada(tmp_path):
    expected = _write_directory(tmp_path / "expected")
    changed_gold = _gold_frame().with_columns(pl.lit(41, dtype=pl.Int64).alias("CH_TOTAL"))
    actual = _write_directory(tmp_path / "actual", gold=changed_gold)
    differences = compare_outputs(expected, actual)

    with pytest.raises(UnexplainedDifference, match="field=CH_TOTAL"):
        require_explained(differences, frozenset())


def test_require_explained_aceita_diferenca_com_regra_aprovada(tmp_path):
    differences = (
        ComparisonDifference("gold", "k", "CH_TOTAL", 40, 41, rule="uma-regra-aprovada"),
    )

    require_explained(differences, frozenset({"uma-regra-aprovada"}))


def test_schema_divergente_produz_diferenca_de_schema(tmp_path):
    expected = _write_directory(tmp_path / "expected")
    bad_schema_gold = _gold_frame().with_columns(pl.col("CH_TOTAL").cast(pl.String))
    actual = _write_directory(tmp_path / "actual", gold=bad_schema_gold)

    differences = compare_outputs(expected, actual)

    assert any(d.layer == "gold" and d.field == "__schema__" for d in differences)


def _render(doc: dict) -> bytes:
    return (json.dumps(doc, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def test_serving_compara_bytes_apos_substituir_campos_volateis():
    expected_doc = _SERVING
    actual_doc = {**_SERVING, "run_id": "run-golden-001", "generated_at": "2026-05-01T00:00:00Z"}

    normalized_expected = volatile_normalized(expected_doc, ("run_id", "generated_at"))
    normalized_actual = volatile_normalized(actual_doc, ("run_id", "generated_at"))

    assert _render(normalized_expected) == _render(normalized_actual)


def test_nenhuma_tolerancia_numerica_e_aplicada(tmp_path):
    expected = _write_directory(tmp_path / "expected")
    float_gold = _gold_frame().with_columns(pl.col("CH_TOTAL").cast(pl.Float64) + 1e-9)
    actual = _write_directory(tmp_path / "actual", gold=float_gold)

    differences = compare_outputs(expected, actual)

    assert any(d.layer == "gold" for d in differences)


def test_regra_de_manifest_ids_rejeita_aridade_ou_posicao_errada(tmp_path):
    expected = _write_directory(tmp_path / "expected")
    wrong_arity_gold = _gold_frame().with_columns(
        pl.Series("_source_manifest_ids", [["so-um-id"]], dtype=pl.List(pl.String))
    )
    actual = _write_directory(tmp_path / "actual", gold=wrong_arity_gold)

    differences = compare_outputs(expected, actual)

    matches = [d for d in differences if d.field == "_source_manifest_ids"]
    assert len(matches) == 1
    assert matches[0].rule == ""


def test_regra_de_manifest_ids_aceita_ids_normalizados_na_camada_certa(tmp_path):
    expected = _write_directory(tmp_path / "expected")
    normalized_gold = _gold_frame().with_columns(
        pl.Series(
            "_source_manifest_ids",
            [["normalized-run-1-unit-local-1", "normalized-run-1-unit-nacional-1"]],
            dtype=pl.List(pl.String),
        )
    )
    actual = _write_directory(tmp_path / "actual", gold=normalized_gold)

    differences = compare_outputs(expected, actual)

    matches = [d for d in differences if d.field == "_source_manifest_ids"]
    assert len(matches) == 1
    assert matches[0].rule == "manifest_ids_are_normalized_layer"
    assert matches[0].rule in APPROVED_RULES


def test_regra_de_manifest_ids_com_contexto_exige_id_exato_no_slot_certo(tmp_path):
    expected = _write_directory(tmp_path / "expected")
    right_slot_gold = _gold_frame().with_columns(
        pl.Series(
            "_source_manifest_ids",
            [["normalized-run-1-unit-local-1", "normalized-run-1-unit-nacional-1"]],
            dtype=pl.List(pl.String),
        )
    )
    run_ids = {
        "local": "normalized-run-1-unit-local-1", "national": "normalized-run-1-unit-nacional-1"
    }
    actual = _write_directory(tmp_path / "actual", gold=right_slot_gold, run_ids=run_ids)

    differences = compare_outputs(expected, actual)

    matches = [d for d in differences if d.field == "_source_manifest_ids"]
    assert len(matches) == 1
    assert matches[0].rule == "manifest_ids_are_normalized_layer"


def test_regra_de_manifest_ids_com_contexto_rejeita_slots_trocados(tmp_path):
    expected = _write_directory(tmp_path / "expected")
    swapped_gold = _gold_frame().with_columns(
        pl.Series(
            "_source_manifest_ids",
            [["normalized-run-1-unit-nacional-1", "normalized-run-1-unit-local-1"]],
            dtype=pl.List(pl.String),
        )
    )
    run_ids = {
        "local": "normalized-run-1-unit-local-1", "national": "normalized-run-1-unit-nacional-1"
    }
    actual = _write_directory(tmp_path / "actual", gold=swapped_gold, run_ids=run_ids)

    differences = compare_outputs(expected, actual)

    matches = [d for d in differences if d.field == "_source_manifest_ids"]
    assert len(matches) == 1
    assert matches[0].rule == ""


def test_regra_de_manifest_ids_nao_cobre_outros_campos(tmp_path):
    expected = _write_directory(tmp_path / "expected")
    changed_gold = _gold_frame().with_columns(pl.lit("M").alias("SEXO"))
    actual = _write_directory(tmp_path / "actual", gold=changed_gold)

    differences = compare_outputs(expected, actual)

    matches = [d for d in differences if d.field == "SEXO"]
    assert len(matches) == 1
    assert matches[0].rule == ""


def test_regra_de_valores_string_exige_str_exato(tmp_path):
    expected = _write_directory(tmp_path / "expected")

    exact_str = [{**_DIVERGENCES[0], "local_value": "40"}]
    actual_exact = _write_directory(tmp_path / "actual_exact", divergences=exact_str)
    exact_diffs = compare_outputs(expected, actual_exact)
    local_value_exact = [d for d in exact_diffs if d.field == "local_value"]
    assert len(local_value_exact) == 1
    assert local_value_exact[0].rule == "divergence_values_are_strings"
    assert local_value_exact[0].rule in APPROVED_RULES

    wrong_str = [{**_DIVERGENCES[0], "local_value": "41"}]
    actual_wrong = _write_directory(tmp_path / "actual_wrong", divergences=wrong_str)
    wrong_diffs = compare_outputs(expected, actual_wrong)
    local_value_wrong = [d for d in wrong_diffs if d.field == "local_value"]
    assert len(local_value_wrong) == 1
    assert local_value_wrong[0].rule == ""


def test_regra_de_valores_string_nao_cobre_outros_campos(tmp_path):
    expected = _write_directory(tmp_path / "expected")
    changed = [{**_DIVERGENCES[0], "selected_source": "40"}]
    actual = _write_directory(tmp_path / "actual", divergences=changed)

    differences = compare_outputs(expected, actual)

    matches = [d for d in differences if d.field == "selected_source"]
    assert len(matches) == 1
    assert matches[0].rule == ""


def test_valida_conjunto_versionado_copiado_bate_consigo_mesmo(tmp_path):
    manifest = json.loads((VERSIONED_ROOT / MANIFEST_FILE).read_text(encoding="utf-8"))
    expected = tmp_path / "expected"
    expected.mkdir()
    shutil.copy(VERSIONED_ROOT / GOLD_FILE, expected / GOLD_FILE)
    shutil.copy(VERSIONED_ROOT / SERVING_FILE, expected / SERVING_FILE)
    shutil.copy(VERSIONED_ROOT / MANIFEST_FILE, expected / MANIFEST_FILE)
    _write_json(expected / DIVERGENCE_FILE, manifest["divergences"])
    _write_json(expected / KPIS_FILE, manifest["kpis"])
    actual = tmp_path / "actual"
    shutil.copytree(expected, actual)

    differences = compare_outputs(expected, actual)

    assert differences == ()
