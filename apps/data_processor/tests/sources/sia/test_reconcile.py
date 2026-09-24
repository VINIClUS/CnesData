"""TDD de reconcile_sia: totais exatos, BPI vs BPIHST sem precedência e proveniência."""

from __future__ import annotations

import json
from io import BytesIO
from typing import TYPE_CHECKING

import polars as pl
import pytest

from cnes_contracts.manifests.raw import SourceType
from cnes_contracts.manifests.validation import manifest_sha256
from data_processor.sources.sia.contract import PROVENANCE_METADATA_KEY, SiaContractError
from data_processor.sources.sia.reconcile import KPIS_METADATA_KEY, reconcile_sia

if TYPE_CHECKING:
    from .conftest import SiaHarness

_CONSULTA = "CONSULTA MEDICA EM ATENCAO ESPECIALIZADA"


def _frame(sia: SiaHarness, key: str) -> pl.DataFrame:
    return sia.read_frame(key)


def test_reconcilia_totais_exatos_por_cnes_procedimento_e_fonte(sia: SiaHarness) -> None:
    result = sia.reconcile_all()

    reconciled = _frame(sia, result.reconciliation_manifest.object_key)
    assert reconciled.rows() == [
        ("2026-01", "0077485", "0301010072", "SIA_APA", _CONSULTA, 1, 3, 45000),
        ("2026-01", "2077485", "0301010072", "SIA_APA", _CONSULTA, 2, 2, 30000),
        ("2026-01", "2077485", "0301010072", "SIA_BPI", _CONSULTA, 1, 3, None),
        ("2026-01", "2077485", "0301010072", "SIA_BPIHST", _CONSULTA, 2, 5, None),
        ("2026-01", "2077493", "0301010072", "SIA_BPI", _CONSULTA, 1, 1, None),
        ("2026-01", "2077493", "0304010286", "SIA_APA", None, 1, 2**31 - 1, 9_000_000_000),
    ]
    assert result.reconciliation_manifest.row_count == reconciled.height


def test_codigo_fora_do_sigtap_vira_divergencia_com_contador_sem_descartar(
    sia: SiaHarness,
) -> None:
    result = sia.reconcile_all()

    divergences = _frame(sia, result.divergence_manifest.object_key)
    unknown = divergences.filter(pl.col("tipo") == "procedimento_desconhecido")
    assert unknown.select("cnes", "cod_procedimento", "fonte", "linhas").rows() == [
        ("2077493", "0304010286", "SIA_APA", 1),
    ]
    assert result.kpis["procedimento_desconhecido"] == 1


def test_bpihst_nao_e_descartado_nem_preferido_sobre_bpi(sia: SiaHarness) -> None:
    result = sia.reconcile_all()

    reconciled = _frame(sia, result.reconciliation_manifest.object_key)
    by_fonte = reconciled.filter(pl.col("cnes") == "2077485").group_by("fonte").agg(
        pl.col("quantidade").sum()
    ).sort("fonte")
    assert by_fonte.rows() == [("SIA_APA", 2), ("SIA_BPI", 3), ("SIA_BPIHST", 5)]
    divergences = _frame(sia, result.divergence_manifest.object_key)
    candidates = divergences.filter(pl.col("tipo") == "candidato_duplicado_bpi_bpihst")
    assert candidates.select("cnes", "fonte", "referencias").rows() == [
        ("2077485", "SIA_BPI+SIA_BPIHST", ["SIA_BPI:0", "SIA_BPIHST:0"]),
    ]


def test_bpihst_zero_row_conta_como_slot_presente(sia: SiaHarness) -> None:
    result = sia.reconcile_all({"SIA_BPIHST": []})

    assert result.kpis["linhas_sia_bpihst"] == 0
    assert result.kpis["candidato_duplicado_bpi_bpihst"] == 0
    reconciled = _frame(sia, result.reconciliation_manifest.object_key)
    assert "SIA_BPIHST" not in reconciled["fonte"].to_list()


def test_kpis_fecham_contabilidade_de_todos_os_subtipos(sia: SiaHarness) -> None:
    result = sia.reconcile_all()

    raw_counts = {k: len(v) for k, v in sia.load_fixture("raw_rows.json").items()}
    for subtype, entrada in raw_counts.items():
        slug = subtype.lower()
        assert result.kpis[f"linhas_{slug}"] + result.kpis[f"qualidade_{slug}"] == entrada
    assert result.kpis["datas_invalidas_normalizadas"] == 1
    assert result.kpis["quantidade_total"] == 2**31 - 1 + 5 + 4 + 5
    assert result.kpis["valor_aprovado_cents_total"] == 9_000_075_000


def test_kpi_de_datas_invalidas_conta_somente_linhas_normalizadas(sia: SiaHarness) -> None:
    apa = sia.load_fixture("raw_rows.json")["SIA_APA"]
    rejected_with_bad_date = {**apa[6], "apa_cmp": "202512"}

    result = sia.reconcile_all({"SIA_APA": [apa[0], rejected_with_bad_date]})

    assert result.kpis["datas_invalidas_normalizadas"] == 0
    assert result.kpis["linhas_sia_apa"] == 1
    assert result.kpis["qualidade_sia_apa"] == 1


def test_registra_hashes_de_todos_os_raw_manifests_contribuintes(sia: SiaHarness) -> None:
    result = sia.reconcile_all()

    expected = sorted(manifest_sha256(raw) for raw in sia.raws)
    for manifest in (result.reconciliation_manifest, result.divergence_manifest):
        metadata = pl.read_parquet_metadata(BytesIO(sia.store.objects[manifest.object_key]))
        assert json.loads(metadata[PROVENANCE_METADATA_KEY]) == expected
    reconciliation = pl.read_parquet_metadata(
        BytesIO(sia.store.objects[result.reconciliation_manifest.object_key])
    )
    assert json.loads(reconciliation[KPIS_METADATA_KEY]) == result.kpis


def test_bytes_reconciliados_sao_idempotentes(sia: SiaHarness) -> None:
    request = sia.reconcile_request(sia.normalize_all())

    first = reconcile_sia(request, sia.store)
    second = reconcile_sia(request, sia.store)

    assert first == second


def test_exige_os_dez_manifests_normalizados(sia: SiaHarness) -> None:
    manifests = tuple(
        item for item in sia.normalize_all()
        if not item.object_key.endswith("quality_issues_dim_municipio.parquet")
    )

    with pytest.raises(SiaContractError, match=r"missing=quality_issues_dim_municipio\.parquet"):
        reconcile_sia(sia.reconcile_request(manifests), sia.store)


def test_rejeita_destinos_fora_do_layout(sia: SiaHarness) -> None:
    request = sia.reconcile_request(sia.normalize_all())
    key = request.reconciliation_key.replace("sia.parquet", "x.parquet")
    wrong = request.model_copy(update={"reconciliation_key": key})

    with pytest.raises(SiaContractError, match="sia_reconcile_targets_invalid"):
        reconcile_sia(wrong, sia.store)


def test_rejeita_manifest_normalizado_de_outra_fonte(sia: SiaHarness) -> None:
    manifests = sia.normalize_all()
    foreign = manifests[0].model_copy(update={"source_type": SourceType.SIHD})
    request = sia.reconcile_request(manifests).model_copy(
        update={"normalized_manifests": (foreign, *manifests[1:])}
    )

    with pytest.raises(SiaContractError, match="sia_normalized_source_invalid"):
        reconcile_sia(request, sia.store)


def test_rejeita_normalizado_adulterado(sia: SiaHarness) -> None:
    manifests = sia.normalize_all()
    sia.store.objects[manifests[0].object_key] += b"x"

    with pytest.raises(SiaContractError, match="input_sha256_mismatch"):
        reconcile_sia(sia.reconcile_request(manifests), sia.store)
