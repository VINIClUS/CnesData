"""Contrato Edge ↔ processor: Parquet real do dump_agent_go passa pelos adapters SIA."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from data_processor.adapters.sia_adapter import canonicalize_apa, canonicalize_bpi
from data_processor.adapters.sia_dim_sync import build_reference_municipio, build_reference_sigtap
from data_processor.sources.sia.normalize import normalize_sia

from .conftest import SUBTYPES, SiaHarness

# Regenerado por `go test ./internal/writer -update-sia-golden` em apps/dump_agent_go.
_GOLDEN = Path(__file__).resolve().parents[2] / "fixtures" / "sia" / "edge_golden"


def _golden(subtype: str) -> pl.DataFrame:
    return pl.read_parquet(_GOLDEN / f"{subtype}.parquet")


def test_apa_do_edge_canonicaliza_com_join_do_cabecalho() -> None:
    canonical = canonicalize_apa(_golden("SIA_APA"))

    assert canonical.height == 4
    assert canonical["cnes"].unique().to_list() == ["2269481"]
    assert canonical["quantidade"].to_list() == [2, 2, 2, 2]
    assert canonical["valor_aprovado_cents"].to_list() == [15025, 15025, None, 15025]
    assert canonical["dt_inicio_invalida"].to_list() == [False, False, False, True]


@pytest.mark.parametrize(("subtype", "rows"), [("SIA_BPI", 8), ("SIA_BPIHST", 12)])
def test_bpi_do_edge_canonicaliza(subtype: str, rows: int) -> None:
    canonical = canonicalize_bpi(_golden(subtype), subtype)

    assert canonical.height == rows
    assert not canonical["dt_atendimento_invalida"].any()
    assert canonical["folha"].to_list() == [1] * rows
    assert canonical["dt_atendimento"].min() >= date(2025, 12, 1)


def test_sigtap_do_edge_segue_layout_tb_procedimento() -> None:
    reference = build_reference_sigtap(_golden("DIM_SIGTAP"))

    assert reference["cod_procedimento"].null_count() == 0
    assert reference["competencia_sigtap"].unique().to_list() == ["2026-01"]
    assert set(reference["complexidade"]) <= {"0", "1", "2", "3"}


def test_cadmun_do_edge_gera_ibge6() -> None:
    reference = build_reference_municipio(_golden("DIM_MUNICIPIO"))

    assert reference["ibge6"].to_list() == ["354130", "355030"]


def test_golden_do_edge_normaliza_todos_os_subtipos(sia: SiaHarness) -> None:
    for subtype in SUBTYPES:
        raw = sia.put_raw(subtype, _golden(subtype))
        result = normalize_sia(sia.normalize_request(raw), sia.store)
        assert len(result.manifests) == 2, subtype
