"""Testes para glosas_builder."""
from datetime import datetime
from pathlib import Path

import pandas as pd

from analysis.glosas_builder import construir_glosas
from pipeline.state import PipelineState

_TS = datetime(2026, 3, 31, 12, 0, 0)
_COLUNAS = [
    "competencia",
    "regra",
    "cpf",
    "cns",
    "nome_profissional",
    "sexo",
    "cnes_estabelecimento",
    "motivo",
    "criado_em_firebird",
    "criado_em_pipeline",
    "atualizado_em_pipeline",
]


def _state() -> PipelineState:
    return PipelineState(
        competencia_ano=2026,
        competencia_mes=3,
        output_path=Path("data/processed/r.csv"),
        executar_nacional=False,
        executar_hr=False,
    )


def test_glosas_rq003b_colunas_corretas():
    state = _state()
    state.df_multi_unidades = pd.DataFrame(
        [{"CPF": "111", "CNS": "222", "NOME_PROFISSIONAL": "Ana", "SEXO": "F", "CNES": "999"}]
    )
    result = construir_glosas("2026-03", state, _TS)

    assert list(result.columns) == _COLUNAS
    row = result[result["regra"] == "RQ003B"].iloc[0]
    assert row["cpf"] == "111"
    assert row["cns"] == "222"
    assert row["nome_profissional"] == "Ana"
    assert row["sexo"] == "F"
    assert row["cnes_estabelecimento"] == "999"


def test_glosas_ghost_inclui_motivo():
    state = _state()
    state.df_ghost = pd.DataFrame(
        [
            {
                "CPF": "111",
                "CNS": "222",
                "NOME_PROFISSIONAL": "João",
                "SEXO": "M",
                "CNES": "888",
                "MOTIVO_GHOST": "CPF ausente",
            }
        ]
    )
    result = construir_glosas("2026-03", state, _TS)

    row = result[result["regra"] == "GHOST"].iloc[0]
    assert row["motivo"] == "CPF ausente"


def test_glosas_missing_sem_cns_nem_cnes():
    state = _state()
    state.df_missing = pd.DataFrame(
        [{"CPF": "333", "NOME_PROFISSIONAL": "Maria"}]
    )
    result = construir_glosas("2026-03", state, _TS)

    row = result[result["regra"] == "MISSING"].iloc[0]
    assert row["cpf"] == "333"
    assert row["cns"] is None or pd.isna(row["cns"])
    assert row["cnes_estabelecimento"] is None or pd.isna(row["cnes_estabelecimento"])


def test_glosas_rq010_motivo_literal():
    state = _state()
    state.df_cbo_diverg = pd.DataFrame([{"CNS": "444", "CNES": "777"}])
    result = construir_glosas("2026-03", state, _TS)

    row = result[result["regra"] == "RQ010"].iloc[0]
    assert row["motivo"] == "CBO_LOCAL != CBO_NACIONAL"


def test_glosas_rq011_delta_ch():
    state = _state()
    state.df_ch_diverg = pd.DataFrame([{"CNS": "555", "CNES": "666", "DELTA_CH": 5.0}])
    result = construir_glosas("2026-03", state, _TS)

    row = result[result["regra"] == "RQ011"].iloc[0]
    assert row["motivo"] == "DELTA_CH=5.0"


def test_glosas_df_vazio_retorna_schema_correto():
    state = _state()
    result = construir_glosas("2026-03", state, _TS)

    assert list(result.columns) == _COLUNAS
    assert len(result) == 0


def test_glosas_competencia_e_timestamps():
    state = _state()
    state.df_multi_unidades = pd.DataFrame(
        [{"CPF": "111", "CNS": "222", "NOME_PROFISSIONAL": "Ana", "SEXO": "F", "CNES": "999"}]
    )
    state.df_missing = pd.DataFrame([{"CPF": "333", "NOME_PROFISSIONAL": "Maria"}])
    result = construir_glosas("2026-03", state, _TS)

    assert (result["competencia"] == "2026-03").all()
    assert (result["criado_em_pipeline"] == _TS).all()
    assert (result["atualizado_em_pipeline"] == _TS).all()
    assert result["criado_em_firebird"].isna().all()
