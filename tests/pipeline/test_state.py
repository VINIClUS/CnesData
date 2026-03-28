from pathlib import Path

import pandas as pd
import pytest

from pipeline.state import PipelineState


def test_construcao_minima_preenche_defaults():
    state = PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=False,
    )

    assert state.competencia_ano == 2024
    assert state.competencia_mes == 12
    assert state.con is None
    assert state.cbo_lookup == {}
    assert isinstance(state.df_prof_local, pd.DataFrame)
    assert state.df_prof_local.empty
    assert state.df_estab_local.empty
    assert state.df_prof_nacional.empty
    assert state.df_estab_nacional.empty
    assert state.df_processado.empty
    assert state.df_multi_unidades.empty
    assert state.df_acs_incorretos.empty
    assert state.df_ace_incorretos.empty
    assert state.df_ghost.empty
    assert state.df_missing.empty
    assert state.df_estab_fantasma.empty
    assert state.df_estab_ausente.empty
    assert state.df_prof_fantasma.empty
    assert state.df_prof_ausente.empty
    assert state.df_cbo_diverg.empty
    assert state.df_ch_diverg.empty


def test_competencia_str_formata_corretamente():
    state = PipelineState(
        competencia_ano=2024,
        competencia_mes=3,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=False,
        executar_hr=False,
    )

    assert state.competencia_str == "2024-03"
