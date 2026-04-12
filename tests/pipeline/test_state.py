from pathlib import Path

import pandas as pd

from pipeline.state import PipelineState


def _state(**kwargs) -> PipelineState:
    return PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        **kwargs,
    )


def test_construcao_minima_preenche_defaults():
    state = _state()
    assert state.competencia_ano == 2024
    assert state.competencia_mes == 12
    assert state.cbo_lookup == {}
    assert isinstance(state.df_prof_local, pd.DataFrame)
    assert state.df_prof_local.empty
    assert state.df_estab_local.empty
    assert state.df_prof_nacional.empty
    assert state.df_estab_nacional.empty
    assert state.df_processado.empty


def test_competencia_str_formata_corretamente():
    state = PipelineState(
        competencia_ano=2024,
        competencia_mes=3,
        output_path=Path("data/processed/report.csv"),
    )
    assert state.competencia_str == "2024-03"


def test_state_target_source_default_local():
    assert _state().target_source == "LOCAL"


def test_state_target_source_aceita_nacional():
    assert _state(target_source="NACIONAL").target_source == "NACIONAL"


def test_state_target_source_aceita_ambos():
    assert _state(target_source="AMBOS").target_source == "AMBOS"


def test_state_force_reingestao_default_false():
    assert _state().force_reingestao is False
