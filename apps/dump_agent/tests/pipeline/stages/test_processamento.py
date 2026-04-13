from pathlib import Path
from unittest.mock import patch

import polars as pl
from cnes_domain.pipeline.state import PipelineState
from dump_agent.stages.processamento import ProcessamentoStage


def _state_com_prof() -> PipelineState:
    state = PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
    )
    state.cbo_lookup = {"515105": "Agente Comunitário"}
    state.df_prof_local = pl.DataFrame({"CPF": ["12345678901"], "CNES": ["1234567"]})
    return state


@patch("dump_agent.stages.processamento.transformar")
def test_chama_transformar_com_cbo_lookup(mock_transformar):
    df_transformado = pl.DataFrame({"CPF": ["12345678901"]})
    mock_transformar.return_value = df_transformado

    state = _state_com_prof()
    ProcessamentoStage().execute(state)

    mock_transformar.assert_called_once_with(
        state.df_prof_local, cbo_lookup={"515105": "Agente Comunitário"}
    )
    assert state.df_processado is df_transformado


@patch("dump_agent.stages.processamento.transformar")
def test_df_processado_populado_no_state(mock_transformar):
    df_resultado = pl.DataFrame({"CPF": ["99988877766"]})
    mock_transformar.return_value = df_resultado

    state = _state_com_prof()
    ProcessamentoStage().execute(state)

    assert len(state.df_processado) == 1
    assert state.df_processado["CPF"][0] == "99988877766"


def test_skip_quando_df_prof_local_vazio():
    state = PipelineState(
        competencia_ano=2024, competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
    )
    ProcessamentoStage().execute(state)
    assert state.df_processado.is_empty()
