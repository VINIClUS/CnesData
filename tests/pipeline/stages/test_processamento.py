from pathlib import Path
from unittest.mock import patch

import pandas as pd

from pipeline.state import PipelineState
from pipeline.stages.processamento import ProcessamentoStage


def _state_com_prof() -> PipelineState:
    state = PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=False,
    )
    state.cbo_lookup = {"515105": "Agente Comunitário"}
    state.df_prof_local = pd.DataFrame({"CPF": ["12345678901"], "CNES": ["1234567"]})
    return state


@patch("pipeline.stages.processamento.transformar")
def test_chama_transformar_com_cbo_lookup(mock_transformar):
    df_transformado = pd.DataFrame({"CPF": ["12345678901"]})
    mock_transformar.return_value = df_transformado

    state = _state_com_prof()
    ProcessamentoStage().execute(state)

    mock_transformar.assert_called_once_with(
        state.df_prof_local, cbo_lookup={"515105": "Agente Comunitário"}
    )
    assert state.df_processado is df_transformado


@patch("pipeline.stages.processamento.transformar")
def test_df_processado_populado_no_state(mock_transformar):
    df_resultado = pd.DataFrame({"CPF": ["99988877766"]})
    mock_transformar.return_value = df_resultado

    state = _state_com_prof()
    ProcessamentoStage().execute(state)

    assert len(state.df_processado) == 1
    assert state.df_processado["CPF"].iloc[0] == "99988877766"
