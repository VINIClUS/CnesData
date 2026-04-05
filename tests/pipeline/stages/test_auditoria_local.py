from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.state import PipelineState
from pipeline.stages.auditoria_local import AuditoriaLocalStage


def test_skip_quando_local_indisponivel():
    state = PipelineState(
        competencia_ano=2024, competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=False, executar_hr=False,
        local_disponivel=False,
    )
    AuditoriaLocalStage().execute(state)
    assert state.df_multi_unidades.empty
    assert state.df_acs_incorretos.empty


def _state() -> PipelineState:
    state = PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=True,
    )
    state.df_processado = pd.DataFrame({"CPF": ["12345678901"], "CNES": ["1234567"]})
    state.df_estab_local = pd.DataFrame({"CNES": ["1234567"], "TIPO_UNIDADE": ["01"]})
    return state


@patch("pipeline.stages.auditoria_local.config")
@patch("pipeline.stages.auditoria_local.detectar_registro_ausente", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.detectar_folha_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.carregar_folha", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.auditar_lotacao_ace_tace", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.auditar_lotacao_acs_tacs", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.detectar_multiplas_unidades", return_value=pd.DataFrame())
def test_regras_locais_sempre_executam(
    mock_multi, mock_acs, mock_ace, mock_folha, mock_ghost, mock_reg_aus, mock_config,
):
    mock_config.FOLHA_HR_PATH = MagicMock()
    mock_config.FOLHA_HR_PATH.exists.return_value = True
    state = _state()
    AuditoriaLocalStage().execute(state)
    mock_multi.assert_called_once()
    mock_acs.assert_called_once()
    mock_ace.assert_called_once()


@patch("pipeline.stages.auditoria_local.config")
@patch("pipeline.stages.auditoria_local.detectar_registro_ausente", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.detectar_folha_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.carregar_folha", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.auditar_lotacao_ace_tace", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.auditar_lotacao_acs_tacs", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.detectar_multiplas_unidades", return_value=pd.DataFrame())
def test_hr_skipped_quando_executar_hr_false(
    mock_multi, mock_acs, mock_ace, mock_folha, mock_ghost, mock_reg_aus, mock_config,
):
    mock_config.FOLHA_HR_PATH = None
    state = _state()
    state.executar_hr = False
    AuditoriaLocalStage().execute(state)
    mock_folha.assert_not_called()
    mock_ghost.assert_not_called()
    mock_reg_aus.assert_not_called()


@patch("pipeline.stages.auditoria_local.config")
@patch(
    "pipeline.stages.auditoria_local.detectar_registro_ausente",
    return_value=pd.DataFrame({"CPF": ["00000000001"]}),
)
@patch(
    "pipeline.stages.auditoria_local.detectar_folha_fantasma",
    return_value=pd.DataFrame({"CPF": ["99999999999"]}),
)
@patch("pipeline.stages.auditoria_local.carregar_folha", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.auditar_lotacao_ace_tace", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.auditar_lotacao_acs_tacs", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.detectar_multiplas_unidades", return_value=pd.DataFrame())
def test_hr_ghost_e_missing_preenchidos_quando_hr_ativo(
    mock_multi, mock_acs, mock_ace, mock_folha, mock_ghost, mock_reg_aus, mock_config,
):
    mock_config.FOLHA_HR_PATH = MagicMock()
    mock_config.FOLHA_HR_PATH.exists.return_value = True
    state = _state()
    AuditoriaLocalStage().execute(state)
    mock_folha.assert_called_once()
    mock_ghost.assert_called_once()
    mock_reg_aus.assert_called_once()
    assert not state.df_ghost.empty
    assert not state.df_missing.empty
