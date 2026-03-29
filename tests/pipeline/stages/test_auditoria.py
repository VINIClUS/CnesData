from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.state import PipelineState
from pipeline.stages.auditoria import AuditoriaStage


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
    state.df_prof_nacional = pd.DataFrame({"CNS": ["123456789012345"]})
    state.df_estab_nacional = pd.DataFrame({"CNES": ["1234567"]})
    return state


@patch("pipeline.stages.auditoria.config")
@patch("pipeline.stages.auditoria.CnesOficialWebAdapter")
@patch("pipeline.stages.auditoria.CachingVerificadorCnes")
@patch("pipeline.stages.auditoria.resolver_lag_rq006", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_divergencia_carga_horaria", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_divergencia_cbo", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_profissionais_ausentes_local", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_profissionais_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_estabelecimentos_ausentes_local", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_estabelecimentos_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_registro_ausente", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_folha_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.carregar_folha", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.auditar_lotacao_ace_tace", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.auditar_lotacao_acs_tacs", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_multiplas_unidades", return_value=pd.DataFrame())
def test_regras_locais_sempre_executam(
    mock_multi, mock_acs, mock_ace, mock_folha, mock_ghost, mock_reg_aus,
    mock_efant, mock_eaus, mock_pfant, mock_paus, mock_cbo, mock_ch,
    mock_resolver, mock_caching, mock_web, mock_config,
):
    mock_config.FOLHA_HR_PATH = MagicMock()
    mock_config.FOLHA_HR_PATH.exists.return_value = True
    mock_config.CACHE_DIR = Path("data/cache")
    state = _state()
    AuditoriaStage().execute(state)
    mock_multi.assert_called_once()
    mock_acs.assert_called_once()
    mock_ace.assert_called_once()


@patch("pipeline.stages.auditoria.config")
@patch("pipeline.stages.auditoria.CnesOficialWebAdapter")
@patch("pipeline.stages.auditoria.CachingVerificadorCnes")
@patch("pipeline.stages.auditoria.resolver_lag_rq006", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_divergencia_carga_horaria", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_divergencia_cbo", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_profissionais_ausentes_local", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_profissionais_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_estabelecimentos_ausentes_local", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_estabelecimentos_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_registro_ausente", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_folha_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.carregar_folha", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.auditar_lotacao_ace_tace", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.auditar_lotacao_acs_tacs", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_multiplas_unidades", return_value=pd.DataFrame())
def test_hr_skipped_quando_executar_hr_false(
    mock_multi, mock_acs, mock_ace, mock_folha, mock_ghost, mock_reg_aus,
    mock_efant, mock_eaus, mock_pfant, mock_paus, mock_cbo, mock_ch,
    mock_resolver, mock_caching, mock_web, mock_config,
):
    mock_config.FOLHA_HR_PATH = None
    mock_config.CACHE_DIR = Path("data/cache")
    state = _state()
    state.executar_hr = False
    AuditoriaStage().execute(state)
    mock_folha.assert_not_called()
    mock_ghost.assert_not_called()
    mock_reg_aus.assert_not_called()
