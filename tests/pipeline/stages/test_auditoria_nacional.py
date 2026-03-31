from pathlib import Path
from unittest.mock import patch

import pandas as pd

from pipeline.state import PipelineState
from pipeline.stages.auditoria_nacional import AuditoriaNacionalStage


def _state() -> PipelineState:
    state = PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=False,
    )
    state.df_processado = pd.DataFrame({"CPF": ["12345678901"], "CNES": ["1234567"]})
    state.df_estab_local = pd.DataFrame({"CNES": ["1234567"], "TIPO_UNIDADE": ["01"]})
    state.df_prof_nacional = pd.DataFrame({"CNS": ["123456789012345"]})
    state.df_estab_nacional = pd.DataFrame({"CNES": ["1234567"]})
    return state


@patch("pipeline.stages.auditoria_nacional.detectar_divergencia_carga_horaria")
@patch("pipeline.stages.auditoria_nacional.detectar_divergencia_cbo")
@patch("pipeline.stages.auditoria_nacional.detectar_profissionais_ausentes_local")
@patch("pipeline.stages.auditoria_nacional.detectar_profissionais_fantasma")
@patch("pipeline.stages.auditoria_nacional.detectar_estabelecimentos_ausentes_local")
@patch("pipeline.stages.auditoria_nacional.detectar_estabelecimentos_fantasma")
def test_skip_quando_nacionais_vazios(
    mock_efant, mock_eaus, mock_pfant, mock_paus, mock_cbo, mock_ch,
):
    state = _state()
    state.df_estab_nacional = pd.DataFrame()
    state.df_prof_nacional = pd.DataFrame()
    AuditoriaNacionalStage().execute(state)
    mock_efant.assert_not_called()
    mock_eaus.assert_not_called()
    mock_pfant.assert_not_called()
    mock_paus.assert_not_called()
    mock_cbo.assert_not_called()
    mock_ch.assert_not_called()


@patch("pipeline.stages.auditoria_nacional.config")
@patch("pipeline.stages.auditoria_nacional.CnesOficialWebAdapter")
@patch("pipeline.stages.auditoria_nacional.CachingVerificadorCnes")
@patch("pipeline.stages.auditoria_nacional.resolver_lag_rq006", return_value=pd.DataFrame())
@patch(
    "pipeline.stages.auditoria_nacional.detectar_divergencia_carga_horaria",
    return_value=pd.DataFrame(),
)
@patch(
    "pipeline.stages.auditoria_nacional.detectar_divergencia_cbo",
    return_value=pd.DataFrame(),
)
@patch(
    "pipeline.stages.auditoria_nacional.detectar_profissionais_ausentes_local",
    return_value=pd.DataFrame(),
)
@patch(
    "pipeline.stages.auditoria_nacional.detectar_profissionais_fantasma",
    return_value=pd.DataFrame(),
)
@patch(
    "pipeline.stages.auditoria_nacional.detectar_estabelecimentos_ausentes_local",
    return_value=pd.DataFrame(),
)
@patch(
    "pipeline.stages.auditoria_nacional.detectar_estabelecimentos_fantasma",
    return_value=pd.DataFrame(),
)
def test_cruzamento_executado_com_dados_nacionais(
    mock_efant, mock_eaus, mock_pfant, mock_paus, mock_cbo, mock_ch,
    mock_resolver, mock_caching, mock_web, mock_config,
):
    mock_config.CACHE_DIR = Path("data/cache")
    state = _state()
    AuditoriaNacionalStage().execute(state)
    mock_pfant.assert_called_once()
    mock_cbo.assert_called_once()
    mock_ch.assert_called_once()


@patch("pipeline.stages.auditoria_nacional.config")
@patch("pipeline.stages.auditoria_nacional.CnesOficialWebAdapter")
@patch("pipeline.stages.auditoria_nacional.CachingVerificadorCnes")
@patch("pipeline.stages.auditoria_nacional.resolver_lag_rq006", return_value=pd.DataFrame())
@patch(
    "pipeline.stages.auditoria_nacional.detectar_divergencia_carga_horaria",
    return_value=pd.DataFrame(),
)
@patch(
    "pipeline.stages.auditoria_nacional.detectar_divergencia_cbo",
    return_value=pd.DataFrame(),
)
@patch(
    "pipeline.stages.auditoria_nacional.detectar_profissionais_ausentes_local",
    return_value=pd.DataFrame(),
)
@patch(
    "pipeline.stages.auditoria_nacional.detectar_profissionais_fantasma",
    return_value=pd.DataFrame(),
)
@patch(
    "pipeline.stages.auditoria_nacional.detectar_estabelecimentos_ausentes_local",
    return_value=pd.DataFrame({"CNES": ["9999999"]}),
)
@patch(
    "pipeline.stages.auditoria_nacional.detectar_estabelecimentos_fantasma",
    return_value=pd.DataFrame(),
)
def test_cnes_excluir_propagado_de_estab_ausente(
    mock_efant, mock_eaus, mock_pfant, mock_paus, mock_cbo, mock_ch,
    mock_resolver, mock_caching, mock_web, mock_config,
):
    mock_config.CACHE_DIR = Path("data/cache")
    state = _state()
    AuditoriaNacionalStage().execute(state)
    _, kwargs = mock_paus.call_args
    assert kwargs.get("cnes_excluir") == frozenset({"9999999"})
