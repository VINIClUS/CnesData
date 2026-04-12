"""Testes para IngestaoNacionalStage sem cache DuckDB."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.state import PipelineState
from pipeline.stages.ingestao_nacional import IngestaoNacionalStage


def _state(executar_nacional: bool = True) -> PipelineState:
    return PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=executar_nacional,
    )


def _df_prof() -> pd.DataFrame:
    return pd.DataFrame({
        "CNS": ["123456789012345"],
        "CPF": [None],
        "NOME_PROFISSIONAL": ["Ana Silva"],
        "CBO": ["515105"],
        "CNES": ["1234567"],
        "TIPO_VINCULO": ["30"],
        "SUS": ["S"],
        "CH_TOTAL": [40],
        "CH_AMBULATORIAL": [20],
        "CH_OUTRAS": [10],
        "CH_HOSPITALAR": [10],
        "FONTE": ["NACIONAL"],
    })


def _df_estab() -> pd.DataFrame:
    return pd.DataFrame({
        "CNES": ["1234567"],
        "NOME_FANTASIA": [None],
        "TIPO_UNIDADE": ["01"],
        "CNPJ_MANTENEDORA": [None],
        "NATUREZA_JURIDICA": [None],
        "COD_MUNICIPIO": ["354130"],
        "VINCULO_SUS": ["S"],
        "FONTE": ["NACIONAL"],
    })


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
@patch("pipeline.stages.ingestao_nacional.config")
def test_popula_state_quando_executar_nacional_true(mock_config, mock_adapter_cls):
    mock_config.GCP_PROJECT_ID = "proj"
    mock_config.ID_MUNICIPIO_IBGE7 = "3541307"
    mock_config.CACHE_DIR = "/tmp"
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state(executar_nacional=True)
    IngestaoNacionalStage().execute(state)

    assert len(state.df_prof_nacional) == 1
    assert len(state.df_estab_nacional) == 1


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
def test_mantém_dfs_vazios_quando_skip(mock_adapter_cls):
    state = _state(executar_nacional=False)
    IngestaoNacionalStage().execute(state)

    assert state.df_prof_nacional.empty
    assert state.df_estab_nacional.empty
    mock_adapter_cls.assert_not_called()


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
@patch("pipeline.stages.ingestao_nacional.config")
def test_soft_fail_deixa_dfs_vazios_em_excecao(mock_config, mock_adapter_cls):
    mock_config.GCP_PROJECT_ID = "proj"
    mock_config.ID_MUNICIPIO_IBGE7 = "3541307"
    mock_config.CACHE_DIR = "/tmp"
    mock_adapter_cls.side_effect = Exception("BigQuery timeout")

    state = _state(executar_nacional=True)
    IngestaoNacionalStage().execute(state)

    assert state.df_prof_nacional.empty
    assert state.df_estab_nacional.empty
    assert state.nacional_disponivel is False


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
@patch("pipeline.stages.ingestao_nacional.config")
def test_busca_profissionais_e_estabelecimentos_em_paralelo(mock_config, mock_adapter_cls):
    mock_config.GCP_PROJECT_ID = "proj"
    mock_config.ID_MUNICIPIO_IBGE7 = "3541307"
    mock_config.CACHE_DIR = "/tmp"
    calls = []
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.side_effect = lambda c: calls.append("prof") or _df_prof()
    mock_adapter.listar_estabelecimentos.side_effect = lambda c: calls.append("estab") or _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    IngestaoNacionalStage().execute(_state(executar_nacional=True))

    assert "prof" in calls and "estab" in calls


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
@patch("pipeline.stages.ingestao_nacional.config")
def test_seta_nacional_disponivel_apos_busca(mock_config, mock_adapter_cls):
    mock_config.GCP_PROJECT_ID = "proj"
    mock_config.ID_MUNICIPIO_IBGE7 = "3543105"
    mock_config.CACHE_DIR = "/tmp"
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state(executar_nacional=True)
    IngestaoNacionalStage().execute(state)

    assert state.nacional_disponivel is True
