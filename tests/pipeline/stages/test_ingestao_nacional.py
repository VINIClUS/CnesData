from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.state import PipelineState
from pipeline.stages.ingestao_nacional import IngestaoNacionalStage, _computar_fingerprint
from storage.database_loader import DatabaseLoader


def _state(executar_nacional: bool = True) -> PipelineState:
    return PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=executar_nacional,
        executar_hr=False,
    )


def _df_processado() -> pd.DataFrame:
    return pd.DataFrame({
        "CPF": ["12345678900", "98765432100"],
        "CBO": ["515105", "223505"],
        "CNES": ["1234567", "7654321"],
    })


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


def _make_stage(db_loader=None):
    if db_loader is None:
        db_loader = MagicMock(spec=DatabaseLoader)
        db_loader.ler_cache_nacional.return_value = None
    return IngestaoNacionalStage(db_loader)


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
def test_popula_state_quando_executar_nacional_true(mock_adapter_cls):
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state(executar_nacional=True)
    _make_stage().execute(state)

    assert len(state.df_prof_nacional) == 1
    assert len(state.df_estab_nacional) == 1


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
def test_mantém_dfs_vazios_quando_skip(mock_adapter_cls):
    state = _state(executar_nacional=False)
    _make_stage().execute(state)

    assert state.df_prof_nacional.empty
    assert state.df_estab_nacional.empty
    mock_adapter_cls.assert_not_called()


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
def test_soft_fail_deixa_dfs_vazios_em_excecao(mock_adapter_cls):
    mock_adapter_cls.side_effect = Exception("BigQuery timeout")

    state = _state(executar_nacional=True)
    _make_stage().execute(state)

    assert state.df_prof_nacional.empty
    assert state.df_estab_nacional.empty


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
def test_busca_profissionais_e_estabelecimentos_em_paralelo(mock_adapter_cls):
    calls = []
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.side_effect = lambda comp: calls.append("prof") or _df_prof()
    mock_adapter.listar_estabelecimentos.side_effect = lambda comp: calls.append("estab") or _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    _make_stage().execute(_state(executar_nacional=True))

    assert "prof" in calls and "estab" in calls


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
@patch("pipeline.stages.ingestao_nacional.config")
def test_skip_quando_cache_hit(mock_config, mock_adapter_cls):
    mock_config.NACIONAL_CACHE_TTL_DIAS = 7
    mock_config.GCP_PROJECT_ID = "proj"
    mock_config.ID_MUNICIPIO_IBGE7 = "3541307"
    mock_config.CACHE_DIR = "/tmp"

    state = _state(executar_nacional=True)
    state.df_processado = _df_processado()
    fingerprint = _computar_fingerprint(state.df_processado)

    db = MagicMock(spec=DatabaseLoader)
    db.ler_cache_nacional.return_value = (fingerprint, datetime.now())

    _make_stage(db).execute(state)

    mock_adapter_cls.assert_not_called()
    db.gravar_cache_nacional.assert_not_called()


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
@patch("pipeline.stages.ingestao_nacional.config")
def test_busca_quando_fingerprint_diferente(mock_config, mock_adapter_cls):
    mock_config.NACIONAL_CACHE_TTL_DIAS = 7
    mock_config.GCP_PROJECT_ID = "proj"
    mock_config.ID_MUNICIPIO_IBGE7 = "3541307"
    mock_config.CACHE_DIR = "/tmp"

    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state(executar_nacional=True)
    state.df_processado = _df_processado()

    db = MagicMock(spec=DatabaseLoader)
    db.ler_cache_nacional.return_value = ("different_fingerprint", datetime.now())

    _make_stage(db).execute(state)

    mock_adapter_cls.assert_called_once()
    db.gravar_cache_nacional.assert_called_once()


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
@patch("pipeline.stages.ingestao_nacional.config")
def test_busca_quando_cache_expirado(mock_config, mock_adapter_cls):
    mock_config.NACIONAL_CACHE_TTL_DIAS = 7
    mock_config.GCP_PROJECT_ID = "proj"
    mock_config.ID_MUNICIPIO_IBGE7 = "3541307"
    mock_config.CACHE_DIR = "/tmp"

    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state(executar_nacional=True)
    state.df_processado = _df_processado()
    fingerprint = _computar_fingerprint(state.df_processado)

    db = MagicMock(spec=DatabaseLoader)
    db.ler_cache_nacional.return_value = (fingerprint, datetime.now() - timedelta(days=8))

    _make_stage(db).execute(state)

    mock_adapter_cls.assert_called_once()
    db.gravar_cache_nacional.assert_called_once()


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
@patch("pipeline.stages.ingestao_nacional.config")
def test_nacional_validado_setado_apos_fetch(mock_config, mock_adapter_cls):
    mock_config.NACIONAL_CACHE_TTL_DIAS = 7
    mock_config.GCP_PROJECT_ID = "proj"
    mock_config.ID_MUNICIPIO_IBGE7 = "3541307"
    mock_config.CACHE_DIR = "/tmp"

    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state(executar_nacional=True)

    _make_stage().execute(state)

    assert state.nacional_validado is True


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
@patch("pipeline.stages.ingestao_nacional.config")
def test_fingerprint_armazenado_em_state(mock_config, mock_adapter_cls):
    mock_config.NACIONAL_CACHE_TTL_DIAS = 7
    mock_config.GCP_PROJECT_ID = "proj"
    mock_config.ID_MUNICIPIO_IBGE7 = "3541307"
    mock_config.CACHE_DIR = "/tmp"

    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state(executar_nacional=True)
    state.df_processado = _df_processado()

    _make_stage().execute(state)

    assert state.fingerprint_local != ""
