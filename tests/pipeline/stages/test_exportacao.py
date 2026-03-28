from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.state import PipelineState
from pipeline.stages.exportacao import ExportacaoStage


def _state() -> PipelineState:
    state = PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/Relatorio_2024-12.csv"),
        executar_nacional=True,
        executar_hr=False,
    )
    state.df_processado = pd.DataFrame({"CPF": ["12345678901"]})
    state.df_multi_unidades = pd.DataFrame()
    state.df_acs_incorretos = pd.DataFrame()
    state.df_ace_incorretos = pd.DataFrame()
    state.df_ghost = pd.DataFrame()
    state.df_missing = pd.DataFrame()
    state.df_estab_fantasma = pd.DataFrame()
    state.df_estab_ausente = pd.DataFrame()
    state.df_prof_fantasma = pd.DataFrame()
    state.df_prof_ausente = pd.DataFrame()
    state.df_cbo_diverg = pd.DataFrame()
    state.df_ch_diverg = pd.DataFrame()
    return state


@patch("pipeline.stages.exportacao.exportar_csv")
@patch("pipeline.stages.exportacao.gerar_relatorio")
@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.DatabaseLoader")
@patch("pipeline.stages.exportacao.config")
def test_exporta_csv_principal(
    mock_config, mock_loader_cls, mock_salvar, mock_criar, mock_gerar, mock_exportar
):
    mock_config.SNAPSHOTS_DIR = Path("data/snapshots")
    mock_config.DUCKDB_PATH = Path("data/cnesdata.duckdb")
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    mock_loader_cls.return_value = MagicMock()

    state = _state()
    ExportacaoStage().execute(state)

    mock_exportar.assert_any_call(state.df_processado, state.output_path)


@patch("pipeline.stages.exportacao.exportar_csv")
@patch("pipeline.stages.exportacao.gerar_relatorio")
@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.DatabaseLoader")
@patch("pipeline.stages.exportacao.config")
def test_grava_snapshot_no_duckdb(
    mock_config, mock_loader_cls, mock_salvar, mock_criar, mock_gerar, mock_exportar
):
    mock_config.SNAPSHOTS_DIR = Path("data/snapshots")
    mock_config.DUCKDB_PATH = Path("data/cnesdata.duckdb")
    snapshot = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    mock_criar.return_value = snapshot
    mock_loader = MagicMock()
    mock_loader_cls.return_value = mock_loader

    ExportacaoStage().execute(_state())

    mock_loader.inicializar_schema.assert_called_once()
    mock_loader.gravar_metricas.assert_called_once_with(snapshot)


@patch("pipeline.stages.exportacao.exportar_csv")
@patch("pipeline.stages.exportacao.gerar_relatorio")
@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.DatabaseLoader")
@patch("pipeline.stages.exportacao.config")
def test_nao_exporta_csv_para_df_vazio(
    mock_config, mock_loader_cls, mock_salvar, mock_criar, mock_gerar, mock_exportar
):
    mock_config.SNAPSHOTS_DIR = Path("data/snapshots")
    mock_config.DUCKDB_PATH = Path("data/cnesdata.duckdb")
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    mock_loader_cls.return_value = MagicMock()

    state = _state()
    ExportacaoStage().execute(state)

    exported_paths = [c.args[1] for c in mock_exportar.call_args_list]
    assert not any("ghost" in str(p) for p in exported_paths)


@patch("pipeline.stages.exportacao.exportar_csv")
@patch("pipeline.stages.exportacao.gerar_relatorio")
@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.DatabaseLoader")
@patch("pipeline.stages.exportacao.config")
def test_persistir_usa_competencia_str_nao_nome_arquivo(
    mock_config, mock_loader_cls, mock_salvar, mock_criar, mock_gerar, mock_exportar, tmp_path
):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.DUCKDB_PATH = tmp_path / "test.duckdb"
    mock_config.HISTORICO_DIR = tmp_path / "historico"
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    mock_loader_cls.return_value = MagicMock()
    state = _state()
    # Filename real de produção — split("_")[-1] seria "CNES", não "2024-12"
    state.output_path = tmp_path / "Relatorio_Profissionais_CNES.csv"

    ExportacaoStage().execute(state)

    args = mock_criar.call_args[0]
    assert args[0] == "2024-12"  # state.competencia_str, não "CNES"


@patch("pipeline.stages.exportacao.exportar_csv")
@patch("pipeline.stages.exportacao.gerar_relatorio")
@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.DatabaseLoader")
@patch("pipeline.stages.exportacao.config")
def test_persistir_grava_todas_11_regras(
    mock_config, mock_loader_cls, mock_salvar, mock_criar, mock_gerar, mock_exportar, tmp_path
):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.DUCKDB_PATH = tmp_path / "test.duckdb"
    mock_config.HISTORICO_DIR = tmp_path / "historico"
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=1, total_missing=2, total_rq005=3
    )
    mock_loader = MagicMock()
    mock_loader_cls.return_value = mock_loader

    ExportacaoStage().execute(_state())

    regras = {call.args[1] for call in mock_loader.gravar_auditoria.call_args_list}
    assert regras == {
        "GHOST", "MISSING", "RQ005",
        "RQ003B", "RQ005_ACS", "RQ005_ACE",
        "RQ006", "RQ007", "RQ008", "RQ009", "RQ010", "RQ011",
    }
