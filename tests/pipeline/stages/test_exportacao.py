import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.state import PipelineState
from pipeline.stages.exportacao import ExportacaoStage


def _mock_storage() -> MagicMock:
    return MagicMock(spec=["gravar_profissionais", "gravar_estabelecimentos", "registrar_pipeline_run"])


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


def _state_nacional() -> PipelineState:
    s = _state()
    s.executar_nacional = True
    s.df_prof_nacional = pd.DataFrame({"CNS": ["001"]})
    s.df_estab_nacional = pd.DataFrame({"CNES": ["001"]})
    return s


def _state_sem_nacional() -> PipelineState:
    s = _state()
    s.executar_nacional = False
    return s


def _make_state(tmp_path: Path) -> PipelineState:
    s = _state()
    s.output_path = tmp_path / "processed" / "report.csv"
    return s


@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.config")
def test_grava_snapshot_json(mock_config, mock_salvar, mock_criar, tmp_path):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.LAST_RUN_PATH = tmp_path / "cache" / "last_run.json"
    snapshot = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    mock_criar.return_value = snapshot

    state = _state()
    state.output_path = tmp_path / "processed" / "Relatorio_2024-12.csv"
    ExportacaoStage(_mock_storage()).execute(state)

    mock_criar.assert_called_once()
    mock_salvar.assert_called_once_with(snapshot, mock_config.SNAPSHOTS_DIR)


@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.config")
def test_persistir_usa_competencia_str_nao_nome_arquivo(
    mock_config, mock_salvar, mock_criar, tmp_path
):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.LAST_RUN_PATH = tmp_path / "cache" / "last_run.json"
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    state = _state()
    state.output_path = tmp_path / "Relatorio_Profissionais_CNES.csv"

    ExportacaoStage(_mock_storage()).execute(state)

    args = mock_criar.call_args[0]
    assert args[0] == "2024-12"


@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.config")
def test_nao_escreve_csv(mock_config, mock_salvar, mock_criar, tmp_path):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.LAST_RUN_PATH = tmp_path / "cache" / "last_run.json"
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )

    state = _state()
    state.output_path = tmp_path / "processed" / "Relatorio_2024-12.csv"
    ExportacaoStage(_mock_storage()).execute(state)

    assert not (tmp_path / "processed" / "Relatorio_2024-12.csv").exists()


@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.config")
def test_grava_pipeline_run(mock_config, mock_salvar, mock_criar, tmp_path):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.LAST_RUN_PATH = tmp_path / "cache" / "last_run.json"
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    mock_storage = _mock_storage()

    state = _state()
    state.nacional_disponivel = True
    state.output_path = tmp_path / "processed" / "report.csv"
    ExportacaoStage(mock_storage).execute(state)

    mock_storage.registrar_pipeline_run.assert_called_once()
    call_args = mock_storage.registrar_pipeline_run.call_args[0]
    assert call_args[0] == "2024-12"
    assert call_args[1]["status"] == "completo"


@patch("pipeline.stages.exportacao.config")
def test_status_sem_dados_locais_quando_local_indisponivel(mock_config, tmp_path):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.LAST_RUN_PATH = tmp_path / "cache" / "last_run.json"
    mock_storage = _mock_storage()

    state = _state()
    state.df_processado = pd.DataFrame()
    state.local_disponivel = False
    state.nacional_disponivel = True
    ExportacaoStage(mock_storage).execute(state)

    call_args = mock_storage.registrar_pipeline_run.call_args[0]
    assert call_args[1]["status"] == "sem_dados_locais"


@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.config")
def test_nacional_only_persiste_profissionais_no_postgres(
    mock_config, mock_salvar, mock_criar, tmp_path
):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.LAST_RUN_PATH = tmp_path / "cache" / "last_run.json"
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    mock_storage = _mock_storage()

    state = _state()
    state.local_disponivel = False
    state.nacional_disponivel = True
    state.df_estab_nacional = pd.DataFrame({"CNES": ["001"]})

    ExportacaoStage(mock_storage).execute(state)

    mock_storage.gravar_profissionais.assert_called_once_with("2024-12", state.df_processado)


@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.config")
def test_nacional_only_persiste_estabelecimentos_no_postgres(
    mock_config, mock_salvar, mock_criar, tmp_path
):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.LAST_RUN_PATH = tmp_path / "cache" / "last_run.json"
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    mock_storage = _mock_storage()

    state = _state()
    state.local_disponivel = False
    state.nacional_disponivel = True
    state.df_estab_nacional = pd.DataFrame({"CNES": ["001"]})

    ExportacaoStage(mock_storage).execute(state)

    mock_storage.gravar_estabelecimentos.assert_called_once_with(
        "2024-12", state.df_estab_nacional
    )


@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.config")
def test_local_disponivel_nao_chama_storage_gravar(mock_config, mock_salvar, mock_criar, tmp_path):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.LAST_RUN_PATH = tmp_path / "cache" / "last_run.json"
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    mock_storage = _mock_storage()

    state = _state()
    state.local_disponivel = True

    ExportacaoStage(mock_storage).execute(state)

    mock_storage.gravar_profissionais.assert_not_called()
    mock_storage.gravar_estabelecimentos.assert_not_called()


@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.config")
def test_salva_snapshot_quando_df_processado_nao_vazio(
    mock_config, mock_salvar, mock_criar, tmp_path
):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.LAST_RUN_PATH = tmp_path / "cache" / "last_run.json"
    snapshot = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    mock_criar.return_value = snapshot

    state = _state()
    state.local_disponivel = False
    state.nacional_disponivel = True
    state.df_estab_nacional = pd.DataFrame({"CNES": ["001"]})

    ExportacaoStage(_mock_storage()).execute(state)

    mock_salvar.assert_called_once_with(snapshot, mock_config.SNAPSHOTS_DIR)


class TestGravarLastRun:

    def test_grava_arquivo_json(self, tmp_path):
        from pipeline.stages.exportacao import _gravar_last_run
        path = tmp_path / "last_run.json"

        _gravar_last_run(_state_nacional(), path)

        assert path.exists()
        dados = json.loads(path.read_text(encoding="utf-8"))
        assert set(dados.keys()) == {"firebird", "bigquery", "hr", "postgres"}

    def test_firebird_ok_quando_local_disponivel(self, tmp_path):
        from pipeline.stages.exportacao import _gravar_last_run
        path = tmp_path / "last_run.json"

        _gravar_last_run(_state_nacional(), path)

        dados = json.loads(path.read_text(encoding="utf-8"))
        assert dados["firebird"]["ok"] is True
        assert dados["firebird"]["ts"] is not None

    def test_bigquery_ok_false_quando_nacional_nao_executado(self, tmp_path):
        from pipeline.stages.exportacao import _gravar_last_run
        path = tmp_path / "last_run.json"

        _gravar_last_run(_state_sem_nacional(), path)

        dados = json.loads(path.read_text(encoding="utf-8"))
        assert dados["bigquery"]["ok"] is False
        assert dados["bigquery"]["ts"] is None

    def test_cria_diretorio_pai_se_nao_existir(self, tmp_path):
        from pipeline.stages.exportacao import _gravar_last_run
        path = tmp_path / "subdir" / "last_run.json"

        _gravar_last_run(_state_nacional(), path)

        assert path.exists()
