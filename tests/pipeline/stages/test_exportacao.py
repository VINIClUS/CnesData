"""Testes para ExportacaoStage simplificada."""
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from pipeline.state import PipelineState
from pipeline.stages.exportacao import ExportacaoStage


def _mock_storage() -> MagicMock:
    return MagicMock(spec=["gravar_profissionais", "gravar_estabelecimentos", "registrar_pipeline_run"])


def _state(local: bool = True, nacional: bool = False) -> PipelineState:
    s = PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
    )
    s.local_disponivel = local
    s.nacional_disponivel = nacional
    s.df_processado = pd.DataFrame({"CPF": ["12345678901"]}) if local else pd.DataFrame()
    s.df_estab_local = pd.DataFrame({"CNES": ["1234567"]}) if local else pd.DataFrame()
    s.df_prof_nacional = pd.DataFrame({"CNS": ["001"]}) if nacional else pd.DataFrame()
    s.df_estab_nacional = pd.DataFrame({"CNES": ["001"]}) if nacional else pd.DataFrame()
    return s


def test_grava_profissionais_locais_quando_local_disponivel():
    mock_storage = _mock_storage()
    ExportacaoStage(mock_storage).execute(_state(local=True))
    mock_storage.gravar_profissionais.assert_called_once_with("2024-12", mock_storage.gravar_profissionais.call_args[0][1])


def test_grava_estabelecimentos_locais_quando_local_disponivel():
    mock_storage = _mock_storage()
    ExportacaoStage(mock_storage).execute(_state(local=True))
    mock_storage.gravar_estabelecimentos.assert_called_once()
    assert mock_storage.gravar_estabelecimentos.call_args[0][0] == "2024-12"


def test_nao_grava_locais_quando_df_processado_vazio():
    mock_storage = _mock_storage()
    state = _state(local=False, nacional=False)
    ExportacaoStage(mock_storage).execute(state)
    mock_storage.gravar_profissionais.assert_not_called()
    mock_storage.gravar_estabelecimentos.assert_not_called()


def test_grava_nacionais_quando_nacional_disponivel():
    mock_storage = _mock_storage()
    ExportacaoStage(mock_storage).execute(_state(local=False, nacional=True))
    assert mock_storage.gravar_profissionais.called
    assert mock_storage.gravar_profissionais.call_args[0][0] == "2024-12"


def test_grava_pipeline_run_sempre():
    mock_storage = _mock_storage()
    ExportacaoStage(mock_storage).execute(_state())
    mock_storage.registrar_pipeline_run.assert_called_once()
    competencia, status_dict = mock_storage.registrar_pipeline_run.call_args[0]
    assert competencia == "2024-12"
    assert "status" in status_dict


def test_status_completo_quando_local_e_nacional_disponiveis():
    mock_storage = _mock_storage()
    ExportacaoStage(mock_storage).execute(_state(local=True, nacional=True))
    _, status_dict = mock_storage.registrar_pipeline_run.call_args[0]
    assert status_dict["status"] == "completo"


def test_status_parcial_quando_so_local():
    mock_storage = _mock_storage()
    ExportacaoStage(mock_storage).execute(_state(local=True, nacional=False))
    _, status_dict = mock_storage.registrar_pipeline_run.call_args[0]
    assert status_dict["status"] == "parcial"


def test_status_sem_dados_locais_quando_so_nacional():
    mock_storage = _mock_storage()
    ExportacaoStage(mock_storage).execute(_state(local=False, nacional=True))
    _, status_dict = mock_storage.registrar_pipeline_run.call_args[0]
    assert status_dict["status"] == "sem_dados_locais"


def test_status_sem_dados_quando_nenhum_disponivel():
    mock_storage = _mock_storage()
    state = _state(local=False, nacional=False)
    ExportacaoStage(mock_storage).execute(state)
    _, status_dict = mock_storage.registrar_pipeline_run.call_args[0]
    assert status_dict["status"] == "sem_dados"


def test_nao_escreve_arquivo_em_disco(tmp_path):
    mock_storage = _mock_storage()
    state = _state()
    state.output_path = tmp_path / "processed" / "report.csv"
    ExportacaoStage(mock_storage).execute(state)
    assert not list(tmp_path.rglob("*.csv"))
    assert not list(tmp_path.rglob("*.json"))
