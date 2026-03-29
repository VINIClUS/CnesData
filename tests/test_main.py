"""Testes unitários do ponto de entrada do pipeline."""
from pathlib import Path
from unittest.mock import MagicMock, patch

from main import main


@patch("main.PipelineOrchestrator")
@patch("main.parse_args")
@patch("main.configurar_logging")
@patch("main.config")
def test_main_usa_orchestrator(mock_config, mock_log, mock_args, mock_orch_cls):
    mock_config.COMPETENCIA_ANO = 2024
    mock_config.COMPETENCIA_MES = 12
    mock_config.OUTPUT_PATH = Path("data/processed/report.csv")
    mock_config.FOLHA_HR_PATH = None
    mock_config.LOGS_DIR = MagicMock()
    mock_config.LOG_FILE = Path("logs/test.log")
    mock_args.return_value = MagicMock(
        competencia=None, output_dir=None, skip_nacional=False,
        skip_hr=False, verbose=False,
    )
    mock_orch = MagicMock()
    mock_orch_cls.return_value = mock_orch

    result = main()

    mock_orch.executar.assert_called_once()
    assert result == 0


@patch("main.PipelineOrchestrator")
@patch("main.parse_args")
@patch("main.configurar_logging")
@patch("main.config")
def test_main_retorna_1_em_excecao(mock_config, mock_log, mock_args, mock_orch_cls):
    mock_config.COMPETENCIA_ANO = 2024
    mock_config.COMPETENCIA_MES = 12
    mock_config.OUTPUT_PATH = Path("data/processed/report.csv")
    mock_config.FOLHA_HR_PATH = None
    mock_config.LOGS_DIR = MagicMock()
    mock_config.LOG_FILE = Path("logs/test.log")
    mock_args.return_value = MagicMock(
        competencia=None, output_dir=None, skip_nacional=False,
        skip_hr=False, verbose=False,
    )
    mock_orch = MagicMock()
    mock_orch.executar.side_effect = RuntimeError("boom")
    mock_orch_cls.return_value = mock_orch

    result = main()

    assert result == 1
