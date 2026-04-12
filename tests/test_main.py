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
    mock_config.LOGS_DIR = MagicMock()
    mock_config.LOG_FILE = Path("logs/test.log")
    mock_config.DB_URL = ""
    mock_args.return_value = MagicMock(
        competencia=None, output_dir=None, source="LOCAL", verbose=False,
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
    mock_config.LOGS_DIR = MagicMock()
    mock_config.LOG_FILE = Path("logs/test.log")
    mock_config.DB_URL = ""
    mock_args.return_value = MagicMock(
        competencia=None, output_dir=None, source="LOCAL", verbose=False,
    )
    mock_orch = MagicMock()
    mock_orch.executar.side_effect = RuntimeError("boom")
    mock_orch_cls.return_value = mock_orch

    assert main() == 1


@patch("main.PipelineOrchestrator")
@patch("main.parse_args")
@patch("main.configurar_logging")
@patch("main.config")
def test_main_inicializa_schema_antes_do_pipeline(mock_config, mock_log, mock_args, mock_orch_cls):
    mock_config.COMPETENCIA_ANO = 2024
    mock_config.COMPETENCIA_MES = 12
    mock_config.OUTPUT_PATH = Path("data/processed/report.csv")
    mock_config.LOGS_DIR = MagicMock()
    mock_config.LOG_FILE = Path("logs/test.log")
    mock_config.DB_URL = ""
    mock_args.return_value = MagicMock(
        competencia=None, output_dir=None, source="LOCAL", verbose=False,
    )
    call_order: list[str] = []
    mock_orch = MagicMock()
    mock_orch.executar.side_effect = lambda _: call_order.append("executar")
    mock_orch_cls.return_value = mock_orch

    main()

    assert "executar" in call_order


def test_main_usa_nova_ordem_de_stages():
    import ast
    src = Path("src/main.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    from_imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    assert "IngestaoLocalStage" in from_imports
    assert "ProcessamentoStage" in from_imports
    assert "IngestaoNacionalStage" in from_imports
    assert "ExportacaoStage" in from_imports
    assert "DatabaseLoader" not in from_imports
