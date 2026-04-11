"""Testes unitários do ponto de entrada do pipeline."""
from pathlib import Path
from unittest.mock import MagicMock, patch

from main import main


@patch("main.HistoricoReader")
@patch("main.DatabaseLoader")
@patch("main.PipelineOrchestrator")
@patch("main.parse_args")
@patch("main.configurar_logging")
@patch("main.config")
def test_main_usa_orchestrator(mock_config, mock_log, mock_args, mock_orch_cls, mock_db_loader, mock_hist_reader):
    mock_config.COMPETENCIA_ANO = 2024
    mock_config.COMPETENCIA_MES = 12
    mock_config.OUTPUT_PATH = Path("data/processed/report.csv")
    mock_config.FOLHA_HR_PATH = None
    mock_config.LOGS_DIR = MagicMock()
    mock_config.LOG_FILE = Path("logs/test.log")
    mock_config.DB_URL = ""
    mock_args.return_value = MagicMock(
        competencia=None, output_dir=None, skip_nacional=False,
        skip_hr=False, verbose=False,
    )
    mock_orch = MagicMock()
    mock_orch_cls.return_value = mock_orch

    result = main()

    mock_orch.executar.assert_called_once()
    assert result == 0


@patch("main.HistoricoReader")
@patch("main.DatabaseLoader")
@patch("main.PipelineOrchestrator")
@patch("main.parse_args")
@patch("main.configurar_logging")
@patch("main.config")
def test_main_retorna_1_em_excecao(mock_config, mock_log, mock_args, mock_orch_cls, mock_db_loader, mock_hist_reader):
    mock_config.COMPETENCIA_ANO = 2024
    mock_config.COMPETENCIA_MES = 12
    mock_config.OUTPUT_PATH = Path("data/processed/report.csv")
    mock_config.FOLHA_HR_PATH = None
    mock_config.LOGS_DIR = MagicMock()
    mock_config.LOG_FILE = Path("logs/test.log")
    mock_config.DB_URL = ""
    mock_args.return_value = MagicMock(
        competencia=None, output_dir=None, skip_nacional=False,
        skip_hr=False, verbose=False,
    )
    mock_orch = MagicMock()
    mock_orch.executar.side_effect = RuntimeError("boom")
    mock_orch_cls.return_value = mock_orch

    result = main()

    assert result == 1


@patch("main.HistoricoReader")
@patch("main.DatabaseLoader")
@patch("main.PipelineOrchestrator")
@patch("main.parse_args")
@patch("main.configurar_logging")
@patch("main.config")
def test_main_inicializa_schema_antes_do_pipeline(
    mock_config, mock_log, mock_args, mock_orch_cls, mock_db_loader_cls, mock_hist_reader
):
    mock_config.COMPETENCIA_ANO = 2024
    mock_config.COMPETENCIA_MES = 12
    mock_config.OUTPUT_PATH = Path("data/processed/report.csv")
    mock_config.FOLHA_HR_PATH = None
    mock_config.LOGS_DIR = MagicMock()
    mock_config.LOG_FILE = Path("logs/test.log")
    mock_config.DB_URL = ""
    mock_args.return_value = MagicMock(
        competencia=None, output_dir=None, skip_nacional=False,
        skip_hr=False, verbose=False,
    )
    call_order: list[str] = []
    mock_db_instance = mock_db_loader_cls.return_value
    mock_db_instance.inicializar_schema.side_effect = lambda: call_order.append("schema")
    mock_orch = MagicMock()
    mock_orch.executar.side_effect = lambda _: call_order.append("executar")
    mock_orch_cls.return_value = mock_orch

    main()

    assert call_order == ["schema", "executar"]


def test_main_usa_nova_ordem_de_stages():
    import ast
    from pathlib import Path
    src = Path("src/main.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    imports = [
        node.names[0].name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    ]
    assert not any(
        "auditoria" == n and "local" not in n and "nacional" not in n
        for n in imports
    ), "AuditoriaStage ainda importada"


def test_main_importa_metricas_stage():
    from pathlib import Path
    src = Path("src/main.py").read_text(encoding="utf-8")
    assert "MetricasStage" in src
    assert "AuditoriaLocalStage" in src
    assert "AuditoriaNacionalStage" in src
