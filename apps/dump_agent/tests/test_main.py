"""Testes unitarios do ponto de entrada do dump_agent."""
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch


@patch("dump_agent.main._MAX_JITTER", 0.0)
@patch("dump_agent.main._setup_logging")
@patch("dump_agent.main.run_worker", new_callable=AsyncMock)
def test_main_invoca_worker(mock_run, mock_logging):
    from dump_agent.main import main
    result = asyncio.run(main())

    mock_run.assert_awaited_once()
    assert result == 0


def test_main_nao_importa_postgres():
    import ast

    src = Path(
        "apps/dump_agent/src/dump_agent/main.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(src)
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    forbidden = {
        "PostgresAdapter", "PipelineOrchestrator",
        "PipelineState", "execute_job",
    }
    assert imports & forbidden == set()
