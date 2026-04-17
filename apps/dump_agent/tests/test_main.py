"""Testes unitarios do ponto de entrada do dump_agent."""
from pathlib import Path
from unittest.mock import AsyncMock, patch


@patch("dump_agent.main._MAX_JITTER", 0.0)
@patch("dump_agent.main._setup_logging")
@patch("dump_agent.main.resolve_machine_id", return_value="m-test")
@patch("dump_agent.main.acquire_single_instance_lock")
@patch("dump_agent.main.install_shutdown_handler")
@patch("dump_agent.main.run_worker", new_callable=AsyncMock)
def test_main_invoca_worker_com_stop_event(
    mock_run, mock_install, mock_lock, mock_mid, mock_logging,
):
    mock_lock.return_value.__enter__ = lambda self: None
    mock_lock.return_value.__exit__ = lambda self, *exc: None

    from dump_agent.main import main_sync
    result = main_sync()

    assert result == 0
    mock_install.assert_called_once()
    mock_run.assert_awaited_once()
    call_kwargs = mock_run.await_args.kwargs
    assert "stop_event" in call_kwargs


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


@patch("dump_agent.main._MAX_JITTER", 0.0)
@patch("dump_agent.main._setup_logging")
@patch("dump_agent.main.resolve_machine_id", return_value="m-test")
@patch("dump_agent.main.acquire_single_instance_lock")
@patch("dump_agent.main.install_shutdown_handler")
@patch("dump_agent.main.run_worker", new_callable=AsyncMock)
@patch("dump_agent.main.faulthandler")
def test_main_habilita_faulthandler(
    mock_fh, mock_run, mock_install, mock_lock, mock_mid, mock_logging,
):
    mock_lock.return_value.__enter__ = lambda self: None
    mock_lock.return_value.__exit__ = lambda self, *exc: None

    from dump_agent.main import main_sync
    main_sync()
    mock_fh.enable.assert_called_once()
