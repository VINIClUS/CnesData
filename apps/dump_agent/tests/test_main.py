"""Testes unitarios do ponto de entrada do dump_agent."""
import asyncio
import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch


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


@patch("dump_agent.main.logs_dir")
def test_setup_logging_configura_handlers_verbose(mock_logs, tmp_path):
    mock_logs.return_value = tmp_path
    (tmp_path).mkdir(parents=True, exist_ok=True)
    from dump_agent.main import _setup_logging
    root = logging.getLogger()
    before = list(root.handlers)
    _setup_logging(verbose=True)
    after = list(root.handlers)
    added = [h for h in after if h not in before]
    assert any(isinstance(h, logging.StreamHandler) for h in added)
    for h in added:
        root.removeHandler(h)


@patch("dump_agent.main.logs_dir")
def test_setup_logging_configura_handlers_nao_verbose(mock_logs, tmp_path):
    mock_logs.return_value = tmp_path
    from dump_agent.main import _setup_logging
    root = logging.getLogger()
    before = list(root.handlers)
    _setup_logging(verbose=False)
    after = list(root.handlers)
    added = [h for h in after if h not in before]
    assert any(isinstance(h, logging.StreamHandler) for h in added)
    for h in added:
        root.removeHandler(h)


@patch("dump_agent.main.run_worker", new_callable=AsyncMock)
def test_async_main_retorna_zero_quando_stop_event_setado_antes_jitter(mock_run):
    async def body():
        from dump_agent.main import _async_main
        stop_event = asyncio.Event()
        stop_event.set()
        with patch("dump_agent.main._MAX_JITTER", 3600.0):
            result = await _async_main("http://x", "m1", stop_event)
        assert result == 0
        mock_run.assert_not_awaited()

    asyncio.run(body())


@patch("dump_agent.main._MAX_JITTER", 0.0)
@patch("dump_agent.main.run_worker", new_callable=AsyncMock)
def test_async_main_invoca_worker_apos_jitter(mock_run):
    async def body():
        from dump_agent.main import _async_main
        stop_event = asyncio.Event()
        result = await _async_main("http://x", "m1", stop_event)
        assert result == 0
        mock_run.assert_awaited_once()

    asyncio.run(body())


@patch("dump_agent.main._MAX_JITTER", 0.0)
@patch("dump_agent.main._setup_logging")
@patch("dump_agent.main.resolve_machine_id", return_value="m1")
@patch("dump_agent.main.acquire_single_instance_lock")
@patch("dump_agent.main.install_shutdown_handler")
@patch("dump_agent.main.run_worker", new_callable=AsyncMock)
def test_main_sync_reconfigure_falha_com_attribute_error(
    mock_run, mock_install, mock_lock, mock_mid, mock_logging,
):
    mock_lock.return_value.__enter__ = lambda self: None
    mock_lock.return_value.__exit__ = lambda self, *exc: None
    mock_stdout = MagicMock()
    mock_stdout.reconfigure.side_effect = AttributeError("no reconfigure")
    with patch("dump_agent.main.sys") as mock_sys:
        mock_sys.stdout = mock_stdout
        mock_sys.argv = []
        from dump_agent.main import main_sync
        result = main_sync()
    assert result == 0


@patch("dump_agent.main._MAX_JITTER", 0.0)
@patch("dump_agent.main._setup_logging")
@patch("dump_agent.main.resolve_machine_id", return_value="m1")
@patch("dump_agent.main.acquire_single_instance_lock")
@patch("dump_agent.main.install_shutdown_handler")
@patch("dump_agent.main.run_worker", new_callable=AsyncMock)
def test_main_sync_reconfigure_falha_com_oserror(
    mock_run, mock_install, mock_lock, mock_mid, mock_logging,
):
    mock_lock.return_value.__enter__ = lambda self: None
    mock_lock.return_value.__exit__ = lambda self, *exc: None
    mock_stdout = MagicMock()
    mock_stdout.reconfigure.side_effect = OSError("ioerr")
    with patch("dump_agent.main.sys") as mock_sys:
        mock_sys.stdout = mock_stdout
        mock_sys.argv = []
        from dump_agent.main import main_sync
        result = main_sync()
    assert result == 0


@patch("dump_agent.main._MAX_JITTER", 0.0)
@patch("dump_agent.main._setup_logging")
@patch("dump_agent.main.resolve_machine_id", return_value="m1")
@patch("dump_agent.main.acquire_single_instance_lock")
@patch("dump_agent.main.install_shutdown_handler")
@patch("dump_agent.main.run_worker", new_callable=AsyncMock)
def test_on_stop_seta_evento_via_loop_running(
    mock_run, mock_install, mock_lock, mock_mid, mock_logging,
):
    mock_lock.return_value.__enter__ = lambda self: None
    mock_lock.return_value.__exit__ = lambda self, *exc: None
    captured_cb = {}

    def capture_handler(cb):
        captured_cb["cb"] = cb

    mock_install.side_effect = capture_handler

    from dump_agent.main import main_sync
    main_sync()

    assert "cb" in captured_cb
    cb = captured_cb["cb"]
    loop = asyncio.new_event_loop()
    try:
        ev = asyncio.Event()

        async def run_with_event():
            cb()
            return ev.is_set()

        loop.run_until_complete(run_with_event())
    finally:
        loop.close()


@patch("dump_agent.main._MAX_JITTER", 0.0)
@patch("dump_agent.main._setup_logging")
@patch("dump_agent.main.resolve_machine_id", return_value="m1")
@patch("dump_agent.main.acquire_single_instance_lock")
@patch("dump_agent.main.install_shutdown_handler")
@patch("dump_agent.main.run_worker", new_callable=AsyncMock)
def test_on_stop_seta_evento_sem_loop_rodando(
    mock_run, mock_install, mock_lock, mock_mid, mock_logging,
):
    mock_lock.return_value.__enter__ = lambda self: None
    mock_lock.return_value.__exit__ = lambda self, *exc: None
    captured_cb = {}

    def capture_handler(cb):
        captured_cb["cb"] = cb

    mock_install.side_effect = capture_handler

    from dump_agent.main import main_sync
    main_sync()

    cb = captured_cb["cb"]
    with patch("dump_agent.main.asyncio.get_event_loop") as mock_get:
        mock_loop = MagicMock()
        mock_loop.is_running.return_value = False
        mock_get.return_value = mock_loop
        cb()


@patch("dump_agent.main._MAX_JITTER", 0.0)
@patch("dump_agent.main._setup_logging")
@patch("dump_agent.main.resolve_machine_id", return_value="m1")
@patch("dump_agent.main.acquire_single_instance_lock")
@patch("dump_agent.main.install_shutdown_handler")
@patch("dump_agent.main.run_worker", new_callable=AsyncMock)
def test_on_stop_sem_loop_disponivel_levanta_runtime_error(
    mock_run, mock_install, mock_lock, mock_mid, mock_logging,
):
    mock_lock.return_value.__enter__ = lambda self: None
    mock_lock.return_value.__exit__ = lambda self, *exc: None
    captured_cb = {}

    def capture_handler(cb):
        captured_cb["cb"] = cb

    mock_install.side_effect = capture_handler

    from dump_agent.main import main_sync
    main_sync()

    cb = captured_cb["cb"]
    with patch("dump_agent.main.asyncio.get_event_loop", side_effect=RuntimeError("no loop")):
        cb()
