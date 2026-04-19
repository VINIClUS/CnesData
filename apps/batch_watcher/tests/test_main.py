"""Testes do entrypoint main.py do batch_watcher."""

from unittest.mock import MagicMock, patch


@patch("batch_watcher.main.create_engine")
@patch("batch_watcher.main.run_once")
@patch("batch_watcher.main.init_telemetry")
def test_main_chama_init_telemetry_e_run_once(
    mock_init_telem, mock_run, mock_engine,
):
    from batch_watcher.main import main
    mock_run.return_value = 0
    mock_engine.return_value = MagicMock()

    rc = main()

    assert rc == 0
    mock_init_telem.assert_called_once_with("batch-watcher")
    mock_run.assert_called_once()


@patch("batch_watcher.main.create_engine")
@patch("batch_watcher.main.run_once")
@patch("batch_watcher.main.init_telemetry")
def test_main_dispose_engine_em_sucesso(
    mock_init_telem, mock_run, mock_engine,
):
    from batch_watcher.main import main
    mock_run.return_value = 0
    eng = MagicMock()
    mock_engine.return_value = eng

    main()

    eng.dispose.assert_called_once()


@patch("batch_watcher.main.create_engine")
@patch("batch_watcher.main.run_once")
@patch("batch_watcher.main.init_telemetry")
def test_main_dispose_engine_mesmo_em_exception(
    mock_init_telem, mock_run, mock_engine,
):
    import pytest
    from batch_watcher.main import main
    eng = MagicMock()
    mock_engine.return_value = eng
    mock_run.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError):
        main()

    eng.dispose.assert_called_once()
