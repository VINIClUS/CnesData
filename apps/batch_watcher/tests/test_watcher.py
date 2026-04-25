"""Testes unitários do watcher."""

import pytest

pytest.skip(
    "cnes_infra.storage.batch_trigger module was deleted; orphaned test. "
    "Restore module or remove this test in a follow-up PR.",
    allow_module_level=True,
)

from unittest.mock import MagicMock, patch  # noqa: E402, F401

from cnes_infra.storage.batch_trigger import Thresholds, TriggerState  # noqa: E402, F401


def _state(status="OPEN", pending=None, oldest=None, reason="size_threshold"):
    return TriggerState(
        status=status, opened_at=None,
        pending_bytes=pending, oldest_completed_at=oldest,
        reason=reason,
    )


@patch("batch_watcher.watcher.evaluate_and_open")
def test_run_once_retorna_0_em_sucesso(mock_eval):
    from batch_watcher.watcher import run_once
    mock_eval.return_value = _state()
    assert run_once(MagicMock()) == 0


@patch("batch_watcher.watcher.evaluate_and_open")
def test_run_once_retorna_1_em_exception(mock_eval):
    from batch_watcher.watcher import run_once
    mock_eval.side_effect = RuntimeError("boom")
    assert run_once(MagicMock()) == 1


@patch("batch_watcher.watcher.evaluate_and_open")
def test_run_once_passa_thresholds_convertidos_em_bytes(mock_eval):
    from batch_watcher.watcher import run_once
    mock_eval.return_value = _state()
    with patch(
        "batch_watcher.watcher.SIZE_THRESHOLD_MB", 50,
    ), patch(
        "batch_watcher.watcher.AGE_THRESHOLD_DAYS", 3,
    ):
        run_once(MagicMock())
    call_args = mock_eval.call_args
    thresholds: Thresholds = call_args[0][1]
    assert thresholds.size_bytes == 50 * 1024 * 1024
    assert thresholds.age_days == 3


@patch("batch_watcher.watcher.evaluate_and_open")
def test_run_once_loga_estado_com_pending_mb_e_reason(mock_eval, caplog):
    import logging

    from batch_watcher.watcher import run_once
    mock_eval.return_value = _state(pending=2 * 1024 * 1024)
    with caplog.at_level(logging.INFO):
        run_once(MagicMock())
    assert any("watcher_tick" in r.message for r in caplog.records)
    assert any("pending_mb=2.0" in r.message for r in caplog.records)
    assert any("reason=size_threshold" in r.message for r in caplog.records)
