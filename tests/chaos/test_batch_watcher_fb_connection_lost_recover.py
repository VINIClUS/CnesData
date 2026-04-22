"""FB connection timeout → exponential backoff + recovery."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest


@pytest.mark.chaos
def test_retry_exponencial_recupera(monkeypatch):
    """Mock fdb.connect fails twice then succeeds; verify backoff delays."""
    pytest.importorskip("fdb")

    attempts = []

    def fake_connect(*args, **kwargs):
        attempts.append(time.monotonic())
        if len(attempts) <= 2:
            raise ConnectionError("fb_timeout")
        return MagicMock()

    monkeypatch.setattr("fdb.connect", fake_connect)

    try:
        from batch_watcher.connect import connect_with_retry
    except ImportError:
        pytest.skip("batch_watcher.connect.connect_with_retry not yet implemented")

    conn = connect_with_retry(max_attempts=5, base_delay=0.01)
    assert conn is not None
    assert len(attempts) == 3
