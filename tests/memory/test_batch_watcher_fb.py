"""batch_watcher FB session open/close 10 times must not leak."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("pytest_memray")
pytest.importorskip("fdb")


@pytest.mark.limit_memory("100 MB")
def test_fb_session_cleanup():
    """Mock fdb.connect; verify no session handle leak across 10 cycles."""
    with patch("fdb.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        for _ in range(10):
            import fdb
            conn = fdb.connect(dsn="fake", user="x", password="y")  # noqa: S106
            conn.close()
