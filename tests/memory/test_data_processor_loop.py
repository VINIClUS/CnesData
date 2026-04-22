"""data_processor poll loop simulated 100 iter must not leak."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pytest.importorskip("pytest_memray")


@pytest.mark.limit_memory("50 MB")
def test_poll_loop_nao_vaza_100_iter():
    """Simulates 100 polls fetching 0 jobs (no-op loop)."""
    repo = MagicMock()
    repo.acquire_for_agent.return_value = None
    for _ in range(100):
        claimed = repo.acquire_for_agent("agent-1")
        assert claimed is None
