"""500 reap passes against a small mock repo must not leak."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pytest.importorskip("pytest_memray")


@pytest.mark.limit_memory("30 MB")
def test_reaper_500_iter_bounded():
    repo = MagicMock()
    repo.reap_expired_leases.return_value = 0
    for _ in range(500):
        repo.reap_expired_leases()
