"""Root-level pytest fixtures."""
from __future__ import annotations

import pytest


@pytest.fixture
def assert_query_limit():
    def _check(response, limit: int = 15) -> None:
        count = int(response.headers.get("X-Query-Count", "0"))
        assert count <= limit, f"n_plus_1_violation count={count} limit={limit}"
    return _check
