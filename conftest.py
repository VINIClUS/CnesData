"""Root conftest — exposes shared fixtures across packages/ and apps/ tests.

Defined inline (no `from tests.conftest import ...`) to avoid pytest
module-discovery conflicts with `apps/*/tests/` dirs that lack `__init__.py`.
"""
from __future__ import annotations

import pytest


@pytest.fixture
def assert_query_limit():
    def _check(response, limit: int = 15) -> None:
        count = int(response.headers.get("X-Query-Count", "0"))
        assert count <= limit, f"n_plus_1_violation count={count} limit={limit}"
    return _check


__all__ = ["assert_query_limit"]
