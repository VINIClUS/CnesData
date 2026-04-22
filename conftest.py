"""Root conftest: re-exports fixtures from tests/conftest.py so they are
available across packages/ and apps/ test trees."""
from __future__ import annotations

from tests.conftest import assert_query_limit

__all__ = ["assert_query_limit"]
