"""Auto-apply memleak marker to all tests in this directory."""
from __future__ import annotations

import pytest


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "tests/memory/" in str(item.fspath).replace("\\", "/"):
            item.add_marker(pytest.mark.memleak)
