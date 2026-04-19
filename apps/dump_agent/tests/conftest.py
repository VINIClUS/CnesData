"""Conftest for dump_agent — auto-skip platform-specific markers."""
from __future__ import annotations

import sys

import pytest


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item],
) -> None:
    is_windows = sys.platform == "win32"
    skip_linux_only = pytest.mark.skip(reason="linux_only: skipped on Windows")
    skip_windows_only = pytest.mark.skip(reason="windows_only: skipped on POSIX")
    for item in items:
        if "linux_only" in item.keywords and is_windows:
            item.add_marker(skip_linux_only)
        elif "windows_only" in item.keywords and not is_windows:
            item.add_marker(skip_windows_only)
