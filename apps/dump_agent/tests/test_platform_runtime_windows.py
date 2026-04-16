"""Windows-only tests for platform_runtime Win32 branch."""
from __future__ import annotations

import sys  # noqa: F401
import threading
from pathlib import Path  # noqa: F401
from unittest.mock import MagicMock  # noqa: F401

import pytest

from dump_agent import platform_runtime

pytestmark = pytest.mark.windows_only


class TestWindowsHandler:
    def test_handler_retorna_true_em_ctrl_c(self):
        platform_runtime._temp_dirs.clear()
        platform_runtime._on_stop_callback = None
        assert platform_runtime._windows_handler(0) is True

    def test_handler_chama_callback_registrado(self):
        called = threading.Event()
        platform_runtime._temp_dirs.clear()
        platform_runtime._on_stop_callback = called.set
        platform_runtime._windows_handler(1)
        assert called.is_set()

    def test_handler_limpa_tempdirs_registrados(self, tmp_path):
        subdir = tmp_path / "dump_xyz"
        subdir.mkdir()
        (subdir / "f.tmp").write_bytes(b"x")
        platform_runtime._temp_dirs.clear()
        platform_runtime.register_temp_dir(subdir)
        platform_runtime._on_stop_callback = None
        platform_runtime._windows_handler(0)
        assert not subdir.exists()
