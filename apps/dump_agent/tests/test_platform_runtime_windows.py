"""Windows-only tests for platform_runtime Win32 branch."""
from __future__ import annotations

import sys
import threading
from pathlib import Path  # noqa: F401
from unittest.mock import MagicMock

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


class TestInstallWindowsHandler:
    def test_chama_set_console_ctrl_handler(self, monkeypatch):
        fake_kernel32 = MagicMock()
        fake_kernel32.SetConsoleCtrlHandler.return_value = 1
        monkeypatch.setattr(
            platform_runtime, "_kernel32", fake_kernel32,
        )
        called = threading.Event()
        platform_runtime.install_shutdown_handler(on_stop=called.set)
        fake_kernel32.SetConsoleCtrlHandler.assert_called_once()
        args = fake_kernel32.SetConsoleCtrlHandler.call_args[0]
        assert args[1] is True

    def test_levanta_oserror_quando_api_retorna_zero(self, monkeypatch):
        fake_kernel32 = MagicMock()
        fake_kernel32.SetConsoleCtrlHandler.return_value = 0
        monkeypatch.setattr(
            platform_runtime, "_kernel32", fake_kernel32,
        )
        with pytest.raises(OSError, match="SetConsoleCtrlHandler"):
            platform_runtime.install_shutdown_handler(on_stop=lambda: None)


class TestWindowsMutex:
    def test_bloqueia_segunda_aquisicao(self):
        name = f"test_mutex_{id(object())}"
        with platform_runtime.acquire_single_instance_lock(name):
            with pytest.raises(RuntimeError, match="already_running"):
                with platform_runtime.acquire_single_instance_lock(name):
                    pass

    def test_libera_handle_no_exit(self):
        name = f"test_release_{id(object())}"
        with platform_runtime.acquire_single_instance_lock(name):
            pass
        with platform_runtime.acquire_single_instance_lock(name):
            pass


class TestFbclientDllPathWindows:
    def test_usa_meipass_quando_frozen(self, tmp_path, monkeypatch):
        dll = tmp_path / "fbclient.dll"
        dll.write_bytes(b"x")
        monkeypatch.delenv("FIREBIRD_DLL", raising=False)
        monkeypatch.setattr(sys, "frozen", True, raising=False)
        monkeypatch.setattr(sys, "_MEIPASS", str(tmp_path), raising=False)
        assert platform_runtime.fbclient_dll_path() == dll

    def test_usa_executable_parent_como_fallback_frozen(
        self, tmp_path, monkeypatch,
    ):
        exe_dir = tmp_path / "app"
        exe_dir.mkdir()
        (exe_dir / "fbclient.dll").write_bytes(b"x")
        (exe_dir / "dump_agent.exe").write_bytes(b"")
        monkeypatch.delenv("FIREBIRD_DLL", raising=False)
        monkeypatch.setattr(sys, "frozen", True, raising=False)
        monkeypatch.setattr(
            sys, "_MEIPASS", str(tmp_path / "nonexistent"), raising=False,
        )
        monkeypatch.setattr(
            sys, "executable", str(exe_dir / "dump_agent.exe"),
        )
        assert platform_runtime.fbclient_dll_path() == exe_dir / "fbclient.dll"
