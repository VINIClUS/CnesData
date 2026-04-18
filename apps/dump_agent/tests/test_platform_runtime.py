"""Portable tests for platform_runtime (run on both Linux and Windows)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from dump_agent import platform_runtime


class TestIsFrozen:
    def test_retorna_false_em_dev(self, monkeypatch):
        monkeypatch.delattr(sys, "frozen", raising=False)
        assert platform_runtime.is_frozen() is False

    def test_retorna_true_quando_sys_frozen_setado(self, monkeypatch):
        monkeypatch.setattr(sys, "frozen", True, raising=False)
        assert platform_runtime.is_frozen() is True


class TestAppDataDir:
    def test_cria_diretorio_se_nao_existe(self, tmp_path, monkeypatch):
        target = tmp_path / "state"
        if sys.platform == "win32":
            monkeypatch.setenv("LOCALAPPDATA", str(target))
        else:
            monkeypatch.setenv("XDG_STATE_HOME", str(target))
        result = platform_runtime.app_data_dir()
        assert result.exists()
        assert result.is_dir()

    def test_retorna_mesmo_path_em_chamadas_repetidas(self, tmp_path, monkeypatch):
        target = tmp_path / "state2"
        if sys.platform == "win32":
            monkeypatch.setenv("LOCALAPPDATA", str(target))
        else:
            monkeypatch.setenv("XDG_STATE_HOME", str(target))
        assert platform_runtime.app_data_dir() == platform_runtime.app_data_dir()


class TestLogsDir:
    def test_respeita_env_dump_logs_dir(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DUMP_LOGS_DIR", str(tmp_path / "custom-logs"))
        assert platform_runtime.logs_dir() == tmp_path / "custom-logs"
        assert (tmp_path / "custom-logs").exists()

    def test_usa_app_data_dir_como_default(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DUMP_LOGS_DIR", raising=False)
        if sys.platform == "win32":
            monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "la"))
        else:
            monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xs"))
        result = platform_runtime.logs_dir()
        assert result.name == "logs"
        assert result.parent == platform_runtime.app_data_dir()
        assert result.exists()


class TestTempDirRegistry:
    def test_register_adiciona_ao_set(self, tmp_path):
        platform_runtime._temp_dirs.clear()
        platform_runtime.register_temp_dir(tmp_path)
        assert tmp_path in platform_runtime._temp_dirs

    def test_register_idempotente_com_path_duplicado(self, tmp_path):
        platform_runtime._temp_dirs.clear()
        platform_runtime.register_temp_dir(tmp_path)
        platform_runtime.register_temp_dir(tmp_path)
        assert len(platform_runtime._temp_dirs) == 1

    def test_unregister_remove_do_set(self, tmp_path):
        platform_runtime._temp_dirs.clear()
        platform_runtime.register_temp_dir(tmp_path)
        platform_runtime.unregister_temp_dir(tmp_path)
        assert tmp_path not in platform_runtime._temp_dirs

    def test_unregister_idempotente_quando_ja_removido(self, tmp_path):
        platform_runtime._temp_dirs.clear()
        platform_runtime.unregister_temp_dir(tmp_path)
        platform_runtime.unregister_temp_dir(tmp_path)
        assert tmp_path not in platform_runtime._temp_dirs


class TestResolveMachineId:
    def test_env_var_tem_precedencia(self, monkeypatch):
        monkeypatch.setenv("MACHINE_ID", "docker-01")
        assert platform_runtime.resolve_machine_id() == "docker-01"

    def test_gera_uuid_e_persiste_no_primeiro_run(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.delenv("MACHINE_ID", raising=False)
        if sys.platform == "win32":
            monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
        else:
            monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        mid = platform_runtime.resolve_machine_id()
        assert len(mid) == 8
        store = platform_runtime.app_data_dir() / "machine_id"
        assert store.read_text().strip() == mid

    def test_le_machine_id_persistido(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MACHINE_ID", raising=False)
        if sys.platform == "win32":
            monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
        else:
            monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        store = platform_runtime.app_data_dir() / "machine_id"
        store.write_text("preserved")
        assert platform_runtime.resolve_machine_id() == "preserved"


class TestFbclientDllPath:
    def test_env_var_tem_precedencia(self, tmp_path, monkeypatch):
        fake = tmp_path / "fbclient.dll"
        fake.write_bytes(b"dummy")
        monkeypatch.setenv("FIREBIRD_DLL", str(fake))
        assert platform_runtime.fbclient_dll_path() == fake

    def test_levanta_quando_env_aponta_para_inexistente(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setenv("FIREBIRD_DLL", str(tmp_path / "missing.dll"))
        with pytest.raises(FileNotFoundError, match="fbclient"):
            platform_runtime.fbclient_dll_path()

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Linux-only fallback behavior",
    )
    def test_linux_usa_find_library_quando_env_ausente(
        self, monkeypatch,
    ):
        monkeypatch.delenv("FIREBIRD_DLL", raising=False)
        monkeypatch.setattr(sys, "frozen", False, raising=False)
        from ctypes import util
        monkeypatch.setattr(
            util, "find_library",
            lambda name: "/usr/lib/x86_64-linux-gnu/libfbclient.so.2"
            if name == "fbclient" else None,
        )
        monkeypatch.setattr(Path, "exists", lambda self: True)
        result = platform_runtime.fbclient_dll_path()
        assert "libfbclient" in str(result)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-only code")
class TestWindowsHandler:
    def test_handler_retorna_true(self):
        from dump_agent import platform_runtime as pr
        pr._temp_dirs.clear()
        pr._on_stop_callback = None
        assert pr._windows_handler(0) is True

    def test_handler_chama_callback(self):
        import threading
        from dump_agent import platform_runtime as pr
        called = threading.Event()
        pr._temp_dirs.clear()
        pr._on_stop_callback = called.set
        pr._windows_handler(1)
        assert called.is_set()

    def test_handler_swallows_callback_exception(self):
        from dump_agent import platform_runtime as pr
        pr._temp_dirs.clear()
        pr._on_stop_callback = lambda: 1 / 0
        result = pr._windows_handler(0)
        assert result is True

    def test_handler_limpa_tempdirs(self, tmp_path):
        from dump_agent import platform_runtime as pr
        subdir = tmp_path / "to_clean"
        subdir.mkdir()
        pr._temp_dirs.clear()
        pr._temp_dirs.add(subdir)
        pr._on_stop_callback = None
        pr._windows_handler(0)
        assert not subdir.exists()

    def test_handler_unknown_ctrl_code(self):
        from dump_agent import platform_runtime as pr
        pr._temp_dirs.clear()
        pr._on_stop_callback = None
        result = pr._windows_handler(99)
        assert result is True


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-only code")
class TestInstallWindowsHandlerPortable:
    def test_registra_handler_via_mock(self, monkeypatch):
        from unittest.mock import MagicMock
        from dump_agent import platform_runtime as pr
        fake_kernel32 = MagicMock()
        fake_kernel32.SetConsoleCtrlHandler.return_value = 1
        monkeypatch.setattr(pr, "_kernel32", fake_kernel32)
        sentinel = MagicMock()
        pr._install_windows_handler(sentinel)
        fake_kernel32.SetConsoleCtrlHandler.assert_called_once()
        assert pr._on_stop_callback is sentinel

    def test_levanta_quando_set_console_ctrl_handler_falha(self, monkeypatch):
        from unittest.mock import MagicMock
        from dump_agent import platform_runtime as pr
        fake_kernel32 = MagicMock()
        fake_kernel32.SetConsoleCtrlHandler.return_value = 0
        monkeypatch.setattr(pr, "_kernel32", fake_kernel32)
        with pytest.raises(OSError):
            pr._install_windows_handler(lambda: None)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-only code")
class TestWindowsMutexPortable:
    def test_cria_e_libera_mutex(self):
        from dump_agent import platform_runtime as pr
        with pr._WindowsMutex("test_portable_cria"):
            pass

    def test_levanta_quando_mutex_ja_existe(self):
        from dump_agent import platform_runtime as pr
        with pr._WindowsMutex("test_portable_already"):
            with pytest.raises(RuntimeError, match="already_running"):
                pr._WindowsMutex("test_portable_already")

    def test_levanta_quando_handle_invalido(self, monkeypatch):
        from unittest.mock import MagicMock
        from dump_agent import platform_runtime as pr
        fake_kernel32 = MagicMock()
        fake_kernel32.CreateMutexW.return_value = 0
        fake_kernel32.get_last_error = MagicMock(return_value=5)
        import ctypes
        monkeypatch.setattr(ctypes, "get_last_error", lambda: 5)
        fake_kernel32.CreateMutexW.return_value = None
        original_kernel32 = pr._kernel32
        pr._kernel32 = fake_kernel32
        try:
            with pytest.raises((OSError, RuntimeError, TypeError)):
                pr._WindowsMutex("test_invalid")
        finally:
            pr._kernel32 = original_kernel32


class TestResolveMachineIdEdgeCases:
    def test_arq_existente_vazio_gera_novo_id(self, tmp_path, monkeypatch):
        import sys
        monkeypatch.delenv("MACHINE_ID", raising=False)
        if sys.platform == "win32":
            monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
        else:
            monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
        store = platform_runtime.app_data_dir() / "machine_id"
        store.write_text("", encoding="utf-8")
        mid = platform_runtime.resolve_machine_id()
        assert len(mid) == 8
        assert store.read_text().strip() == mid


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-only raise")
class TestFbclientDllPathWindows:
    def test_levanta_quando_env_ausente_no_windows(self, monkeypatch):
        monkeypatch.delenv("FIREBIRD_DLL", raising=False)
        monkeypatch.setattr(sys, "frozen", False, raising=False)
        with pytest.raises(FileNotFoundError, match="fbclient_windows_requires"):
            platform_runtime.fbclient_dll_path()


class TestInstallShutdownHandlerPortable:
    @pytest.mark.skipif(sys.platform != "win32", reason="Windows-only")
    def test_chama_windows_handler(self, monkeypatch):
        from unittest.mock import MagicMock
        mock_install = MagicMock()
        monkeypatch.setattr(platform_runtime, "_install_windows_handler", mock_install)
        sentinel = MagicMock()
        platform_runtime.install_shutdown_handler(sentinel)
        mock_install.assert_called_once_with(sentinel)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-only code")
class TestAcquireSingleInstanceLockPortable:
    def test_chama_windows_mutex(self, monkeypatch):
        from unittest.mock import MagicMock
        mock_mutex_cls = MagicMock()
        mock_instance = MagicMock()
        mock_mutex_cls.return_value = mock_instance
        monkeypatch.setattr(platform_runtime, "_WindowsMutex", mock_mutex_cls)
        result = platform_runtime.acquire_single_instance_lock("test_lock")
        mock_mutex_cls.assert_called_once_with("test_lock")
        assert result is mock_instance


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-only code")
class TestWindowsMutexExitNoHandle:
    def test_exit_com_handle_none_nao_levanta(self):
        from dump_agent import platform_runtime as pr
        from unittest.mock import MagicMock, patch
        fake_kernel32 = MagicMock()
        fake_kernel32.CreateMutexW.return_value = 1
        import ctypes
        with patch.object(ctypes, "get_last_error", return_value=0):
            mutex = pr._WindowsMutex.__new__(pr._WindowsMutex)
            mutex._handle = None
            mutex.__exit__(None, None, None)
