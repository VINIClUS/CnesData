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
