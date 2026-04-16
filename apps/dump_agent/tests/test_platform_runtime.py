"""Portable tests for platform_runtime (run on both Linux and Windows)."""
from __future__ import annotations

import sys
from pathlib import Path  # noqa: F401

import pytest  # noqa: F401

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
