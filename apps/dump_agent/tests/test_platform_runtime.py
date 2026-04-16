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
