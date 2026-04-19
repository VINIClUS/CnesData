"""Testes de config do batch_watcher."""

import importlib


def test_defaults_quando_env_nao_setada(monkeypatch):
    monkeypatch.delenv("WATCHER_SIZE_THRESHOLD_MB", raising=False)
    monkeypatch.delenv("WATCHER_AGE_THRESHOLD_DAYS", raising=False)
    import batch_watcher.config as cfg
    importlib.reload(cfg)
    assert cfg.SIZE_THRESHOLD_MB == 100
    assert cfg.AGE_THRESHOLD_DAYS == 2


def test_override_via_env(monkeypatch):
    monkeypatch.setenv("WATCHER_SIZE_THRESHOLD_MB", "500")
    monkeypatch.setenv("WATCHER_AGE_THRESHOLD_DAYS", "7")
    import batch_watcher.config as cfg
    importlib.reload(cfg)
    assert cfg.SIZE_THRESHOLD_MB == 500
    assert cfg.AGE_THRESHOLD_DAYS == 7
