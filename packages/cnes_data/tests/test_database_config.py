"""Precedência da configuração de banco sem depender de Runtime."""

import pytest

from cnes_data import config


def test_override_nao_le_ambiente_nem_dotenv(monkeypatch):
    monkeypatch.delenv("DB_URL", raising=False)

    def reject_load(**kwargs):
        pytest.fail("dotenv_loaded_with_explicit_override")

    monkeypatch.setattr(config, "load_dotenv", reject_load)
    assert config.resolve_database_url("sqlite:///explicit.db") == "sqlite:///explicit.db"


def test_sem_override_resolve_db_url_do_ambiente(monkeypatch):
    monkeypatch.setattr(config, "load_dotenv", lambda **kwargs: None)
    monkeypatch.setenv("DB_URL", "postgresql://user:password@localhost/database")
    assert config.resolve_database_url(None) == (
        "postgresql+psycopg://user:password@localhost/database"
    )


def test_sem_override_nem_db_url_falha_explicitamente(monkeypatch):
    monkeypatch.setattr(config, "load_dotenv", lambda **kwargs: None)
    monkeypatch.delenv("DB_URL", raising=False)
    with pytest.raises(OSError, match="env_missing key=DB_URL"):
        config.resolve_database_url("")
