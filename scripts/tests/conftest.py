"""Shared fixtures for scripts/tests/.

Provides `pg_engine` (session-scoped) mirroring packages/cnes_infra/tests/conftest.py
so seed script tests can run `gold.*` queries after alembic upgrade head.
"""
from __future__ import annotations

import os

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text

_PG_URL = os.getenv(
    "PG_TEST_URL",
    "postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test",
)


@pytest.fixture(scope="session")
def pg_engine():
    engine = create_engine(_PG_URL)
    try:
        with engine.connect() as con:
            con.execute(text("SELECT 1"))
    except Exception:
        pytest.skip(f"postgres indisponível em {_PG_URL}; rode 'docker compose up -d' primeiro")
    cfg = Config()
    cfg.set_main_option("script_location", "cnes_infra:alembic")
    cfg.set_main_option("sqlalchemy.url", _PG_URL)
    command.upgrade(cfg, "head")
    yield engine
    engine.dispose()
