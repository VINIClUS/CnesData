"""Fixtures para testes de /api/v1/jobs com Postgres real."""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
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
        pytest.skip(
            f"postgres indisponível em {_PG_URL}; "
            "rode 'docker compose up -d' primeiro",
        )
    cfg = Config()
    cfg.set_main_option("script_location", "cnes_infra:alembic")
    cfg.set_main_option("sqlalchemy.url", _PG_URL)
    command.upgrade(cfg, "head")
    yield engine
    engine.dispose()


@pytest.fixture
def api_client(pg_engine):
    from central_api.deps import get_conn, get_engine

    with (
        patch("central_api.app.init_telemetry"),
        patch("central_api.deps.install_rls_listener"),
        patch("central_api.deps.instrument_engine"),
        patch("central_api.deps.install_query_counter"),
        patch("central_api.deps.create_engine", return_value=pg_engine),
    ):
        from central_api.app import create_app
        app = create_app()

    def _override_conn():
        with pg_engine.begin() as conn:
            yield conn

    app.dependency_overrides[get_engine] = lambda: pg_engine
    app.dependency_overrides[get_conn] = _override_conn
    with TestClient(app, raise_server_exceptions=True) as client:
        yield client
    app.dependency_overrides.clear()
