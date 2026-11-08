"""Root conftest — exposes shared fixtures across packages/ and apps/ tests.

Defined inline (no `from tests.conftest import ...`) to avoid pytest
module-discovery conflicts with `apps/*/tests/` dirs that lack `__init__.py`.
"""
from __future__ import annotations

import pytest


@pytest.fixture
def assert_query_limit():
    def _check(response, limit: int = 15) -> None:
        count = int(response.headers.get("X-Query-Count", "0"))
        assert count <= limit, f"n_plus_1_violation count={count} limit={limit}"
    return _check


@pytest.fixture(scope="session")
def pg_engine():
    """Disponibiliza Postgres sob demanda para testes de integração."""
    import os

    from alembic import command
    from alembic.config import Config
    from sqlalchemy import create_engine, text

    url = os.getenv(
        "PG_TEST_URL",
        "postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test",
    )
    engine = create_engine(url)
    try:
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        except Exception:
            pytest.skip("postgres_unavailable configure=PG_TEST_URL")
        config = Config()
        config.set_main_option("script_location", "cnes_data:alembic")
        config.set_main_option("sqlalchemy.url", url)
        command.upgrade(config, "head")
        yield engine
    finally:
        engine.dispose()


__all__ = ["assert_query_limit", "pg_engine"]
