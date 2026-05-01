"""Tests for scripts/seed_dims_publicos.py."""
from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from scripts.seed_dims_publicos import seed_all

pytestmark = pytest.mark.postgres


FIXTURES = Path("docs/fixtures/reference-data")


@pytest.fixture
def pg_engine():
    """Create Postgres connection for tests."""
    import os
    url = os.getenv(
        "PG_TEST_URL",
        "postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test",
    )
    engine = create_engine(url)
    try:
        with engine.connect() as con:
            con.execute(text("SELECT 1"))
    except Exception:
        pytest.skip(
            f"postgres unavailable at {url}; "
            "run 'docker compose up -d postgres' first",
        )
    return engine


def test_seed_all_retorna_contagens(pg_engine):
    with pg_engine.begin() as conn:
        counts = seed_all(conn, FIXTURES)
    assert counts["cbo"] >= 1
    assert counts["cid"] >= 1
    assert counts["municipio"] >= 1
    assert counts["sigtap"] >= 1


def test_seed_idempotente(pg_engine):
    with pg_engine.begin() as conn:
        seed_all(conn, FIXTURES)
    with pg_engine.begin() as conn:
        seed_all(conn, FIXTURES)
    with pg_engine.begin() as conn:
        cbo_count = conn.execute(text("SELECT COUNT(*) FROM gold.dim_cbo")).scalar()
    assert cbo_count >= 1
