"""central_api survives Postgres container restart (testcontainers chaos_infra)."""
from __future__ import annotations

import time

import pytest


@pytest.mark.chaos_infra
def test_central_api_sobrevive_pg_restart():
    pytest.importorskip("testcontainers.postgres")
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as pg:
        engine = create_engine(pg.get_connection_url())

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        pg.stop()
        time.sleep(1)
        with pytest.raises(Exception):
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        pg.start()

        for _ in range(30):
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                break
            except Exception:
                time.sleep(1)
        else:
            pytest.fail("pg_chaos_recovery_timeout")
