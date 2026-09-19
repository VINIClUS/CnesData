"""central_api survives Postgres container restart (testcontainers chaos_infra)."""
from __future__ import annotations

import socket
import time

import pytest


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.mark.chaos_infra
def test_central_api_sobrevive_pg_restart():
    pytest.importorskip("testcontainers.postgres")
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text
    from testcontainers.postgres import PostgresContainer

    pg = PostgresContainer("postgres:16-alpine").with_bind_ports(5432, _free_port())
    with pg:
        engine = create_engine(pg.get_connection_url())

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        pg.get_wrapped_container().stop()
        time.sleep(1)
        with pytest.raises(Exception):
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        pg.get_wrapped_container().start()

        for _ in range(30):
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                break
            except Exception:
                time.sleep(1)
        else:
            pytest.fail("pg_chaos_recovery_timeout")
