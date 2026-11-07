"""Fixtures para testes de integracao dos repositorios."""
import pytest


@pytest.fixture
def pg_conn(pg_engine):
    conn = pg_engine.connect()
    trans = conn.begin()
    try:
        yield conn
    finally:
        trans.rollback()
        conn.close()
