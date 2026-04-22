"""SQLAlchemy query counter listener tests."""
from __future__ import annotations

from sqlalchemy import create_engine, text

from cnes_infra.storage.query_counter import install_query_counter


def test_listener_incrementa_ao_executar_query(monkeypatch):
    counts = []

    def fake_increment():
        counts.append(1)

    from central_api import middleware
    monkeypatch.setattr(middleware, "increment_query_count", fake_increment)

    engine = create_engine("sqlite:///:memory:")
    install_query_counter(engine)

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        conn.execute(text("SELECT 2"))
        conn.execute(text("SELECT 3"))

    assert len(counts) == 3


def test_listener_nao_crasha_sem_contexto_middleware():
    engine = create_engine("sqlite:///:memory:")
    install_query_counter(engine)

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
