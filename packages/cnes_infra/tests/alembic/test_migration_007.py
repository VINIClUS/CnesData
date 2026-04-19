"""Verifica que a migração 007 aplica os storage params de autovacuum."""

import pytest
from sqlalchemy import text


@pytest.mark.postgres
def test_007_aplica_autovacuum_params_em_queue_jobs(pg_engine):
    with pg_engine.connect() as con:
        result = con.execute(text(
            "SELECT reloptions FROM pg_class "
            "WHERE oid = 'queue.jobs'::regclass"
        )).scalar()
    assert result is not None
    opts = set(result)
    assert "autovacuum_vacuum_scale_factor=0.05" in opts
    assert "autovacuum_vacuum_threshold=50" in opts
    assert "autovacuum_analyze_scale_factor=0.02" in opts
    assert "autovacuum_analyze_threshold=50" in opts
    assert "autovacuum_vacuum_cost_delay=2" in opts
    assert "autovacuum_vacuum_cost_limit=1000" in opts


@pytest.mark.postgres
def test_007_aplica_autovacuum_params_em_queue_jobs_dlq(pg_engine):
    with pg_engine.connect() as con:
        result = con.execute(text(
            "SELECT reloptions FROM pg_class "
            "WHERE oid = 'queue.jobs_dlq'::regclass"
        )).scalar()
    assert result is not None
    opts = set(result)
    assert "autovacuum_vacuum_scale_factor=0.1" in opts
    assert "autovacuum_vacuum_threshold=100" in opts


@pytest.mark.postgres
def test_007_aplica_autovacuum_params_em_landing_raw_payload(pg_engine):
    with pg_engine.connect() as con:
        result = con.execute(text(
            "SELECT reloptions FROM pg_class "
            "WHERE oid = 'landing.raw_payload'::regclass"
        )).scalar()
    assert result is not None
    opts = set(result)
    assert "autovacuum_vacuum_scale_factor=0.1" in opts
    assert "autovacuum_vacuum_threshold=100" in opts
