"""Verifica migração 017 — agent_version + machine_id em landing.extractions."""

import pytest
from sqlalchemy import text


@pytest.mark.postgres
def test_017_adiciona_coluna_agent_version_nullable(pg_engine):
    with pg_engine.connect() as con:
        row = con.execute(text(
            "SELECT data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema='landing' AND table_name='extractions' "
            "AND column_name='agent_version'"
        )).one()
    assert row.data_type == "text"
    assert row.is_nullable == "YES"


@pytest.mark.postgres
def test_017_adiciona_coluna_machine_id_nullable(pg_engine):
    with pg_engine.connect() as con:
        row = con.execute(text(
            "SELECT data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema='landing' AND table_name='extractions' "
            "AND column_name='machine_id'"
        )).one()
    assert row.data_type == "text"
    assert row.is_nullable == "YES"


@pytest.mark.postgres
def test_017_cria_indice_parcial_machine_id(pg_engine):
    with pg_engine.connect() as con:
        existe = con.execute(text(
            "SELECT 1 FROM pg_indexes "
            "WHERE schemaname='landing' AND indexname='ix_extractions_machine_id'"
        )).scalar()
    assert existe == 1
