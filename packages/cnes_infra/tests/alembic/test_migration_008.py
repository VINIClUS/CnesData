"""Verifica que a migração 008 cria batch_trigger e adiciona size_bytes."""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError


@pytest.mark.postgres
def test_008_adiciona_size_bytes_com_default_zero(pg_engine):
    with pg_engine.connect() as con:
        result = con.execute(text(
            "SELECT data_type, column_default, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = 'landing' "
            "  AND table_name = 'raw_payload' "
            "  AND column_name = 'size_bytes'"
        )).first()
    assert result is not None, "size_bytes column missing"
    assert result.data_type == "bigint"
    assert result.column_default == "0"
    assert result.is_nullable == "NO"


@pytest.mark.postgres
def test_008_cria_batch_trigger_com_check_constraint_status(pg_engine):
    with pg_engine.connect() as con:
        result = con.execute(text(
            "SELECT pg_get_constraintdef(oid) "
            "FROM pg_constraint "
            "WHERE conname = 'chk_batch_trigger_status'"
        )).scalar()
    assert result is not None
    assert "'OPEN'" in result
    assert "'CLOSED'" in result


@pytest.mark.postgres
def test_008_cria_batch_trigger_com_check_constraint_tenant(pg_engine):
    with pg_engine.connect() as con:
        result = con.execute(text(
            "SELECT pg_get_constraintdef(oid) "
            "FROM pg_constraint "
            "WHERE conname = 'chk_batch_trigger_tenant'"
        )).scalar()
    assert result is not None
    assert "tenant_id IS NULL" in result


@pytest.mark.postgres
def test_008_insere_seed_row_global_closed(pg_engine):
    with pg_engine.connect() as con:
        result = con.execute(text(
            "SELECT status, tenant_id FROM queue.batch_trigger "
            "WHERE tenant_id IS NULL"
        )).first()
    assert result is not None
    assert result.status == "CLOSED"
    assert result.tenant_id is None


@pytest.mark.postgres
def test_008_impede_duplicar_seed_global(pg_engine):
    with pg_engine.connect() as con, pytest.raises(IntegrityError):
        con.execute(text(
            "INSERT INTO queue.batch_trigger (tenant_id, status) "
            "VALUES (NULL, 'CLOSED')"
        ))
