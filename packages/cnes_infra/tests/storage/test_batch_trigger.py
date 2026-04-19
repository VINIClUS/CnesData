"""Testes de integração do batch_trigger."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from cnes_infra.storage.batch_trigger import (
    Thresholds,
    close_if_drained,
    evaluate_and_open,
    read_state,
)
from cnes_infra.storage.job_queue import enqueue

_TENANT = "355030"


def _limpar_batch_trigger(engine):
    with engine.begin() as con:
        con.execute(text(
            "UPDATE queue.batch_trigger SET "
            "status='CLOSED', opened_at=NULL, closed_at=NULL, "
            "reason=NULL, pending_bytes=NULL, oldest_completed_at=NULL "
            "WHERE tenant_id IS NULL"
        ))
        con.execute(text("DELETE FROM queue.jobs"))
        con.execute(text("DELETE FROM landing.raw_payload"))


def _criar_job_completed(
    engine, size_bytes: int = 0,
    completed_offset_days: int = 0,
) -> uuid.UUID:
    payload_id = uuid.uuid4()
    completed_at = datetime.now(UTC) - timedelta(days=completed_offset_days)
    with engine.begin() as con:
        con.execute(text(
            "INSERT INTO landing.raw_payload "
            "(id, tenant_id, source_system, competencia, payload, size_bytes) "
            "VALUES (:id, :t, 'CNES', '2026-01', '{}'::jsonb, :s)"
        ), {"id": payload_id, "t": _TENANT, "s": size_bytes})
    job_id = enqueue(engine, _TENANT, "CNES", payload_id)
    with engine.begin() as con:
        con.execute(text(
            "UPDATE queue.jobs SET "
            "status='COMPLETED', completed_at=:c "
            "WHERE id = :id"
        ), {"id": job_id, "c": completed_at})
    return job_id


@pytest.fixture
def reset_trigger(pg_engine):
    _limpar_batch_trigger(pg_engine)
    yield
    _limpar_batch_trigger(pg_engine)


@pytest.mark.postgres
def test_read_state_retorna_seed_row_closed(pg_engine, reset_trigger):
    state = read_state(pg_engine)
    assert state is not None
    assert state.status == "CLOSED"


@pytest.mark.postgres
def test_evaluate_nao_abre_sem_threshold_batido(pg_engine, reset_trigger):
    _criar_job_completed(pg_engine, size_bytes=1024)
    state = evaluate_and_open(
        pg_engine, Thresholds(size_bytes=100_000_000, age_days=365),
    )
    assert state.status == "CLOSED"
    assert state.pending_bytes == 1024


@pytest.mark.postgres
def test_evaluate_abre_por_size_threshold(pg_engine, reset_trigger):
    _criar_job_completed(pg_engine, size_bytes=200)
    state = evaluate_and_open(
        pg_engine, Thresholds(size_bytes=100, age_days=365),
    )
    assert state.status == "OPEN"
    assert state.reason == "size_threshold"


@pytest.mark.postgres
def test_evaluate_abre_por_age_threshold(pg_engine, reset_trigger):
    _criar_job_completed(
        pg_engine, size_bytes=1, completed_offset_days=10,
    )
    state = evaluate_and_open(
        pg_engine, Thresholds(size_bytes=10_000_000, age_days=2),
    )
    assert state.status == "OPEN"
    assert state.reason == "age_threshold"


@pytest.mark.postgres
def test_evaluate_size_vence_age_em_tie(pg_engine, reset_trigger):
    _criar_job_completed(
        pg_engine, size_bytes=200, completed_offset_days=10,
    )
    state = evaluate_and_open(
        pg_engine, Thresholds(size_bytes=100, age_days=2),
    )
    assert state.reason == "size_threshold"


@pytest.mark.postgres
def test_evaluate_idempotente_preserva_opened_at(pg_engine, reset_trigger):
    _criar_job_completed(pg_engine, size_bytes=200)
    s1 = evaluate_and_open(
        pg_engine, Thresholds(size_bytes=100, age_days=365),
    )
    _criar_job_completed(pg_engine, size_bytes=50)
    s2 = evaluate_and_open(
        pg_engine, Thresholds(size_bytes=100, age_days=365),
    )
    assert s1.opened_at is not None
    assert s2.opened_at == s1.opened_at
    assert s2.pending_bytes == 250


@pytest.mark.postgres
def test_evaluate_refresh_pending_bytes_mesmo_quando_fica_closed(
    pg_engine, reset_trigger,
):
    _criar_job_completed(pg_engine, size_bytes=1024)
    state = evaluate_and_open(
        pg_engine, Thresholds(size_bytes=100_000_000, age_days=365),
    )
    assert state.status == "CLOSED"
    assert state.pending_bytes == 1024


@pytest.mark.postgres
def test_close_fecha_quando_fila_vazia(pg_engine, reset_trigger):
    _criar_job_completed(pg_engine, size_bytes=200)
    evaluate_and_open(
        pg_engine, Thresholds(size_bytes=100, age_days=365),
    )
    with pg_engine.begin() as con:
        con.execute(text("DELETE FROM queue.jobs"))
    fechou = close_if_drained(pg_engine)
    assert fechou is True
    state = read_state(pg_engine)
    assert state.status == "CLOSED"


@pytest.mark.postgres
def test_close_nao_fecha_se_jobs_pendentes(pg_engine, reset_trigger):
    _criar_job_completed(pg_engine, size_bytes=200)
    evaluate_and_open(
        pg_engine, Thresholds(size_bytes=100, age_days=365),
    )
    fechou = close_if_drained(pg_engine)
    assert fechou is False
    state = read_state(pg_engine)
    assert state.status == "OPEN"


@pytest.mark.postgres
def test_read_state_retorna_none_para_tenant_inexistente(pg_engine, reset_trigger):
    state = read_state(pg_engine, tenant_id="999999")
    assert state is None
