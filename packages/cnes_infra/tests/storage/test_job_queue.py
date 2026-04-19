"""Testes de integração do job_queue."""

import uuid

import pytest
from sqlalchemy import text

from cnes_infra.storage.job_queue import (
    acquire_for_agent,
    complete_upload,
    enqueue,
    transition_to_streaming,
)


def _criar_payload(engine, tenant_id: str = "355030") -> uuid.UUID:
    payload_id = uuid.uuid4()
    with engine.begin() as con:
        con.execute(
            text(
                "INSERT INTO landing.raw_payload "
                "(id, tenant_id, source_system, competencia, payload) "
                "VALUES (:id, :t, 'CNES', '2026-01', '{}'::jsonb)"
            ),
            {"id": payload_id, "t": tenant_id},
        )
    return payload_id


def _preparar_job_acquired(engine, tenant_id: str = "355030") -> uuid.UUID:
    payload_id = _criar_payload(engine, tenant_id)
    enqueue(engine, tenant_id, "CNES", payload_id)
    job = acquire_for_agent(engine, "machine-1")
    assert job is not None
    return job.id


@pytest.mark.postgres
def test_complete_upload_grava_size_bytes(pg_engine):
    job_id = _preparar_job_acquired(pg_engine)
    transition_to_streaming(pg_engine, job_id, "machine-1")

    ok = complete_upload(
        pg_engine, job_id, "machine-1", "key/abc.parquet.gz", 2048,
    )

    assert ok is True
    with pg_engine.connect() as con:
        row = con.execute(text(
            "SELECT r.size_bytes, r.object_key "
            "FROM queue.jobs j "
            "JOIN landing.raw_payload r ON r.id = j.payload_id "
            "WHERE j.id = :id"
        ), {"id": job_id}).first()
    assert row.size_bytes == 2048
    assert row.object_key == "key/abc.parquet.gz"


@pytest.mark.postgres
def test_complete_upload_rejeita_chamada_sem_size_bytes(pg_engine):
    job_id = _preparar_job_acquired(pg_engine)
    transition_to_streaming(pg_engine, job_id, "machine-1")

    with pytest.raises(TypeError):
        complete_upload(  # type: ignore[call-arg]
            pg_engine, job_id, "machine-1", "key/x.gz",
        )
