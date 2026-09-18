"""Tests for extractions_repo.mint_upload_url (FU1 T2)."""
from __future__ import annotations

from datetime import date
from uuid import uuid4

import pytest
from sqlalchemy import text

from cnes_infra.storage import extractions_repo

pytestmark = pytest.mark.postgres


@pytest.fixture
def _cleanup_extractions(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(text("TRUNCATE landing.extractions CASCADE"))
    yield
    with pg_engine.begin() as conn:
        conn.execute(text("TRUNCATE landing.extractions CASCADE"))


@pytest.mark.usefixtures("_cleanup_extractions")
def test_aceita_insere_pending_row(pg_engine) -> None:
    job_id = uuid4()
    minio_key = "354130/CNES_VINCULO/2026-01-01/foo.parquet.gz"
    result = extractions_repo.mint_upload_url(
        pg_engine,
        job_id=job_id,
        tenant_id="354130",
        source_type="CNES_LOCAL",
        competencia=date(2026, 1, 1),
        fato_subtype="CNES_VINCULO",
        minio_key=minio_key,
        agent_version="0.5.0",
        machine_id="EDGE-01",
    )
    assert result == job_id

    with pg_engine.begin() as conn:
        conn.execute(text("SET LOCAL row_security = off"))
        row = conn.execute(
            text(
                "SELECT status, agent_version, machine_id, files "
                "FROM landing.extractions WHERE job_id = :j"
            ),
            {"j": str(job_id)},
        ).one()
    assert row.status == "PENDING"
    assert row.agent_version == "0.5.0"
    assert row.machine_id == "EDGE-01"
    assert row.files[0]["minio_key"] == minio_key
    assert row.files[0]["fato_subtype"] == "CNES_VINCULO"
    assert row.files[0]["sha256"] == "0" * 64


@pytest.mark.usefixtures("_cleanup_extractions")
def test_rejeita_duplicate_job_id(pg_engine) -> None:
    job_id = uuid4()
    extractions_repo.mint_upload_url(
        pg_engine,
        job_id=job_id,
        tenant_id="354130",
        source_type="CNES_LOCAL",
        competencia=date(2026, 1, 1),
        fato_subtype="CNES_VINCULO",
        minio_key="a.parquet.gz",
    )
    second = extractions_repo.mint_upload_url(
        pg_engine,
        job_id=job_id,
        tenant_id="354130",
        source_type="CNES_LOCAL",
        competencia=date(2026, 1, 1),
        fato_subtype="CNES_VINCULO",
        minio_key="a.parquet.gz",
    )
    assert second is None
