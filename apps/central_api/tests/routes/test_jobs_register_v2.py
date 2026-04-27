"""Tests for /api/v1/jobs/register N-file manifest route (Task 7)."""
from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text

from cnes_infra.storage import extractions_repo

if TYPE_CHECKING:
    from fastapi.testclient import TestClient

pytestmark = pytest.mark.postgres


_TENANT = "354130"


class TestJobsRegisterV2:
    def test_register_com_dois_files_persiste(
        self, api_client: TestClient, pg_engine,
    ) -> None:
        job_id = extractions_repo.enqueue(
            pg_engine,
            tenant_id=_TENANT,
            source_type="BPA_MAG",
            competencia=date(2026, 1, 1),
            files=[{
                "minio_key": "bpa/placeholder.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 1,
                "sha256": "0" * 64,
            }],
        )

        resp = api_client.post(
            "/api/v1/jobs/register",
            json={
                "job_id": str(job_id),
                "files": [
                    {"minio_key": "bpa/2026-01/bpa_c.parquet.gz",
                     "fato_subtype": "BPA_C",
                     "size_bytes": 1024, "sha256": "a" * 64},
                    {"minio_key": "bpa/2026-01/bpa_i.parquet.gz",
                     "fato_subtype": "BPA_I",
                     "size_bytes": 2048, "sha256": "b" * 64},
                ],
            },
            headers={"X-Tenant-Id": _TENANT},
        )
        assert resp.status_code == 200

        with pg_engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT files, status FROM landing.extractions "
                    "WHERE job_id = :j",
                ),
                {"j": str(job_id)},
            ).one()
        assert len(row.files) == 2
        assert row.status == "REGISTERED"

    def test_register_rejeita_files_vazio(
        self, api_client: TestClient,
    ) -> None:
        resp = api_client.post(
            "/api/v1/jobs/register",
            json={
                "job_id": "00000000-0000-0000-0000-000000000000",
                "files": [],
            },
            headers={"X-Tenant-Id": _TENANT},
        )
        assert resp.status_code == 422

    def test_register_job_inexistente_404(
        self, api_client: TestClient,
    ) -> None:
        resp = api_client.post(
            "/api/v1/jobs/register",
            json={
                "job_id": "00000000-0000-0000-0000-000000000000",
                "files": [{
                    "minio_key": "x.parquet.gz",
                    "fato_subtype": "BPA_C",
                    "size_bytes": 1,
                    "sha256": "0" * 64,
                }],
            },
            headers={"X-Tenant-Id": _TENANT},
        )
        assert resp.status_code == 404

    def test_register_persiste_agent_version_e_machine_id(
        self, api_client: TestClient, pg_engine,
    ) -> None:
        job_id = extractions_repo.enqueue(
            pg_engine,
            tenant_id=_TENANT,
            source_type="BPA_MAG",
            competencia=date(2026, 2, 1),
            files=[{
                "minio_key": "bpa/placeholder.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 1,
                "sha256": "0" * 64,
            }],
        )

        resp = api_client.post(
            "/api/v1/jobs/register",
            json={
                "job_id": str(job_id),
                "files": [{
                    "minio_key": "bpa/2026-02/bpa_c.parquet.gz",
                    "fato_subtype": "BPA_C",
                    "size_bytes": 1024,
                    "sha256": "a" * 64,
                }],
                "agent_version": "1.2.3",
                "machine_id": "edge-01",
            },
            headers={"X-Tenant-Id": _TENANT},
        )
        assert resp.status_code == 200

        with pg_engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT agent_version, machine_id "
                    "FROM landing.extractions WHERE job_id = :j",
                ),
                {"j": str(job_id)},
            ).one()
        assert row.agent_version == "1.2.3"
        assert row.machine_id == "edge-01"

    def test_register_sem_metadata_persiste_null(
        self, api_client: TestClient, pg_engine,
    ) -> None:
        job_id = extractions_repo.enqueue(
            pg_engine,
            tenant_id=_TENANT,
            source_type="BPA_MAG",
            competencia=date(2026, 3, 1),
            files=[{
                "minio_key": "bpa/placeholder.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 1,
                "sha256": "0" * 64,
            }],
        )

        resp = api_client.post(
            "/api/v1/jobs/register",
            json={
                "job_id": str(job_id),
                "files": [{
                    "minio_key": "bpa/2026-03/bpa_c.parquet.gz",
                    "fato_subtype": "BPA_C",
                    "size_bytes": 1024,
                    "sha256": "a" * 64,
                }],
            },
            headers={"X-Tenant-Id": _TENANT},
        )
        assert resp.status_code == 200

        with pg_engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT agent_version, machine_id "
                    "FROM landing.extractions WHERE job_id = :j",
                ),
                {"j": str(job_id)},
            ).one()
        assert row.agent_version is None
        assert row.machine_id is None
