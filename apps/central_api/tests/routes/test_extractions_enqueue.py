"""Tests for POST /api/v1/extractions/enqueue admin route (Task 8)."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text

if TYPE_CHECKING:
    from fastapi.testclient import TestClient

pytestmark = pytest.mark.postgres


_TENANT = "354130"


class TestExtractionsEnqueue:
    def test_enqueue_bpa_mag_cria_dois_jobs(
        self, api_client: TestClient, pg_engine,
    ) -> None:
        resp = api_client.post(
            "/api/v1/extractions/enqueue",
            json={"source_type": "BPA_MAG",
                  "tenant_id": _TENANT,
                  "competencia": "2026-02-01"},
            headers={"X-Admin-Token": "test-admin",
                     "X-Tenant-Id": _TENANT},
        )
        assert resp.status_code == 201
        body = resp.json()
        assert len(body["job_ids"]) == 2
        returned_ids = {str(j) for j in body["job_ids"]}

        with pg_engine.begin() as conn:
            rows = conn.execute(text(
                "SELECT job_id FROM landing.extractions "
                "WHERE tenant_id = :t "
                "  AND source_type = 'BPA_MAG' "
                "  AND competencia = '2026-02-01'",
            ), {"t": _TENANT}).fetchall()
        persisted_ids = {str(r[0]) for r in rows}
        assert returned_ids <= persisted_ids
        assert len(returned_ids) == 2

    def test_enqueue_sia_local_cria_cinco_jobs_com_deps(
        self, api_client: TestClient, pg_engine,
    ) -> None:
        resp = api_client.post(
            "/api/v1/extractions/enqueue",
            json={"source_type": "SIA_LOCAL",
                  "tenant_id": _TENANT,
                  "competencia": "2026-02-02"},
            headers={"X-Admin-Token": "test-admin",
                     "X-Tenant-Id": _TENANT},
        )
        assert resp.status_code == 201
        job_ids = resp.json()["job_ids"]
        assert len(job_ids) == 5

        with pg_engine.begin() as conn:
            rows = conn.execute(text(
                "SELECT files, depends_on FROM landing.extractions "
                "WHERE job_id = ANY(CAST(:ids AS uuid[]))",
            ), {"ids": "{" + ",".join(job_ids) + "}"}).fetchall()

        fato_rows = [r for r in rows
                     if not r[0][0]["fato_subtype"].startswith("DIM_")]
        assert len(fato_rows) == 3
        for fato in fato_rows:
            assert len(fato.depends_on) >= 1

    def test_enqueue_sem_admin_token_401(
        self, api_client: TestClient,
    ) -> None:
        resp = api_client.post(
            "/api/v1/extractions/enqueue",
            json={"source_type": "BPA_MAG",
                  "tenant_id": _TENANT,
                  "competencia": "2026-01-03"},
            headers={"X-Tenant-Id": _TENANT},
        )
        assert resp.status_code == 401

    def test_enqueue_source_invalido_422(
        self, api_client: TestClient,
    ) -> None:
        resp = api_client.post(
            "/api/v1/extractions/enqueue",
            json={"source_type": "UNKNOWN",
                  "tenant_id": _TENANT,
                  "competencia": "2026-01-04"},
            headers={"X-Admin-Token": "test-admin",
                     "X-Tenant-Id": _TENANT},
        )
        assert resp.status_code == 422
