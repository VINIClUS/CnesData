"""Tests for POST /api/v1/jobs/{job_id}/fail (A1).

Before this route existed, a failed extraction 404'd on every FailJob call
from the agent — outbox_adapter.go's classify.go treats 404 as a terminal
drop, so the envelope was deleted and landing.extractions stayed PENDING
forever with the real error lost. See
docs/edge-agent-audit-2026-09-20.md.
"""
from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import text

from cnes_infra.storage import extractions_repo

if TYPE_CHECKING:
    from fastapi.testclient import TestClient

pytestmark = pytest.mark.postgres

_TENANT = "354130"


def _enqueue(pg_engine, **overrides) -> object:
    defaults = {
        "tenant_id": _TENANT,
        "source_type": "BPA_MAG",
        "competencia": date(2026, 1, 1),
        "files": [{
            "minio_key": "bpa/placeholder.parquet.gz",
            "fato_subtype": "BPA_C",
            "size_bytes": 1,
            "sha256": "0" * 64,
        }],
    }
    defaults.update(overrides)
    return extractions_repo.enqueue(pg_engine, **defaults)


class TestJobsFailV2:
    def test_fail_marca_status_e_persiste_error_detail(
        self, api_client: TestClient, pg_engine,
    ) -> None:
        job_id = _enqueue(pg_engine)

        resp = api_client.post(
            f"/api/v1/jobs/{job_id}/fail",
            json={"error": "firebird_unreachable"},
            headers={"X-Tenant-Id": _TENANT},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"job_id": str(job_id), "status": "FAILED"}

        with pg_engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT status, error_detail FROM landing.extractions "
                    "WHERE job_id = :j",
                ),
                {"j": str(job_id)},
            ).one()
        assert row.status == "FAILED"
        assert row.error_detail == "firebird_unreachable"

    def test_fail_job_inexistente_404(self, api_client: TestClient) -> None:
        resp = api_client.post(
            f"/api/v1/jobs/{uuid4()}/fail",
            json={"error": "whatever"},
            headers={"X-Tenant-Id": _TENANT},
        )
        assert resp.status_code == 404

    def test_fail_rejeita_erro_vazio(
        self, api_client: TestClient, pg_engine,
    ) -> None:
        job_id = _enqueue(pg_engine)
        resp = api_client.post(
            f"/api/v1/jobs/{job_id}/fail",
            json={"error": ""},
            headers={"X-Tenant-Id": _TENANT},
        )
        assert resp.status_code == 422

    def test_fail_rejeita_job_id_invalido(self, api_client: TestClient) -> None:
        resp = api_client.post(
            "/api/v1/jobs/not-a-uuid/fail",
            json={"error": "x"},
            headers={"X-Tenant-Id": _TENANT},
        )
        assert resp.status_code == 422

    def test_fail_retry_e_idempotente_preserva_motivo_original(
        self, api_client: TestClient, pg_engine,
    ) -> None:
        """Agent's outbox can retry a /fail call after a transient error;
        the second attempt must not clobber the original reason nor error
        out — it 404s (row is no longer PENDING/CLAIMED), matching the
        terminal-drop classification the Go client already applies."""
        job_id = _enqueue(pg_engine)

        first = api_client.post(
            f"/api/v1/jobs/{job_id}/fail",
            json={"error": "original_cause"},
            headers={"X-Tenant-Id": _TENANT},
        )
        assert first.status_code == 200

        second = api_client.post(
            f"/api/v1/jobs/{job_id}/fail",
            json={"error": "retry_cause"},
            headers={"X-Tenant-Id": _TENANT},
        )
        assert second.status_code == 404

        with pg_engine.begin() as conn:
            error_detail = conn.execute(
                text(
                    "SELECT error_detail FROM landing.extractions "
                    "WHERE job_id = :j",
                ),
                {"j": str(job_id)},
            ).scalar_one()
        assert error_detail == "original_cause"

    def test_fail_nao_afeta_job_ja_completed(
        self, api_client: TestClient, pg_engine,
    ) -> None:
        job_id = _enqueue(pg_engine)
        extractions_repo.mark_completed(pg_engine, job_id=job_id)

        resp = api_client.post(
            f"/api/v1/jobs/{job_id}/fail",
            json={"error": "too_late"},
            headers={"X-Tenant-Id": _TENANT},
        )
        assert resp.status_code == 404

        with pg_engine.begin() as conn:
            status = conn.execute(
                text(
                    "SELECT status FROM landing.extractions WHERE job_id = :j",
                ),
                {"j": str(job_id)},
            ).scalar_one()
        assert status == "COMPLETED"
