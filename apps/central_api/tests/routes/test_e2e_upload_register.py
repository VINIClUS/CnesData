"""E2E: mint -> fake MinIO PUT -> register; sha256 lands em landing.extractions."""
from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import text

if TYPE_CHECKING:
    from fastapi.testclient import TestClient

pytestmark = pytest.mark.postgres


_TENANT = "354130"


class _FakeStorage:
    def presigned_put(self, key: str, expires: int = 3600) -> str:
        return f"https://minio/fake?key={key}&exp={expires}"


@pytest.fixture
def _cleanup_extractions(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(text("TRUNCATE landing.extractions CASCADE"))
    yield
    with pg_engine.begin() as conn:
        conn.execute(text("TRUNCATE landing.extractions CASCADE"))


def _mint_upload_url(client: TestClient, job_id: str) -> str:
    resp = client.post(
        "/api/v1/jobs/upload-url",
        headers={"X-Tenant-Id": _TENANT},
        json={
            "job_id": job_id,
            "tenant_id": _TENANT,
            "source_type": "CNES_LOCAL",
            "tipo_extracao": "profissionais",
            "competencia": "2026-01-01",
            "intent": "cnes_profissionais",
            "agent_version": "0.5.0",
            "machine_id": "EDGE-01",
        },
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["extraction_id"] == job_id
    minio_key = body["minio_key"]
    assert minio_key.endswith(".parquet.gz")
    return minio_key


def _register_extraction(
    client: TestClient, job_id: str, minio_key: str, sha: str, size: int,
) -> dict:
    resp = client.post(
        "/api/v1/jobs/register",
        headers={"X-Tenant-Id": _TENANT},
        json={
            "job_id": job_id,
            "files": [{
                "minio_key": minio_key,
                "fato_subtype": "CNES_VINCULO",
                "size_bytes": size,
                "sha256": sha,
            }],
            "agent_version": "0.5.0",
            "machine_id": "EDGE-01",
            "sha256": sha,
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


@pytest.mark.usefixtures("_cleanup_extractions")
def test_e2e_aceita_mint_upload_register_persiste_sha256(
    api_client: TestClient, pg_engine, monkeypatch,
) -> None:
    monkeypatch.setattr(
        "central_api.routes.jobs._object_storage",
        lambda: _FakeStorage(),
    )
    job_id = str(uuid4())
    minio_key = _mint_upload_url(api_client, job_id)

    parquet_bytes = b"\x00\x01\x02\x03"
    sha = hashlib.sha256(parquet_bytes).hexdigest()
    body = _register_extraction(
        api_client, job_id, minio_key, sha, len(parquet_bytes),
    )
    assert body["status"] == "REGISTERED"

    with pg_engine.begin() as conn:
        conn.execute(text("SET LOCAL row_security = off"))
        row = conn.execute(
            text(
                "SELECT status, sha256, agent_version, machine_id "
                "FROM landing.extractions WHERE job_id = :j",
            ),
            {"j": job_id},
        ).one()
    assert row.status == "REGISTERED"
    assert row.sha256 == sha
    assert row.agent_version == "0.5.0"
    assert row.machine_id == "EDGE-01"
