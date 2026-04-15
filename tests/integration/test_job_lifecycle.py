"""E2E — ciclo de vida: enqueue → acquire → streaming → complete-upload."""
import gzip
import io
import json
import urllib.request
import uuid

import polars as pl
import pytest
from sqlalchemy import text

pytestmark = pytest.mark.e2e

TENANT_ID = "355030"
SOURCE = "cnes_profissional"


def _enqueue_job(pg_engine) -> uuid.UUID:
    payload_id = uuid.uuid4()
    job_id = uuid.uuid4()
    with pg_engine.begin() as con:
        con.execute(
            text(
                "INSERT INTO landing.raw_payload "
                "(id, tenant_id, source_system, competencia, payload) "
                "VALUES (:pid, :tid, :src, '2026-04', '{}'::jsonb)"
            ),
            {"pid": str(payload_id), "tid": TENANT_ID, "src": SOURCE},
        )
        con.execute(
            text(
                "INSERT INTO queue.jobs "
                "(id, status, source_system, tenant_id, payload_id) "
                "VALUES (:id, 'PENDING', :src, :tid, :pid)"
            ),
            {
                "id": str(job_id),
                "src": SOURCE,
                "tid": TENANT_ID,
                "pid": str(payload_id),
            },
        )
    return job_id


def _make_parquet_gz() -> bytes:
    df = pl.DataFrame({
        "cpf": ["12345678901"],
        "nome": ["PROFISSIONAL TESTE"],
        "cnes": ["1234567"],
    })
    buf = io.BytesIO()
    df.write_parquet(buf)
    return gzip.compress(buf.getvalue())


def _api_post(api_url, path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{api_url}/api/v1{path}",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    return urllib.request.urlopen(req)


def test_acquire_retorna_204_sem_jobs(api_url):
    resp = _api_post(
        api_url,
        "/jobs/acquire",
        {"machine_id": "test-agent", "source_system": "inexistent_system"},
    )
    assert resp.status == 204


def test_ciclo_acquire_streaming_complete(api_url, pg_engine):
    _enqueue_job(pg_engine)

    resp = _api_post(
        api_url,
        "/jobs/acquire",
        {"machine_id": "e2e-agent", "source_system": SOURCE},
    )
    assert resp.status == 200
    acquire_data = json.loads(resp.read())
    job_id = acquire_data["job_id"]
    assert acquire_data["upload_url"].startswith("http")
    obj_key = acquire_data["object_key"]

    resp = _api_post(
        api_url,
        f"/jobs/{job_id}/streaming",
        {"machine_id": "e2e-agent"},
    )
    assert resp.status == 200

    resp = _api_post(
        api_url,
        f"/jobs/{job_id}/heartbeat",
        {"machine_id": "e2e-agent"},
    )
    assert resp.status == 200
    hb_data = json.loads(resp.read())
    assert hb_data["renewed"] is True

    resp = _api_post(
        api_url,
        f"/jobs/{job_id}/complete-upload",
        {"machine_id": "e2e-agent", "object_key": obj_key},
    )
    assert resp.status == 200

    resp = urllib.request.urlopen(
        f"{api_url}/api/v1/jobs/{job_id}",
    )
    status_data = json.loads(resp.read())
    assert status_data["status"] == "COMPLETED"


def test_heartbeat_rejeita_machine_id_invalido(api_url, pg_engine):
    job_id = _enqueue_job(pg_engine)

    _api_post(
        api_url,
        "/jobs/acquire",
        {"machine_id": "agent-a", "source_system": SOURCE},
    )

    with pytest.raises(urllib.error.HTTPError, match="409"):
        _api_post(
            api_url,
            f"/jobs/{job_id}/heartbeat",
            {"machine_id": "agent-b-intruder"},
        )


def test_reap_leases_retorna_contagem(api_url):
    req = urllib.request.Request(
        f"{api_url}/api/v1/admin/reap-leases",
        method="POST",
        data=b"",
    )
    resp = urllib.request.urlopen(req)
    body = json.loads(resp.read())
    assert "reaped" in body
    assert isinstance(body["reaped"], int)
