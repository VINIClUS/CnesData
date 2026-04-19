"""Teste unitário da rota /jobs/{id}/complete-upload."""

import uuid
from unittest.mock import patch

from fastapi.testclient import TestClient

from central_api.app import create_app


def test_route_propaga_size_bytes_ao_storage():
    app = create_app()
    client = TestClient(app)
    job_id = uuid.uuid4()

    with patch(
        "central_api.routes.jobs.complete_upload",
        return_value=True,
    ) as mock_complete:
        resp = client.post(
            f"/api/v1/jobs/{job_id}/complete-upload",
            json={
                "machine_id": "m",
                "object_key": "k",
                "size_bytes": 4096,
            },
            headers={"X-Tenant-Id": "355030"},
        )

    assert resp.status_code == 200
    assert mock_complete.call_count == 1
    call_args = mock_complete.call_args.args
    assert call_args[2] == "m"
    assert call_args[3] == "k"
    assert call_args[4] == 4096


def test_route_retorna_422_sem_size_bytes():
    app = create_app()
    client = TestClient(app)
    job_id = uuid.uuid4()

    resp = client.post(
        f"/api/v1/jobs/{job_id}/complete-upload",
        json={"machine_id": "m", "object_key": "k"},
        headers={"X-Tenant-Id": "355030"},
    )

    assert resp.status_code == 422
