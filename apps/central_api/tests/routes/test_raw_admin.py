from datetime import UTC, datetime

from fastapi import FastAPI
from fastapi.testclient import TestClient

from central_api.routes import raw_admin
from cnes_infra import config
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane


def test_enqueue_admin_exige_token_e_chave_e_reproduz(tmp_path, monkeypatch) -> None:
    control = SQLiteControlPlane(tmp_path / "raw.db", lambda: datetime.now(UTC))
    control.initialize()
    monkeypatch.setattr(config, "ADMIN_TOKEN", "secret")
    app = FastAPI()
    app.include_router(raw_admin.router)
    app.dependency_overrides[raw_admin.get_control_plane] = lambda: control
    api = TestClient(app)
    path = "/api/v1/admin/raw-jobs/enqueue"
    body = {"tenant_id": "354130", "agent_id": "agent-1", "competencia": "2026-09"}

    assert api.post(path, json=body).status_code == 401
    assert api.post(path, json=body, headers={"X-Admin-Token": "secret"}).status_code == 422
    headers = {"X-Admin-Token": "secret", "Idempotency-Key": "request-1"}
    first = api.post(path, json=body, headers=headers)
    replay = api.post(path, json=body, headers=headers)
    conflict = api.post(path, json=body | {"competencia": "2026-08"}, headers=headers)

    assert first.status_code == 201
    assert len(first.json()["job_ids"]) == 10
    assert replay.json() == first.json()
    assert conflict.status_code == 409
