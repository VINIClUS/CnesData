"""Tests for /dashboard/agents/status + /dashboard/agents/runs routes."""
from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from central_api.middleware import AuthenticatedUser
from central_api.repositories.dashboard_repo import RunRow, SourceStatus
from central_api.routes import dashboard


def _build(user, repo) -> TestClient:
    app = FastAPI()
    app.state.dashboard_repo = repo

    @app.middleware("http")
    async def inject(request: Request, call_next):
        if user is not None:
            request.state.user = user
        return await call_next(request)

    app.include_router(dashboard.router, prefix="/api/v1/dashboard")
    return TestClient(app)


def _user(tenants: list[str]) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=uuid4(), email="g@m", display_name=None,
        role="gestor", tenant_ids=tenants,
    )


def test_agents_status_retorna_sources_e_grava_audit() -> None:
    user = _user(["354130"])
    repo = MagicMock()
    repo.agent_status.return_value = [
        SourceStatus(
            fonte_sistema="CNES_LOCAL",
            last_extracao_ts=datetime.now(UTC),
            last_competencia=202604,
            lag_months=0, row_count=100,
            status="ok", last_machine_id=None,
        ),
    ]
    c = _build(user, repo)
    r = c.get(
        "/api/v1/dashboard/agents/status",
        headers={"X-Tenant-Id": "354130"},
    )
    assert r.status_code == 200
    body = r.json()
    assert "fetched_at" in body
    assert body["sources"][0]["fonte_sistema"] == "CNES_LOCAL"
    assert body["sources"][0]["status"] == "ok"
    actions = [c.kwargs["action"] for c in repo.log_action.call_args_list]
    assert "view_status" in actions


def test_agents_status_responde_403_tenant_nao_pertence() -> None:
    user = _user(["354130"])
    c = _build(user, MagicMock())
    r = c.get(
        "/api/v1/dashboard/agents/status",
        headers={"X-Tenant-Id": "999999"},
    )
    assert r.status_code == 403


def test_agents_runs_retorna_lista_e_grava_audit() -> None:
    user = _user(["354130"])
    repo = MagicMock()
    repo.recent_runs.return_value = [
        RunRow(
            id=uuid4(),
            extracao_ts=datetime.now(UTC),
            fonte_sistema="CNES_LOCAL",
            competencia=202604,
            row_count=100,
            sha256="a" * 64,
            machine_id=None,
        ),
    ]
    c = _build(user, repo)
    r = c.get(
        "/api/v1/dashboard/agents/runs",
        headers={"X-Tenant-Id": "354130"},
    )
    assert r.status_code == 200
    body = r.json()
    assert len(body["runs"]) == 1
    assert body["runs"][0]["fonte_sistema"] == "CNES_LOCAL"
    actions = [c.kwargs["action"] for c in repo.log_action.call_args_list]
    assert "view_runs" in actions


def test_agents_runs_rejeita_limit_invalido() -> None:
    user = _user(["354130"])
    c = _build(user, MagicMock())
    r = c.get(
        "/api/v1/dashboard/agents/runs?limit=200",
        headers={"X-Tenant-Id": "354130"},
    )
    assert r.status_code == 400
    assert r.json()["detail"] == "limit_out_of_range"


def test_agents_status_responde_401_sem_user() -> None:
    c = _build(None, MagicMock())
    r = c.get(
        "/api/v1/dashboard/agents/status",
        headers={"X-Tenant-Id": "354130"},
    )
    assert r.status_code == 401
