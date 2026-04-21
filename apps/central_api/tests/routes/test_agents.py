"""Teste da rota GET /api/v1/agents/status."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine

from central_api.repositories.agent_status_repo import AgentStatus


def _make_app():
    with (
        patch("central_api.app.init_telemetry"),
        patch("central_api.deps.install_rls_listener"),
        patch("central_api.deps.instrument_engine"),
        patch("central_api.deps.create_engine"),
        patch("central_api.deps.reap_expired_leases", return_value=0),
    ):
        from central_api.app import create_app
        return create_app()


@pytest.fixture
def app():
    return _make_app()


@pytest.fixture
def mock_engine():
    return MagicMock(spec=Engine)


@pytest.fixture
def client(app, mock_engine):
    from central_api.deps import get_engine
    app.dependency_overrides[get_engine] = lambda: mock_engine
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    app.dependency_overrides.clear()


def _status(tenant_id: str = "354130", **overrides) -> AgentStatus:
    base = {
        "tenant_id": tenant_id,
        "last_seen": None,
        "agent_version": None,
        "machine_id": None,
        "jobs_completed_7d": 0,
        "jobs_failed_7d": 0,
    }
    base.update(overrides)
    return AgentStatus(**base)


class TestAgentsStatusEndpoint:
    def test_retorna_json_com_campos_esperados(self, client):
        with patch(
            "central_api.routes.agents.query_agent_status",
            return_value=_status(
                tenant_id="354130",
                last_seen=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
                agent_version="0.1.0",
                machine_id="agent-01",
                jobs_completed_7d=7,
                jobs_failed_7d=2,
            ),
        ):
            resp = client.get(
                "/api/v1/agents/status",
                params={"tenant_id": "354130"},
                headers={"X-Tenant-Id": "354130"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["tenant_id"] == "354130"
        assert body["agent_version"] == "0.1.0"
        assert body["machine_id"] == "agent-01"
        assert body["jobs_completed_7d"] == 7
        assert body["jobs_failed_7d"] == 2
        assert body["last_seen"].startswith("2026-04-20")

    def test_retorna_zeros_quando_sem_historico(self, client):
        with patch(
            "central_api.routes.agents.query_agent_status",
            return_value=_status(tenant_id="354130"),
        ):
            resp = client.get(
                "/api/v1/agents/status",
                params={"tenant_id": "354130"},
                headers={"X-Tenant-Id": "354130"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["jobs_completed_7d"] == 0
        assert body["jobs_failed_7d"] == 0
        assert body["last_seen"] is None
        assert body["agent_version"] is None
        assert body["machine_id"] is None

    def test_rejeita_tenant_mismatch(self, client):
        with patch(
            "central_api.routes.agents.query_agent_status",
            return_value=_status(tenant_id="354130"),
        ):
            resp = client.get(
                "/api/v1/agents/status",
                params={"tenant_id": "999999"},
                headers={"X-Tenant-Id": "354130"},
            )
        assert resp.status_code == 403
        assert resp.json()["detail"] == "tenant_mismatch"

    def test_valida_formato_tenant_query(self, client):
        resp = client.get(
            "/api/v1/agents/status",
            params={"tenant_id": "abc"},
            headers={"X-Tenant-Id": "abc"},
        )
        assert resp.status_code == 422

    def test_exige_header_x_tenant_id(self, client):
        resp = client.get(
            "/api/v1/agents/status",
            params={"tenant_id": "354130"},
        )
        assert resp.status_code == 422

    def test_passa_tenant_id_para_repo(self, client):
        with patch(
            "central_api.routes.agents.query_agent_status",
            return_value=_status(tenant_id="354130"),
        ) as mock_query:
            client.get(
                "/api/v1/agents/status",
                params={"tenant_id": "354130"},
                headers={"X-Tenant-Id": "354130"},
            )
        assert mock_query.call_count == 1
        assert mock_query.call_args.kwargs == {"tenant_id": "354130"}
