"""Teste do agent_status_repo."""

import pytest

from central_api.repositories.agent_status_repo import (
    AgentStatus,
    query_agent_status,
)

pytestmark = pytest.mark.postgres


def test_query_agent_status_vazio_retorna_zeros(pg_engine) -> None:
    status = query_agent_status(pg_engine, tenant_id="999999")
    assert isinstance(status, AgentStatus)
    assert status.tenant_id == "999999"
    assert status.jobs_completed_7d == 0
    assert status.jobs_failed_7d == 0
    assert status.last_seen is None
    assert status.agent_version is None


def test_query_agent_status_agrega_completed_e_failed(pg_engine) -> None:
    status = query_agent_status(pg_engine, tenant_id="354130")
    assert isinstance(status, AgentStatus)
    assert status.tenant_id == "354130"
    assert status.jobs_completed_7d >= 0
    assert status.jobs_failed_7d >= 0
