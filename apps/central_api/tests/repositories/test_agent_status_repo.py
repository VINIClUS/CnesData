"""Teste do agent_status_repo — agrega métricas via landing.extractions (v2)."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text

from central_api.repositories.agent_status_repo import (
    AgentStatus,
    query_agent_status,
)

pytestmark = pytest.mark.postgres


_TENANT = "354130"


@pytest.fixture
def _cleanup_extractions(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(text("TRUNCATE landing.extractions CASCADE"))
    yield
    with pg_engine.begin() as conn:
        conn.execute(text("TRUNCATE landing.extractions CASCADE"))


_FILES_JSON = (
    '[{"minio_key":"x.parquet.gz","fato_subtype":"CNES_VINCULO",'
    '"size_bytes":1,"sha256":"a"}]'
)


def _seed(
    pg_engine,
    *,
    tenant_id: str = _TENANT,
    status: str = "REGISTERED",
    registered_at: datetime | None = None,
    created_at: datetime | None = None,
    agent_version: str | None = None,
    machine_id: str | None = None,
) -> None:
    now = datetime.now(UTC)
    with pg_engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO landing.extractions (
                    job_id, tenant_id, source_type, competencia,
                    files, status, created_at, registered_at,
                    agent_version, machine_id
                ) VALUES (
                    :j, :t, 'CNES_LOCAL', '2026-01-01',
                    CAST(:files AS jsonb),
                    :s, :ca, :ra, :av, :mid
                )
            """),
            {
                "j": str(uuid4()),
                "t": tenant_id,
                "files": _FILES_JSON,
                "s": status,
                "ca": created_at or now,
                "ra": registered_at,
                "av": agent_version,
                "mid": machine_id,
            },
        )


@pytest.mark.usefixtures("_cleanup_extractions")
class TestQueryAgentStatus:
    def test_tenant_sem_dados_retorna_zeros_e_none(self, pg_engine):
        status = query_agent_status(pg_engine, tenant_id=_TENANT)
        assert isinstance(status, AgentStatus)
        assert status.tenant_id == _TENANT
        assert status.jobs_completed_7d == 0
        assert status.jobs_failed_7d == 0
        assert status.last_seen is None
        assert status.agent_version is None
        assert status.machine_id is None

    def test_conta_jobs_registered_como_completed(self, pg_engine):
        now = datetime.now(UTC)
        _seed(pg_engine, status="REGISTERED",
              registered_at=now - timedelta(hours=1))
        _seed(pg_engine, status="REGISTERED",
              registered_at=now - timedelta(hours=2))
        status = query_agent_status(pg_engine, tenant_id=_TENANT)
        assert status.jobs_completed_7d == 2
        assert status.jobs_failed_7d == 0

    def test_conta_failed_pre_register_como_failed(self, pg_engine):
        _seed(pg_engine, status="FAILED", registered_at=None)
        _seed(pg_engine, status="DLQ", registered_at=None)
        status = query_agent_status(pg_engine, tenant_id=_TENANT)
        assert status.jobs_completed_7d == 0
        assert status.jobs_failed_7d == 2

    def test_failed_pos_register_conta_como_completed_nao_failed(
        self, pg_engine,
    ):
        now = datetime.now(UTC)
        _seed(pg_engine, status="FAILED",
              registered_at=now - timedelta(hours=1))
        status = query_agent_status(pg_engine, tenant_id=_TENANT)
        assert status.jobs_completed_7d == 1
        assert status.jobs_failed_7d == 0

    def test_pending_nao_conta_em_nenhum_lado(self, pg_engine):
        _seed(pg_engine, status="PENDING", registered_at=None)
        status = query_agent_status(pg_engine, tenant_id=_TENANT)
        assert status.jobs_completed_7d == 0
        assert status.jobs_failed_7d == 0

    def test_janela_7_dias_exclui_rows_antigos(self, pg_engine):
        old = datetime.now(UTC) - timedelta(days=8)
        _seed(pg_engine, status="REGISTERED",
              registered_at=old, created_at=old)
        status = query_agent_status(pg_engine, tenant_id=_TENANT)
        assert status.jobs_completed_7d == 0

    def test_last_seen_eh_max_registered_at(self, pg_engine):
        recent = datetime.now(UTC) - timedelta(minutes=5)
        older = datetime.now(UTC) - timedelta(hours=2)
        _seed(pg_engine, status="REGISTERED", registered_at=older)
        _seed(pg_engine, status="REGISTERED", registered_at=recent)
        status = query_agent_status(pg_engine, tenant_id=_TENANT)
        assert status.last_seen is not None
        assert abs((status.last_seen - recent).total_seconds()) < 1

    def test_agent_version_e_machine_id_sao_max(self, pg_engine):
        now = datetime.now(UTC)
        _seed(pg_engine, status="REGISTERED",
              registered_at=now - timedelta(hours=2),
              agent_version="1.0.0", machine_id="edge-01")
        _seed(pg_engine, status="REGISTERED",
              registered_at=now - timedelta(hours=1),
              agent_version="1.2.3", machine_id="edge-02")
        status = query_agent_status(pg_engine, tenant_id=_TENANT)
        assert status.agent_version == "1.2.3"
        assert status.machine_id == "edge-02"

    def test_isola_outros_tenants(self, pg_engine):
        now = datetime.now(UTC)
        _seed(pg_engine, tenant_id="999999", status="REGISTERED",
              registered_at=now)
        status = query_agent_status(pg_engine, tenant_id=_TENANT)
        assert status.jobs_completed_7d == 0
