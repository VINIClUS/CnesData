"""Testes unitários para funções de lease no job_queue."""

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from cnes_infra.storage.job_queue import (
    acquire_for_agent,
    reap_expired_leases,
    renew_heartbeat,
    transition_to_streaming,
)


@dataclass
class _FakeRow:
    id: uuid.UUID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    status: str = "PENDING"
    source_system: str = "cnes_profissional"
    tenant_id: str = "354130"
    payload_id: uuid.UUID = uuid.UUID("11111111-2222-3333-4444-555555555555")
    created_at: datetime = None
    attempt_count: int = 0
    max_retries: int = 3
    machine_id: str | None = None
    lease_expires_at: datetime | None = None
    heartbeat_at: datetime | None = None
    error_history: list | None = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.error_history is None:
            self.error_history = []


_SENTINEL = object()


def _mock_engine(row=_SENTINEL, rows=None):
    engine = MagicMock()
    con = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(
        return_value=con,
    )
    engine.begin.return_value.__exit__ = MagicMock(
        return_value=False,
    )
    if row is not _SENTINEL:
        con.execute.return_value.first.return_value = row
    if rows is not None:
        con.execute.return_value.fetchall.return_value = rows
    return engine


class TestAcquireForAgent:

    def test_retorna_none_se_nenhum_pending(self):
        engine = _mock_engine(row=None)
        result = acquire_for_agent(engine, "machine-1")
        assert result is None

    def test_retorna_job_com_status_acquired(self):
        engine = _mock_engine(row=_FakeRow())
        result = acquire_for_agent(engine, "machine-1")
        assert result is not None
        assert result.status == "ACQUIRED"
        assert result.machine_id == "machine-1"

    def test_filtra_por_source_system(self):
        engine = _mock_engine(row=_FakeRow())
        result = acquire_for_agent(
            engine, "machine-1", source_system="cnes_profissional",
        )
        assert result is not None


class TestRenewHeartbeat:

    def test_retorna_true_quando_renovado(self):
        engine = _mock_engine()
        con = engine.begin.return_value.__enter__.return_value
        con.execute.return_value.rowcount = 1
        assert renew_heartbeat(
            engine, uuid.uuid4(), "machine-1",
        ) is True

    def test_retorna_false_se_machine_mismatch(self):
        engine = _mock_engine()
        con = engine.begin.return_value.__enter__.return_value
        con.execute.return_value.rowcount = 0
        assert renew_heartbeat(
            engine, uuid.uuid4(), "wrong-machine",
        ) is False


class TestTransitionToStreaming:

    def test_transicao_bem_sucedida(self):
        engine = _mock_engine()
        con = engine.begin.return_value.__enter__.return_value
        con.execute.return_value.rowcount = 1
        assert transition_to_streaming(
            engine, uuid.uuid4(), "machine-1",
        ) is True

    def test_transicao_falha_se_nao_acquired(self):
        engine = _mock_engine()
        con = engine.begin.return_value.__enter__.return_value
        con.execute.return_value.rowcount = 0
        assert transition_to_streaming(
            engine, uuid.uuid4(), "machine-1",
        ) is False


class TestReapExpiredLeases:

    def test_sem_leases_expirados_retorna_zero(self):
        engine = _mock_engine(rows=[])
        assert reap_expired_leases(engine) == 0

    def test_lease_expirado_reseta_para_pending(self):
        expired = _FakeRow(
            status="ACQUIRED",
            attempt_count=0,
            max_retries=3,
            lease_expires_at=datetime.now(timezone.utc)
            - timedelta(minutes=1),
        )
        engine = _mock_engine(rows=[expired])
        count = reap_expired_leases(engine)
        assert count == 1

    def test_lease_expirado_apos_max_retries_vai_para_dlq(self):
        expired = _FakeRow(
            status="STREAMING",
            attempt_count=2,
            max_retries=3,
            lease_expires_at=datetime.now(timezone.utc)
            - timedelta(minutes=1),
        )
        engine = _mock_engine(rows=[expired])
        count = reap_expired_leases(engine)
        assert count == 1
