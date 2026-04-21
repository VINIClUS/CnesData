"""Testes adicionais para branches não cobertos de job_queue."""
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from unittest.mock import MagicMock

from cnes_infra.storage.job_queue import (
    _iso,
    acquire_completed_job,
    complete_processing,
    complete_upload,
    enqueue,
    get_status,
)

_JOB_ID = uuid.UUID("aaaaaaaa-1111-2222-3333-444444444444")
_PAYLOAD_ID = uuid.UUID("bbbbbbbb-5555-6666-7777-888888888888")


@dataclass
class _FakeRow:
    id: uuid.UUID = _JOB_ID
    status: str = "ACQUIRED"
    source_system: str = "cnes_profissional"
    tenant_id: str = "355030"
    payload_id: uuid.UUID = _PAYLOAD_ID
    machine_id: str = "agent-01"
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    attempt_count: int = 0
    max_retries: int = 3
    error_history: list = field(default_factory=list)
    error_detail: str | None = None
    object_key: str | None = "cnes/2026-01.parquet"
    competencia: str | None = "2026-01"


def _mock_engine_begin(row=None, rowcount: int = 1):
    engine = MagicMock()
    con = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=con)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    con.execute.return_value.first.return_value = row
    con.execute.return_value.rowcount = rowcount
    return engine, con


def _mock_engine_connect(row=None):
    engine = MagicMock()
    con = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=con)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    con.execute.return_value.first.return_value = row
    return engine, con


class TestEnqueue:

    def test_retorna_uuid(self):
        engine, _ = _mock_engine_begin()
        job_id = enqueue(engine, "355030", "cnes_profissional", _PAYLOAD_ID)
        assert isinstance(job_id, uuid.UUID)

    def test_chama_insert(self):
        engine, con = _mock_engine_begin()
        enqueue(engine, "355030", "cnes_profissional", _PAYLOAD_ID)
        assert con.execute.called


class TestCompleteUpload:

    def test_retorna_false_se_job_nao_encontrado(self):
        engine, _ = _mock_engine_begin(row=None)
        result = complete_upload(engine, _JOB_ID, "agent-01", "key.parquet", 1024)
        assert result is False

    def test_retorna_false_se_machine_mismatch(self):
        row = _FakeRow(machine_id="outro-agent")
        engine, _ = _mock_engine_begin(row=row)
        result = complete_upload(engine, _JOB_ID, "agent-01", "key.parquet", 1024)
        assert result is False

    def test_retorna_false_se_status_invalido(self):
        row = _FakeRow(status="DONE", machine_id="agent-01")
        engine, _ = _mock_engine_begin(row=row)
        result = complete_upload(engine, _JOB_ID, "agent-01", "key.parquet", 1024)
        assert result is False

    def test_retorna_true_em_sucesso(self):
        row = _FakeRow(status="ACQUIRED", machine_id="agent-01")
        engine, _ = _mock_engine_begin(row=row)
        result = complete_upload(engine, _JOB_ID, "agent-01", "key.parquet", 1024)
        assert result is True


class TestAcquireCompletedJob:

    def test_retorna_none_se_nenhum_completed(self):
        engine, _ = _mock_engine_begin(row=None)
        result = acquire_completed_job(engine, "processor-01")
        assert result is None

    def test_retorna_job_com_status_processing(self):
        row = _FakeRow(status="COMPLETED")
        engine, _ = _mock_engine_begin(row=row)
        result = acquire_completed_job(engine, "processor-01")
        assert result is not None
        assert result.status == "PROCESSING"
        assert result.machine_id == "processor-01"
        assert result.object_key == row.object_key


class TestCompleteProcessing:

    def test_retorna_true_quando_transicao_ok(self):
        engine, _ = _mock_engine_begin(rowcount=1)
        result = complete_processing(engine, _JOB_ID, "processor-01")
        assert result is True

    def test_retorna_false_quando_rowcount_zero(self):
        engine, _ = _mock_engine_begin(rowcount=0)
        result = complete_processing(engine, _JOB_ID, "processor-01")
        assert result is False


class TestGetStatus:

    def test_retorna_none_se_nao_encontrado(self):
        engine, _ = _mock_engine_connect(row=None)
        result = get_status(engine, _JOB_ID)
        assert result is None

    def test_retorna_dict_com_campos(self):
        row = _FakeRow()
        row.started_at = None
        row.completed_at = None
        engine, _ = _mock_engine_connect(row=row)
        result = get_status(engine, _JOB_ID)
        assert result is not None
        assert result["status"] == "ACQUIRED"
        assert result["source_system"] == "cnes_profissional"

    def test_campos_datetime_convertidos(self):
        row = _FakeRow()
        row.started_at = datetime.now(UTC)
        row.completed_at = None
        engine, _ = _mock_engine_connect(row=row)
        result = get_status(engine, _JOB_ID)
        assert isinstance(result["started_at"], str)
        assert result["completed_at"] is None


class TestIso:

    def test_none_retorna_none(self):
        assert _iso(None) is None

    def test_datetime_retorna_string(self):
        dt = datetime(2026, 1, 15, 10, 30, 0, tzinfo=UTC)
        result = _iso(dt)
        assert "2026-01-15" in result
