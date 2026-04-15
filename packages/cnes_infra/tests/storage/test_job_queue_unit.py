"""Testes unitários para retry/DLQ no job_queue."""

import uuid
from dataclasses import dataclass
from unittest.mock import MagicMock

from cnes_infra.storage.job_queue import Job, fail_processing


@dataclass
class _FakeRow:
    id: uuid.UUID = uuid.UUID("12345678-1234-5678-1234-567812345678")
    attempt_count: int = 0
    max_retries: int = 3
    error_history: list = None
    source_system: str = "cnes_profissional"
    tenant_id: str = "354130"
    payload_id: uuid.UUID = uuid.UUID(
        "abcdefab-cdef-abcd-efab-cdefabcdefab"
    )
    machine_id: str = "processor-01"

    def __post_init__(self):
        if self.error_history is None:
            self.error_history = []


_JOB_ID = uuid.UUID("12345678-1234-5678-1234-567812345678")
_PAYLOAD_ID = uuid.UUID("abcdefab-cdef-abcd-efab-cdefabcdefab")


class TestJobDataclass:

    def test_aceita_object_key_e_competencia(self):
        job = Job(
            id=_JOB_ID,
            status="PROCESSING",
            source_system="cnes_profissional",
            tenant_id="354130",
            payload_id=_PAYLOAD_ID,
            object_key="cnes/prof/2025-01.parquet",
            competencia="2025-01",
        )
        assert job.object_key == "cnes/prof/2025-01.parquet"
        assert job.competencia == "2025-01"

    def test_defaults_none(self):
        job = Job(
            id=_JOB_ID,
            status="PENDING",
            source_system="cnes_profissional",
            tenant_id="354130",
            payload_id=_PAYLOAD_ID,
        )
        assert job.object_key is None
        assert job.competencia is None


class TestRetryLogic:

    def _setup_engine(self, row: _FakeRow) -> MagicMock:
        engine = MagicMock()
        con = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(
            return_value=con,
        )
        engine.begin.return_value.__exit__ = MagicMock(
            return_value=False,
        )
        con.execute.return_value.first.return_value = row
        return engine

    def test_primeira_falha_reseta_para_completed(self):
        row = _FakeRow(attempt_count=0, max_retries=3)
        engine = self._setup_engine(row)
        result = fail_processing(
            engine, row.id, "processor-01", "timeout",
        )
        assert result is False

    def test_segunda_falha_ainda_completed(self):
        row = _FakeRow(
            attempt_count=1, max_retries=3,
            error_history=[{"attempt": 1, "error": "err1"}],
        )
        engine = self._setup_engine(row)
        result = fail_processing(
            engine, row.id, "processor-01", "timeout2",
        )
        assert result is False

    def test_terceira_falha_move_para_dlq(self):
        row = _FakeRow(
            attempt_count=2, max_retries=3,
            error_history=[
                {"attempt": 1, "error": "e1"},
                {"attempt": 2, "error": "e2"},
            ],
        )
        engine = self._setup_engine(row)
        result = fail_processing(
            engine, row.id, "processor-01", "fatal_error",
        )
        assert result is True

    def test_retorna_false_se_job_nao_encontrado(self):
        engine = MagicMock()
        con = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(
            return_value=con,
        )
        engine.begin.return_value.__exit__ = MagicMock(
            return_value=False,
        )
        con.execute.return_value.first.return_value = None
        result = fail_processing(
            engine,
            uuid.uuid4(),
            "processor-01",
            "error",
        )
        assert result is False
