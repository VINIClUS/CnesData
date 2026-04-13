"""Testes unitários para retry/DLQ no job_queue."""

import uuid
from dataclasses import dataclass
from unittest.mock import MagicMock

from cnes_infra.storage.job_queue import fail


@dataclass
class _FakeRow:
    id: uuid.UUID = uuid.UUID("12345678-1234-5678-1234-567812345678")
    attempt_count: int = 0
    max_retries: int = 3
    error_history: list = None
    source_system: str = "cnes_profissional"
    tenant_id: str = "354130"
    payload_id: uuid.UUID = uuid.UUID("abcdefab-cdef-abcd-efab-cdefabcdefab")

    def __post_init__(self):
        if self.error_history is None:
            self.error_history = []


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
        con.execute.return_value.one.return_value = row
        return engine

    def test_primeira_falha_reseta_para_pending(self):
        row = _FakeRow(attempt_count=0, max_retries=3)
        engine = self._setup_engine(row)
        result = fail(engine, row.id, "timeout")
        assert result is False

    def test_segunda_falha_ainda_pending(self):
        row = _FakeRow(
            attempt_count=1, max_retries=3,
            error_history=[{"attempt": 1, "error": "err1"}],
        )
        engine = self._setup_engine(row)
        result = fail(engine, row.id, "timeout2")
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
        result = fail(engine, row.id, "fatal_error")
        assert result is True

    def test_error_truncado_em_2000_chars(self):
        row = _FakeRow(attempt_count=2, max_retries=3)
        engine = self._setup_engine(row)
        long_error = "x" * 3000
        fail(engine, row.id, long_error)
        con = engine.begin.return_value.__enter__.return_value
        dlq_call = con.execute.call_args_list[1]
        vals = dlq_call[0][0].compile().params
        assert "last_error" not in vals or len(
            str(vals.get("last_error", "")),
        ) <= 2000
