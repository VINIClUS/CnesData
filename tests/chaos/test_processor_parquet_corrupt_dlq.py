"""Corrupt Parquet → DLQ, never panic."""
from __future__ import annotations

import pytest


@pytest.mark.chaos
def test_parquet_corrupto_vai_para_dlq(tmp_path):
    try:
        from data_processor.ingest import ingest_parquet_or_dlq
    except ImportError:
        pytest.skip("ingest_parquet_or_dlq not yet implemented")

    fake_parquet = tmp_path / "corrupt.parquet"
    fake_parquet.write_bytes(b"not a real parquet" * 100)

    result = ingest_parquet_or_dlq(path=fake_parquet, job_id="j-1")
    assert result.status == "DLQ"
    assert any(w in result.error_detail.lower() for w in ["invalid", "corrupt", "parse"])
