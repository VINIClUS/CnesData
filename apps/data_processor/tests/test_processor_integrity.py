"""Tests for processor.verify_and_route_delta integrity wiring."""
from __future__ import annotations

import hashlib
from unittest.mock import MagicMock

import polars as pl
import pytest

from data_processor.integrity_check import IntegrityError
from data_processor.processor import verify_and_route_delta


def _write_delta_parquet(path, op="I"):
    df = pl.DataFrame({"CNES": ["1"], "_op": [op]})
    df.write_parquet(path)
    return path.read_bytes()


def test_verify_and_route_delta_match(tmp_path):
    f = tmp_path / "x.parquet"
    payload = _write_delta_parquet(f)
    expected = hashlib.sha256(payload).hexdigest()
    counts = verify_and_route_delta(
        str(f), expected, MagicMock(), "cnes", "estabelecimentos",
    )
    assert counts["inserts"] == 1


def test_verify_and_route_delta_mismatch_raises(tmp_path):
    f = tmp_path / "x.parquet"
    _write_delta_parquet(f)
    with pytest.raises(IntegrityError):
        verify_and_route_delta(
            str(f), "0" * 64, MagicMock(), "cnes", "estabelecimentos",
        )


def test_verify_and_route_delta_null_skips(tmp_path):
    f = tmp_path / "x.parquet"
    _write_delta_parquet(f)
    counts = verify_and_route_delta(
        str(f), None, MagicMock(), "cnes", "estabelecimentos",
    )
    assert counts["inserts"] == 1


def test_verify_and_route_delta_missing_op_raises(tmp_path):
    f = tmp_path / "x.parquet"
    pl.DataFrame({"CNES": ["1"]}).write_parquet(f)
    with pytest.raises(ValueError, match="missing_op_column"):
        verify_and_route_delta(
            str(f), None, MagicMock(), "cnes", "estabelecimentos",
        )
