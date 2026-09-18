"""Tests for integrity_check.verify_parquet."""
from __future__ import annotations

import hashlib

import pytest

from data_processor.integrity_check import IntegrityError, verify_parquet


def test_verify_parquet_match_returns_true(tmp_path):
    f = tmp_path / "x.parquet"
    f.write_bytes(b"sample-bytes")
    expected = hashlib.sha256(b"sample-bytes").hexdigest()
    assert verify_parquet(str(f), expected) is True


def test_verify_parquet_mismatch_raises(tmp_path):
    f = tmp_path / "x.parquet"
    f.write_bytes(b"sample-bytes")
    bad = "0" * 64
    with pytest.raises(IntegrityError, match="sha256_mismatch"):
        verify_parquet(str(f), bad)


def test_verify_parquet_null_expected_returns_true(tmp_path):
    f = tmp_path / "x.parquet"
    f.write_bytes(b"sample-bytes")
    assert verify_parquet(str(f), None) is True


def test_verify_parquet_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        verify_parquet(str(tmp_path / "nope.parquet"), None)


def test_verify_parquet_large_file_chunked_read(tmp_path):
    f = tmp_path / "big.parquet"
    payload = b"x" * (2 << 20)  # 2MB
    f.write_bytes(payload)
    expected = hashlib.sha256(payload).hexdigest()
    assert verify_parquet(str(f), expected) is True
