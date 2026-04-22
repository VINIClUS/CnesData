"""Testes unitarios de _download_parquet."""
from __future__ import annotations

import gzip
import io
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from cnes_domain.pipeline.circuit_breaker import CircuitBreaker
from data_processor.processor import _download_parquet


def _make_parquet_bytes(compressed: bool = True) -> bytes:
    df = pl.DataFrame({
        "cpf": ["12345678901"],
        "nome": ["TESTE"],
        "cnes": ["1234567"],
    })
    buf = io.BytesIO()
    df.write_parquet(buf)
    raw = buf.getvalue()
    if compressed:
        return gzip.compress(raw)
    return raw


@pytest.fixture
def breaker():
    return CircuitBreaker(service_name="test")


class TestDownloadParquet:

    def test_rejeita_null_url(self, breaker):
        with pytest.raises(ValueError, match="null_storage"):
            _download_parquet("null://bucket/key", breaker)

    @patch("data_processor.processor.httpx")
    def test_descomprime_gzip(self, mock_httpx, breaker):
        data = _make_parquet_bytes(compressed=True)
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.iter_bytes.return_value = [data]
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_httpx.stream.return_value = mock_resp

        df = _download_parquet(
            "http://minio:9000/bucket/test.parquet.gz", breaker,
        )
        assert len(df) == 1
        assert "cpf" in df.columns

    @patch("data_processor.processor.httpx")
    def test_parquet_sem_compressao(self, mock_httpx, breaker):
        data = _make_parquet_bytes(compressed=False)
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.iter_bytes.return_value = [data]
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_httpx.stream.return_value = mock_resp

        df = _download_parquet(
            "http://minio:9000/bucket/test.parquet", breaker,
        )
        assert len(df) == 1
