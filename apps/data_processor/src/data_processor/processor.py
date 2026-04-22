"""Processor — stub during Gold v2 migration.

TODO(Gold v2): reimplement with extractions_repo download, transformer,
dim_lookup upserts, and vinculo_repo_v2. Previously wired to
PostgresUnitOfWork + job_queue (v1, deleted).
"""
from __future__ import annotations

import gzip
import io
import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import polars as pl

from cnes_domain.observability import tracer

if TYPE_CHECKING:
    from cnes_domain.pipeline.circuit_breaker import CircuitBreaker

logger = logging.getLogger(__name__)

_DOWNLOAD_CHUNK: int = 64 * 1024


def _download_parquet(url: str, breaker: CircuitBreaker) -> pl.DataFrame:
    if url.startswith("null://"):
        raise ValueError("null_storage url_not_downloadable")

    def _fetch() -> Path:
        with tracer.start_as_current_span(
            "download_parquet", attributes={"url": url},
        ):
            with tempfile.NamedTemporaryFile(
                suffix=".parquet", delete=False,
            ) as fd:
                tmp = Path(fd.name)
            with httpx.stream("GET", url, timeout=30.0) as resp:
                resp.raise_for_status()
                buf = io.BytesIO()
                for chunk in resp.iter_bytes(_DOWNLOAD_CHUNK):
                    buf.write(chunk)
            data = buf.getvalue()
            if data[:2] == b"\x1f\x8b":
                data = gzip.decompress(data)
            tmp.write_bytes(data)
            return tmp

    tmp_path = breaker.call(_fetch)
    try:
        return pl.read_parquet(tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)
