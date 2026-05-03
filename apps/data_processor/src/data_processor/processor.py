"""Processor — download-only stub; full ingestion pending SIA/BPA intents.

Current surface: `_download_parquet` utility used by smoke tests. Full
v2 pipeline (extractions_repo → transformer → dim_lookup → vinculo_repo_v2)
is tracked separately as future work.
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
from data_processor.cdc_merger import (
    ApplyIU,
    FatalError,
    has_op_column,
    is_delta_mode_enabled,
    merge_delta,
)

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


def maybe_route_delta(
    df: pl.DataFrame,
    conn: object,
    source: str,
    intent: str,
    apply_iu_fn: ApplyIU | None = None,
) -> dict[str, int] | None:
    """Returns delta counts dict if Parquet uses _op CDC column, else None.

    Caller checks return value:
      - None: fall back to existing legacy snapshot path
      - dict: delta merge already executed (deletes applied; I/U
              applied via apply_iu_fn when supplied, else counted only)
    Raises:
        FatalError: when _op present but DELTA_MODE=false.
    """
    if not has_op_column(df):
        return None
    if not is_delta_mode_enabled():
        raise FatalError("delta_mode_required")
    return merge_delta(df, conn, source, intent, apply_iu_fn)
