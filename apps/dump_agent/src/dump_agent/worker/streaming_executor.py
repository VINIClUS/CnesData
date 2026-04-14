"""Streaming executor — cursor Firebird → gzip/parquet → PUT upload."""

import gzip
import io
import logging

import httpx
import polars as pl

logger = logging.getLogger(__name__)


def stream_to_storage(
    con: object,
    sql: str,
    upload_url: str,
    batch_size: int = 5000,
) -> int:
    """Lê cursor em lotes, comprime e envia via PUT."""
    from cnes_infra.ingestion.cnes_client import iterar_query_em_lotes

    frames: list[pl.DataFrame] = []
    total_rows = 0

    for batch_df in iterar_query_em_lotes(con, sql, batch_size):
        frames.append(batch_df)
        total_rows += len(batch_df)

    if not frames:
        logger.warning("stream_empty sql=%s", sql[:80])
        return 0

    combined = pl.concat(frames)
    payload = _compress_parquet(combined)
    _upload_payload(upload_url, payload)

    logger.info(
        "stream_completed rows=%d bytes=%d",
        total_rows, len(payload),
    )
    return total_rows


def _compress_parquet(df: pl.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.write_parquet(buf)
    parquet_bytes = buf.getvalue()
    return gzip.compress(parquet_bytes)


def _upload_payload(url: str, data: bytes) -> None:
    if url.startswith(("null://", "placeholder://")):
        logger.warning("upload_skipped null_storage url=%s", url[:60])
        return
    response = httpx.put(
        url,
        content=data,
        headers={"Content-Type": "application/octet-stream"},
        timeout=300.0,
    )
    response.raise_for_status()
