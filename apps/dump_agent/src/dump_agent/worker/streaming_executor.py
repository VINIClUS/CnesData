"""Streaming executor — registry + io_guard -> gzip -> PUT upload."""

import gzip
import io
import logging
import os
import shutil
import tempfile
from pathlib import Path

import httpx

from cnes_domain.models.extraction import ExtractionParams
from dump_agent.extractors.registry import REGISTRY
from dump_agent.io_guard import SpoolGuard, pre_flight_check
from dump_agent.platform_runtime import (
    register_temp_dir,
    unregister_temp_dir,
)

logger = logging.getLogger(__name__)

_MIN_FREE_MB = int(os.getenv("DUMP_MIN_FREE_DISK_MB", "500"))
_MAX_SPOOL_MB = int(os.getenv("DUMP_MAX_SPOOL_MB", "200"))
_MAX_SPOOL_BYTES = _MAX_SPOOL_MB * 1024 * 1024


def stream_to_storage(
    con: object,
    params: ExtractionParams,
    upload_url: str,
) -> int:
    extractor = REGISTRY[params.intent]

    with tempfile.TemporaryDirectory(prefix="dump_agent_") as tmp_str:
        tmp_dir = Path(tmp_str)
        register_temp_dir(tmp_dir)
        try:
            pre_flight_check(tmp_dir, _MIN_FREE_MB)

            guard = SpoolGuard(max_bytes=_MAX_SPOOL_BYTES)
            parquet_path = extractor.extract(
                params, con, tmp_dir, guard,
            )

            compressed = _compress_file(parquet_path)
            _upload_payload(upload_url, compressed)

            raw_size = parquet_path.stat().st_size
            compressed_size = len(compressed)
            logger.info(
                "stream_done intent=%s parquet_bytes=%d"
                " compressed_bytes=%d",
                params.intent.value, raw_size, compressed_size,
            )
            return compressed_size
        finally:
            unregister_temp_dir(tmp_dir)


def _compress_file(path: Path) -> bytes:
    buffer = io.BytesIO()
    with path.open("rb") as src, gzip.GzipFile(
        fileobj=buffer, mode="wb",
    ) as dst:
        shutil.copyfileobj(src, dst, length=64 * 1024)
    return buffer.getvalue()


def _upload_payload(url: str, data: bytes) -> None:
    if url.startswith(("null://", "placeholder://")):
        logger.warning("upload_skipped url=%s", url[:60])
        return
    resp = httpx.put(
        url,
        content=data,
        headers={"Content-Type": "application/octet-stream"},
        timeout=300.0,
    )
    resp.raise_for_status()
