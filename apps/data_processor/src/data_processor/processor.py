"""Processor — download Parquet, transforma, persiste no Gold."""
import gzip
import io
import logging

import httpx
import polars as pl
from sqlalchemy import select
from sqlalchemy.engine import Engine

from cnes_domain.ports.object_storage import ObjectStoragePort
from cnes_domain.processing.transformer import transformar
from cnes_infra.storage.job_queue import Job
from cnes_infra.storage.landing import raw_payload
from cnes_infra.storage.postgres_adapter import PostgresAdapter
from data_processor.config import MINIO_BUCKET

logger = logging.getLogger(__name__)


def process_job(
    engine: Engine,
    storage: ObjectStoragePort,
    job: Job,
) -> None:
    """Processa um job COMPLETED: MinIO → transform → Gold."""
    object_key = _get_object_key(engine, job.payload_id)
    if not object_key:
        raise ValueError(
            f"object_key_missing payload_id={job.payload_id}"
        )

    download_url = storage.get_presigned_download_url(
        MINIO_BUCKET, object_key,
    )
    df = _download_parquet(download_url)
    logger.info(
        "downloaded rows=%d job_id=%s", len(df), job.id,
    )

    if job.source_system in ("cnes_profissional", "sihd_producao"):
        df = transformar(df)
        adapter = PostgresAdapter(engine)
        competencia = _get_competencia(engine, job.payload_id)
        adapter.gravar_profissionais(competencia, df)
    elif job.source_system == "cnes_estabelecimento":
        adapter = PostgresAdapter(engine)
        competencia = _get_competencia(engine, job.payload_id)
        adapter.gravar_estabelecimentos(competencia, df)

    logger.info(
        "processed job_id=%s source=%s rows=%d",
        job.id, job.source_system, len(df),
    )


def _get_object_key(
    engine: Engine, payload_id: object,
) -> str | None:
    with engine.connect() as con:
        row = con.execute(
            select(raw_payload.c.object_key)
            .where(raw_payload.c.id == payload_id)
        ).first()
    return row.object_key if row else None


def _get_competencia(
    engine: Engine, payload_id: object,
) -> str:
    with engine.connect() as con:
        row = con.execute(
            select(raw_payload.c.competencia)
            .where(raw_payload.c.id == payload_id)
        ).first()
    if row is None:
        raise ValueError(f"payload_not_found id={payload_id}")
    return row.competencia


def _download_parquet(url: str) -> pl.DataFrame:
    if url.startswith("null://"):
        raise ValueError("null_storage url_not_downloadable")
    resp = httpx.get(url, timeout=120.0)
    resp.raise_for_status()
    raw = resp.content
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return pl.read_parquet(io.BytesIO(raw))
