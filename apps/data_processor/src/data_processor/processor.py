"""Processor — download Parquet, transforma, persiste no Gold."""
import gzip
import io
import logging
import tempfile
from pathlib import Path

import httpx
import polars as pl
from sqlalchemy.engine import Engine

from cnes_domain.pipeline.circuit_breaker import CircuitBreaker
from cnes_domain.ports.object_storage import ObjectStoragePort
from cnes_domain.processing.row_mapper import (
    extrair_fonte,
    mapear_estabelecimentos,
    mapear_profissionais,
    mapear_vinculos,
)
from cnes_domain.processing.transformer import transformar
from cnes_infra.storage.job_queue import Job
from cnes_infra.storage.repositories import PostgresUnitOfWork
from data_processor.adapters.cnes_local_adapter import CnesLocalAdapter
from data_processor.adapters.sihd_local_adapter import SihdLocalAdapter
from data_processor.config import MINIO_BUCKET

logger = logging.getLogger(__name__)

_DOWNLOAD_CHUNK: int = 64 * 1024


def process_job(
    engine: Engine,
    storage: ObjectStoragePort,
    job: Job,
    breaker: CircuitBreaker | None = None,
) -> None:
    """Processa um job COMPLETED: MinIO → transform → Gold."""
    if not job.object_key:
        raise ValueError(f"object_key_missing job_id={job.id}")
    if not job.competencia:
        raise ValueError(f"competencia_missing job_id={job.id}")

    breaker = breaker or CircuitBreaker(service_name="minio")
    download_url = storage.get_presigned_download_url(
        MINIO_BUCKET, job.object_key,
    )
    df = _download_parquet(download_url, breaker)
    logger.info(
        "downloaded rows=%d job_id=%s", len(df), job.id,
    )

    uow = PostgresUnitOfWork(engine)

    if job.source_system in ("cnes_profissional", "profissionais"):
        df = CnesLocalAdapter(df).listar_profissionais()
        df = transformar(df)
        fonte = extrair_fonte(df)
        prof_rows = mapear_profissionais(df)
        vinculo_rows = mapear_vinculos(job.competencia, df)
        with uow:
            uow.profissionais.gravar(prof_rows)
            uow.vinculos.snapshot_replace(
                job.competencia, fonte, vinculo_rows,
            )
    elif job.source_system in (
        "cnes_estabelecimento", "estabelecimentos",
    ):
        df = CnesLocalAdapter(df).listar_estabelecimentos()
        estab_rows = mapear_estabelecimentos(df)
        with uow:
            uow.estabelecimentos.gravar(estab_rows)
    elif job.source_system == "sihd_producao":
        df = SihdLocalAdapter(df).listar_aihs()
        df = transformar(df)
        fonte = extrair_fonte(df)
        prof_rows = mapear_profissionais(df)
        vinculo_rows = mapear_vinculos(job.competencia, df)
        with uow:
            uow.profissionais.gravar(prof_rows)
            uow.vinculos.snapshot_replace(
                job.competencia, fonte, vinculo_rows,
            )

    logger.info(
        "processed job_id=%s source=%s rows=%d",
        job.id, job.source_system, len(df),
    )


def _download_parquet(
    url: str, breaker: CircuitBreaker,
) -> pl.DataFrame:
    if url.startswith("null://"):
        raise ValueError("null_storage url_not_downloadable")

    def _fetch() -> Path:
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
