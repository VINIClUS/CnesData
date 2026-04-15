"""Processor — download Parquet, transforma, persiste no Gold."""
import gzip
import io
import logging
import tempfile
from pathlib import Path

import httpx
import polars as pl
from sqlalchemy.engine import Engine

from cnes_domain.observability import tracer
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


def _persist_profissionais(
    uow: PostgresUnitOfWork,
    competencia: str,
    df: pl.DataFrame,
) -> None:
    df = transformar(df)
    fonte = extrair_fonte(df)
    prof_rows = mapear_profissionais(df)
    vinculo_rows = mapear_vinculos(competencia, df)
    with uow:
        uow.profissionais.gravar(prof_rows)
        uow.vinculos.snapshot_replace(
            competencia, fonte, vinculo_rows,
        )


def _persist_estabelecimentos(
    uow: PostgresUnitOfWork, df: pl.DataFrame,
) -> None:
    estab_rows = mapear_estabelecimentos(df)
    with uow:
        uow.estabelecimentos.gravar(estab_rows)


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

    with tracer.start_as_current_span(
        "process_job",
        attributes={
            "job.id": str(job.id),
            "job.competencia": job.competencia,
            "job.source_system": job.source_system,
        },
    ):
        breaker = breaker or CircuitBreaker(service_name="minio")
        download_url = storage.get_presigned_download_url(
            MINIO_BUCKET, job.object_key,
        )
        df = _download_parquet(download_url, breaker)
        logger.info(
            "downloaded rows=%d job_id=%s", len(df), job.id,
        )

        uow = PostgresUnitOfWork(engine)
        src = job.source_system

        if src in ("cnes_profissional", "profissionais"):
            df = CnesLocalAdapter(df).listar_profissionais()
            _persist_profissionais(uow, job.competencia, df)
        elif src in ("cnes_estabelecimento", "estabelecimentos"):
            df = CnesLocalAdapter(df).listar_estabelecimentos()
            _persist_estabelecimentos(uow, df)
        elif src == "sihd_producao":
            df = SihdLocalAdapter(df).listar_aihs()
            _persist_profissionais(uow, job.competencia, df)

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
