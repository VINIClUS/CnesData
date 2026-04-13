"""Executor — processa um job: raw payload → transform → gold schema."""

import logging

import polars as pl
from cnes_domain.processing.transformer import transformar
from cnes_infra.storage.landing import raw_payload
from cnes_infra.storage.postgres_adapter import PostgresAdapter
from sqlalchemy import select
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_SOURCE_TO_METHOD: dict[str, str] = {
    "cnes_profissional": "profissionais",
    "cnes_estabelecimento": "estabelecimentos",
    "sihd_producao": "profissionais",
}


def execute_job(
    engine: Engine,
    job_id: object,
    payload_id: object,
    source_system: str,
    tenant_id: str,
) -> None:
    """Executa processamento completo de um job.

    Args:
        engine: SQLAlchemy Engine.
        job_id: UUID do job.
        payload_id: UUID do payload no landing.
        source_system: Tipo de dado (cnes_profissional, etc).
        tenant_id: Código IBGE do município (6 dígitos).
    """
    data = _load_payload(engine, payload_id)
    registros = data.get("registros", [])
    if not registros:
        logger.warning("job=%s payload_vazio registros=0", job_id)
        return

    df = pl.DataFrame(registros)
    competencia = _extract_competencia(engine, payload_id)

    adapter = PostgresAdapter(engine)
    method = _SOURCE_TO_METHOD.get(source_system, "profissionais")

    if method == "profissionais":
        df = transformar(df)
        adapter.gravar_profissionais(competencia, df)
    elif method == "estabelecimentos":
        adapter.gravar_estabelecimentos(competencia, df)

    logger.info(
        "job_executado job=%s source=%s rows=%d", job_id, source_system, len(df),
    )


def _load_payload(engine: Engine, payload_id: object) -> dict:
    with engine.connect() as con:
        row = con.execute(
            select(raw_payload.c.payload)
            .where(raw_payload.c.id == payload_id)
        ).first()
    if row is None:
        raise ValueError(f"payload_nao_encontrado id={payload_id}")
    return row.payload


def _extract_competencia(engine: Engine, payload_id: object) -> str:
    with engine.connect() as con:
        row = con.execute(
            select(raw_payload.c.competencia)
            .where(raw_payload.c.id == payload_id)
        ).first()
    if row is None:
        raise ValueError(f"payload_nao_encontrado id={payload_id}")
    return row.competencia
