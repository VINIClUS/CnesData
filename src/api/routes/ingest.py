"""Rotas de ingestão — recebe payloads de dump agents."""

import logging

from fastapi import APIRouter, Depends, status
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from api.deps import get_engine
from api.models import IngestPayload, IngestResponse
from storage.job_queue import enqueue
from storage.landing import raw_payload

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ingestão"])


def _ingerir(
    payload: IngestPayload,
    source_system: str,
    engine: Engine,
) -> IngestResponse:
    import uuid

    payload_id = uuid.uuid4()
    with engine.begin() as con:
        con.execute(
            pg_insert(raw_payload).values(
                id=payload_id,
                tenant_id=payload.tenant_id,
                source_system=source_system,
                competencia=payload.competencia,
                payload={"registros": payload.registros},
            )
        )
    job_id = enqueue(engine, payload.tenant_id, source_system, payload_id)
    logger.info(
        "payload_recebido source=%s tenant=%s job_id=%s registros=%d",
        source_system, payload.tenant_id, job_id, len(payload.registros),
    )
    return IngestResponse(
        job_id=job_id,
        mensagem="Dados recebidos para processamento",
    )


@router.post(
    "/ingest/cnes/estabelecimento",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=IngestResponse,
)
def ingest_estabelecimento(
    payload: IngestPayload,
    engine: Engine = Depends(get_engine),
) -> IngestResponse:
    return _ingerir(payload, "cnes_estabelecimento", engine)


@router.post(
    "/ingest/cnes/profissionais",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=IngestResponse,
)
def ingest_profissionais(
    payload: IngestPayload,
    engine: Engine = Depends(get_engine),
) -> IngestResponse:
    return _ingerir(payload, "cnes_profissional", engine)


@router.post(
    "/ingest/sihd/producao",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=IngestResponse,
)
def ingest_sihd_producao(
    payload: IngestPayload,
    engine: Engine = Depends(get_engine),
) -> IngestResponse:
    return _ingerir(payload, "sihd_producao", engine)
