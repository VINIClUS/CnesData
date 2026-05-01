"""data_processor poll loop — consume landing.extractions (Gold v2, global worker)."""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from cnes_domain.tenant import set_tenant_id
from cnes_infra.storage import extractions_repo

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from cnes_contracts.landing import ClaimedExtraction

logger = logging.getLogger(__name__)


async def pull_next(
    engine: Engine, *, lease_seconds: int = 300,
) -> ClaimedExtraction | None:
    """Claim next PENDING extraction across all tenants."""
    return extractions_repo.claim_next(
        engine, lease_seconds=lease_seconds,
    )


async def process_one(
    engine: Engine,
    claimed: ClaimedExtraction,
    processor_id: str,
) -> None:
    """Mark a claimed extraction COMPLETED (no-op marker; ingestion is separate scope)."""
    set_tenant_id(claimed.tenant_id)
    try:
        extractions_repo.mark_completed(engine, job_id=claimed.job_id)
        logger.info(
            "process_one ingested job_id=%s tenant_id=%s processor_id=%s",
            claimed.job_id, claimed.tenant_id, processor_id,
        )
    except Exception as exc:
        logger.exception(
            "process_one_failed job_id=%s tenant_id=%s",
            claimed.job_id, claimed.tenant_id,
        )
        extractions_repo.mark_failed(
            engine, job_id=claimed.job_id, reason=str(exc),
        )


async def loop(
    engine: Engine,
    *,
    processor_id: str,
    poll_interval_s: float = 5.0,
) -> None:
    """Main processor loop — runs until cancelled."""
    logger.info("poll_loop start processor_id=%s", processor_id)
    while True:
        try:
            ext = await pull_next(engine)
            if ext is None:
                await asyncio.sleep(poll_interval_s)
                continue
            await process_one(engine, ext, processor_id)
        except asyncio.CancelledError:
            logger.info("poll_loop cancelled")
            raise
        except Exception as exc:
            logger.exception("poll_loop_iter_failed err=%s", exc)
            await asyncio.sleep(poll_interval_s)
