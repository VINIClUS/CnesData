"""data_processor poll loop — consume landing.extractions (Gold v2)."""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from cnes_infra.storage import extractions_repo

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


async def pull_next(engine: Engine, processor_id: str, lease_secs: int = 300):
    """Claim next UPLOADED extraction or return None."""
    with engine.begin() as conn:
        return extractions_repo.claim_next(
            conn, processor_id=processor_id, lease_secs=lease_secs,
        )


async def process_one(
    engine: Engine, extraction_id: UUID, processor_id: str,
) -> None:
    """Process a claimed extraction (stub for future ingestion logic)."""
    try:
        with engine.begin() as conn:
            extractions_repo.complete(conn, extraction_id)
        logger.info("process_one ingested=%s", extraction_id)
    except Exception as exc:
        logger.exception("process_one_failed id=%s", extraction_id)
        with engine.begin() as conn:
            extractions_repo.fail(conn, extraction_id, str(exc))


async def loop(
    engine: Engine,
    processor_id: str,
    poll_interval_s: float = 5.0,
) -> None:
    """Main processor loop — runs until cancelled."""
    logger.info("poll_loop start processor_id=%s", processor_id)
    while True:
        try:
            ext = await pull_next(engine, processor_id)
            if ext is None:
                await asyncio.sleep(poll_interval_s)
                continue
            await process_one(engine, ext.id, processor_id)
        except asyncio.CancelledError:
            logger.info("poll_loop cancelled")
            raise
        except Exception as exc:
            logger.exception("poll_loop_iter_failed err=%s", exc)
            await asyncio.sleep(poll_interval_s)
