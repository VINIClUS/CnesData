"""Consumer — thin wrapper delegating to data_processor.poll (Gold v2)."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from data_processor.config import POLL_INTERVAL, PROCESSOR_ID
from data_processor.poll import loop as poll_loop

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from cnes_domain.ports.object_storage import ObjectStoragePort

logger = logging.getLogger(__name__)


async def run_processor(
    engine: Engine, storage: ObjectStoragePort,
) -> None:
    """Entrypoint retained for backwards compatibility with main.py."""
    logger.info(
        "run_processor start processor_id=%s poll=%.1fs",
        PROCESSOR_ID, POLL_INTERVAL,
    )
    await poll_loop(
        engine, processor_id=PROCESSOR_ID, poll_interval_s=POLL_INTERVAL,
    )
