"""Consumer — loop assíncrono que consome jobs da fila PostgreSQL."""

import asyncio
import logging
import signal
from functools import partial

from sqlalchemy.engine import Engine

from storage.job_queue import claim_next, complete, fail
from worker.executor import execute_job

logger = logging.getLogger(__name__)

_POLL_INTERVAL: float = 1.0


async def run_worker(engine: Engine) -> None:
    """Loop principal do worker assíncrono.

    Args:
        engine: SQLAlchemy Engine para conexão com o banco.
    """
    loop = asyncio.get_running_loop()
    running = True

    def _shutdown(sig: signal.Signals) -> None:
        nonlocal running
        logger.info("worker_shutdown signal=%s", sig.name)
        running = False

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, partial(_shutdown, sig))
        except NotImplementedError:
            pass

    logger.info("worker_started poll_interval=%.1fs", _POLL_INTERVAL)
    while running:
        job = await loop.run_in_executor(None, claim_next, engine)
        if job is None:
            await asyncio.sleep(_POLL_INTERVAL)
            continue
        logger.info("job_claimed job_id=%s source=%s", job.id, job.source_system)
        try:
            await loop.run_in_executor(
                None,
                execute_job,
                engine, job.id, job.payload_id,
                job.source_system, job.tenant_id,
            )
            await loop.run_in_executor(None, complete, engine, job.id)
        except Exception as exc:
            logger.exception("job_error job_id=%s", job.id)
            await loop.run_in_executor(
                None, fail, engine, job.id, str(exc),
            )
    logger.info("worker_stopped")
