"""Consumer — poll loop para jobs COMPLETED."""
import asyncio
import logging
import signal
from functools import partial

from cnes_domain.ports.object_storage import ObjectStoragePort
from cnes_domain.tenant import set_tenant_id
from cnes_infra.storage.job_queue import (
    acquire_completed_job,
    complete_processing,
    fail_processing,
)
from cnes_infra.telemetry import get_tracer
from sqlalchemy.engine import Engine

from data_processor.config import POLL_INTERVAL, PROCESSOR_ID
from data_processor.processor import process_job

logger = logging.getLogger(__name__)
_tracer = get_tracer("data_processor")


async def run_processor(
    engine: Engine, storage: ObjectStoragePort,
) -> None:
    """Loop principal do data_processor."""
    loop = asyncio.get_running_loop()
    running = True

    def _shutdown(sig: signal.Signals) -> None:
        nonlocal running
        logger.info("processor_shutdown signal=%s", sig.name)
        running = False

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, partial(_shutdown, sig))
        except NotImplementedError:
            pass

    logger.info(
        "processor_started id=%s poll=%.1fs",
        PROCESSOR_ID, POLL_INTERVAL,
    )

    while running:
        job = await loop.run_in_executor(
            None, acquire_completed_job, engine, PROCESSOR_ID,
        )
        if job is None:
            await asyncio.sleep(POLL_INTERVAL)
            continue

        set_tenant_id(job.tenant_id)
        with _tracer.start_as_current_span("process_job") as span:
            span.set_attribute("job.id", str(job.id))
            span.set_attribute("job.source_system", job.source_system)
            try:
                await loop.run_in_executor(
                    None, process_job, engine, storage, job,
                )
                await loop.run_in_executor(
                    None,
                    complete_processing,
                    engine, job.id, PROCESSOR_ID,
                )
            except Exception as exc:
                logger.exception(
                    "processing_error job_id=%s", job.id,
                )
                await loop.run_in_executor(
                    None,
                    fail_processing,
                    engine, job.id, PROCESSOR_ID, str(exc),
                )

    logger.info("processor_stopped")
