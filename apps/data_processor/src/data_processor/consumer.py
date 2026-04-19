"""Consumer — poll loop para jobs COMPLETED respeitando batch_trigger."""
import asyncio
import logging
import signal
from functools import partial

from sqlalchemy.engine import Engine

from cnes_domain.ports.object_storage import ObjectStoragePort
from cnes_domain.tenant import set_tenant_id
from cnes_infra.storage.batch_trigger import (
    close_if_drained,
    read_state,
)
from cnes_infra.storage.job_queue import (
    acquire_completed_job,
    complete_processing,
    fail_processing,
)
from cnes_infra.telemetry import get_tracer
from data_processor.config import (
    IDLE_POLL_INTERVAL,
    POLL_INTERVAL,
    PROCESSOR_ID,
)
from data_processor.processor import process_job

logger = logging.getLogger(__name__)
_tracer = get_tracer("data_processor")


async def run_processor(
    engine: Engine, storage: ObjectStoragePort,
) -> None:
    """Loop principal — respeita flag OPEN/CLOSED."""
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
        "processor_started id=%s poll=%.1fs idle=%.1fs",
        PROCESSOR_ID, POLL_INTERVAL, IDLE_POLL_INTERVAL,
    )

    while running:
        state = await loop.run_in_executor(None, read_state, engine)
        if state is None or state.status != "OPEN":
            await asyncio.sleep(IDLE_POLL_INTERVAL)
            continue

        job = await loop.run_in_executor(
            None, acquire_completed_job, engine, PROCESSOR_ID,
        )
        if job is None:
            await loop.run_in_executor(None, close_if_drained, engine)
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
