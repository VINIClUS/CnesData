"""Consumer — loop streaming com heartbeat e upload via pre-signed URL."""

import asyncio
import logging
import random
import signal
from functools import partial

import httpx
from pydantic import ValidationError

from cnes_domain.models.extraction import ExtractionParams
from cnes_domain.tenant import set_tenant_id

logger = logging.getLogger(__name__)

_POLL_INTERVAL: float = 1.0
_HEARTBEAT_INTERVAL: float = 300.0


async def run_worker(
    api_base_url: str,
    machine_id: str,
    jitter_max: float = 1800.0,
) -> None:
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

    logger.info(
        "worker_started api=%s machine=%s",
        api_base_url, machine_id,
    )

    while running:
        job_data = await _acquire_job(api_base_url, machine_id)
        if job_data is None:
            await asyncio.sleep(_POLL_INTERVAL)
            continue

        hb_task = asyncio.create_task(
            _heartbeat_loop(
                api_base_url,
                str(job_data["job_id"]),
                machine_id,
            )
        )
        try:
            await _execute_job(
                loop, api_base_url, machine_id, job_data,
            )
        finally:
            hb_task.cancel()
            try:
                await hb_task
            except asyncio.CancelledError:
                pass

        await asyncio.sleep(random.uniform(0, 5))
    logger.info("worker_stopped")


async def _acquire_job(
    api_url: str, machine_id: str,
) -> dict | None:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{api_url}/api/v1/jobs/acquire",
            json={"machine_id": machine_id},
        )
    if resp.status_code == 204:
        return None
    resp.raise_for_status()
    return resp.json()


async def _heartbeat_loop(
    api_url: str, job_id: str, machine_id: str,
) -> None:
    while True:
        await asyncio.sleep(_HEARTBEAT_INTERVAL)
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                await client.post(
                    f"{api_url}/api/v1/jobs/{job_id}/heartbeat",
                    json={"machine_id": machine_id},
                )
            logger.debug("heartbeat_sent job_id=%s", job_id)
        except Exception:
            logger.warning("heartbeat_failed job_id=%s", job_id)


async def _execute_job(
    loop: asyncio.AbstractEventLoop,
    api_url: str,
    machine_id: str,
    job_data: dict,
) -> None:
    job_id = str(job_data["job_id"])
    upload_url = job_data["upload_url"]
    object_key = job_data["object_key"]

    try:
        params = ExtractionParams.model_validate(
            job_data.get("extraction_params", {}),
        )
    except ValidationError:
        logger.exception("invalid_params job_id=%s", job_id)
        return

    set_tenant_id(job_data["tenant_id"])

    try:
        from dump_agent.worker.connection import conectar_firebird

        con = await loop.run_in_executor(None, conectar_firebird)
        try:
            from dump_agent.worker.streaming_executor import (
                stream_to_storage,
            )

            await loop.run_in_executor(
                None, stream_to_storage,
                con, params, upload_url,
            )
        finally:
            con.close()

        async with httpx.AsyncClient(timeout=30.0) as client:
            await client.post(
                f"{api_url}/api/v1/jobs/{job_id}/complete-upload",
                json={
                    "machine_id": machine_id,
                    "object_key": object_key,
                },
            )
        logger.info("job_done job_id=%s", job_id)
    except Exception:
        logger.exception("job_error job_id=%s", job_id)
