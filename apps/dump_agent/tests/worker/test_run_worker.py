"""Tests for run_worker stop_event-driven loop."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch


@patch("dump_agent.worker.consumer._acquire_job", new_callable=AsyncMock)
def test_run_worker_sai_quando_stop_event_setado_durante_idle(
    mock_acquire,
):
    mock_acquire.return_value = None

    async def body():
        from dump_agent.worker.consumer import run_worker

        stop_event = asyncio.Event()

        async def trigger_stop():
            await asyncio.sleep(0.05)
            stop_event.set()

        await asyncio.gather(
            run_worker(
                api_base_url="http://x",
                machine_id="m1",
                stop_event=stop_event,
                jitter_max=0.0,
            ),
            trigger_stop(),
        )

    asyncio.run(body())


def test_run_worker_nao_adquire_job_se_stop_ja_setado():
    async def body():
        from dump_agent.worker.consumer import run_worker

        stop_event = asyncio.Event()
        stop_event.set()

        with patch(
            "dump_agent.worker.consumer._acquire_job",
            new_callable=AsyncMock,
        ) as mock_acq:
            await run_worker(
                api_base_url="http://x",
                machine_id="m1",
                stop_event=stop_event,
                jitter_max=0.0,
            )
            mock_acq.assert_not_awaited()

    asyncio.run(body())
