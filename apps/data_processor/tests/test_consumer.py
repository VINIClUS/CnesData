"""Testes unitários do consumer do data_processor."""
import asyncio
import uuid
from unittest.mock import MagicMock, patch

from cnes_infra.storage.job_queue import Job


def _make_job() -> Job:
    return Job(
        id=uuid.uuid4(),
        status="PROCESSING",
        source_system="cnes_profissional",
        tenant_id="355030",
        payload_id=uuid.uuid4(),
        machine_id="processor-01",
    )


class TestRunProcessor:

    @patch("data_processor.consumer.process_job")
    @patch("data_processor.consumer.complete_processing")
    @patch("data_processor.consumer.acquire_completed_job")
    def test_processa_e_completa_job(
        self, mock_acquire, mock_complete, mock_process,
    ):
        job = _make_job()
        call_count = 0

        def _acquire_side_effect(*args):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return job
            return None

        mock_acquire.side_effect = _acquire_side_effect
        mock_complete.return_value = True

        async def _run():
            from data_processor.consumer import run_processor
            engine = MagicMock()
            storage = MagicMock()

            task = asyncio.create_task(
                run_processor(engine, storage)
            )
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(_run())
        mock_process.assert_called_once()

    @patch("data_processor.consumer.fail_processing")
    @patch("data_processor.consumer.process_job")
    @patch("data_processor.consumer.acquire_completed_job")
    def test_chama_fail_processing_em_excecao(
        self, mock_acquire, mock_process, mock_fail,
    ):
        job = _make_job()
        call_count = 0

        def _acquire_side_effect(*args):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return job
            return None

        mock_acquire.side_effect = _acquire_side_effect
        mock_process.side_effect = RuntimeError("boom")

        async def _run():
            from data_processor.consumer import run_processor
            engine = MagicMock()
            storage = MagicMock()

            task = asyncio.create_task(
                run_processor(engine, storage)
            )
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(_run())
        mock_fail.assert_called_once()
