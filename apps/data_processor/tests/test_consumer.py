"""Testes unitários do consumer do data_processor."""
import asyncio
import uuid
from unittest.mock import DEFAULT, MagicMock, patch

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


def _open_state():
    from cnes_infra.storage.batch_trigger import TriggerState

    return TriggerState(
        status="OPEN", opened_at=None, pending_bytes=0,
        oldest_completed_at=None, reason=None,
    )


def _closed_state():
    from cnes_infra.storage.batch_trigger import TriggerState

    return TriggerState(
        status="CLOSED", opened_at=None,
        pending_bytes=None, oldest_completed_at=None,
        reason=None,
    )


async def _cycle_once(run_coro, delay: float = 0.1) -> None:
    task = asyncio.create_task(run_coro)
    await asyncio.sleep(delay)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def _once_then_none(value):
    count = 0

    def _side_effect(*_args):
        nonlocal count
        count += 1
        return value if count == 1 else None

    return _side_effect


class TestRunProcessor:

    @patch.multiple(
        "data_processor.consumer",
        read_state=DEFAULT, acquire_completed_job=DEFAULT,
        complete_processing=DEFAULT, process_job=DEFAULT,
        close_if_drained=DEFAULT,
    )
    def test_processa_e_completa_job(self, **mocks):
        mocks["read_state"].return_value = _open_state()
        mocks["acquire_completed_job"].side_effect = _once_then_none(_make_job())
        mocks["complete_processing"].return_value = True

        async def _run():
            from data_processor.consumer import run_processor

            await _cycle_once(run_processor(MagicMock(), MagicMock()))

        asyncio.run(_run())
        mocks["process_job"].assert_called_once()

    @patch.multiple(
        "data_processor.consumer",
        read_state=DEFAULT, acquire_completed_job=DEFAULT,
        process_job=DEFAULT, fail_processing=DEFAULT,
        close_if_drained=DEFAULT,
    )
    def test_chama_fail_processing_em_excecao(self, **mocks):
        mocks["read_state"].return_value = _open_state()
        mocks["acquire_completed_job"].side_effect = _once_then_none(_make_job())
        mocks["process_job"].side_effect = RuntimeError("boom")

        async def _run():
            from data_processor.consumer import run_processor

            await _cycle_once(run_processor(MagicMock(), MagicMock()))

        asyncio.run(_run())
        mocks["fail_processing"].assert_called_once()


class TestRunProcessorComFlag:

    @patch("data_processor.consumer.acquire_completed_job")
    @patch("data_processor.consumer.read_state")
    def test_dorme_idle_quando_flag_closed(self, mock_read, mock_acquire):
        mock_read.return_value = _closed_state()

        async def _run():
            from data_processor.consumer import run_processor

            await _cycle_once(run_processor(MagicMock(), MagicMock()), 0.05)

        asyncio.run(_run())
        mock_acquire.assert_not_called()

    @patch.multiple(
        "data_processor.consumer",
        read_state=DEFAULT, acquire_completed_job=DEFAULT,
        close_if_drained=DEFAULT,
    )
    def test_fecha_flag_ao_esvaziar_fila(self, **mocks):
        mocks["read_state"].return_value = _open_state()
        mocks["acquire_completed_job"].return_value = None

        async def _run():
            from data_processor.consumer import run_processor

            await _cycle_once(run_processor(MagicMock(), MagicMock()), 0.05)

        asyncio.run(_run())
        mocks["close_if_drained"].assert_called()
