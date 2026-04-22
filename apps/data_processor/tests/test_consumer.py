"""Testes do wrapper run_processor (delegates to poll.loop)."""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_run_processor_delega_para_poll_loop():
    engine = MagicMock()
    storage = MagicMock()

    async def _fake_loop(*_a, **_kw):
        return None

    with patch(
        "data_processor.consumer.poll_loop", side_effect=_fake_loop,
    ) as mock_loop:
        from data_processor.consumer import run_processor
        await run_processor(engine, storage)

    mock_loop.assert_called_once()
    kwargs = mock_loop.call_args.kwargs
    assert "processor_id" in kwargs
    assert "poll_interval_s" in kwargs


@pytest.mark.asyncio
async def test_run_processor_propaga_cancelamento():
    engine = MagicMock()
    storage = MagicMock()

    async def _raise_cancelled(*_a, **_kw):
        raise asyncio.CancelledError

    with patch(
        "data_processor.consumer.poll_loop", side_effect=_raise_cancelled,
    ):
        from data_processor.consumer import run_processor
        with pytest.raises(asyncio.CancelledError):
            await run_processor(engine, storage)
