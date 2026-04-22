"""Smoke tests for poll loop (Gold v2)."""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest


def _make_engine_with_conn() -> MagicMock:
    engine = MagicMock()
    conn_cm = MagicMock()
    conn_cm.__enter__ = MagicMock(return_value=MagicMock())
    conn_cm.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = conn_cm
    return engine


@pytest.mark.asyncio
async def test_pull_next_retorna_extraction():
    engine = _make_engine_with_conn()
    fake_ext = MagicMock()
    fake_ext.id = uuid4()

    with patch(
        "cnes_infra.storage.extractions_repo.claim_next",
        return_value=fake_ext,
    ):
        from data_processor.poll import pull_next
        ext = await pull_next(engine, "p1")
        assert ext is fake_ext


@pytest.mark.asyncio
async def test_pull_next_retorna_none_sem_trabalho():
    engine = _make_engine_with_conn()
    with patch(
        "cnes_infra.storage.extractions_repo.claim_next",
        return_value=None,
    ):
        from data_processor.poll import pull_next
        ext = await pull_next(engine, "p1")
        assert ext is None


@pytest.mark.asyncio
async def test_process_one_chama_complete():
    engine = _make_engine_with_conn()
    extraction_id = uuid4()
    with patch(
        "cnes_infra.storage.extractions_repo.complete",
    ) as mock_complete:
        from data_processor.poll import process_one
        await process_one(engine, extraction_id, "p1")
        mock_complete.assert_called_once()


@pytest.mark.asyncio
async def test_process_one_chama_fail_em_excecao():
    engine = _make_engine_with_conn()
    extraction_id = uuid4()
    with (
        patch(
            "cnes_infra.storage.extractions_repo.complete",
            side_effect=RuntimeError("boom"),
        ),
        patch(
            "cnes_infra.storage.extractions_repo.fail",
        ) as mock_fail,
    ):
        from data_processor.poll import process_one
        await process_one(engine, extraction_id, "p1")
        mock_fail.assert_called_once()


@pytest.mark.asyncio
async def test_loop_cancela_graciosamente():
    engine = _make_engine_with_conn()
    with patch(
        "cnes_infra.storage.extractions_repo.claim_next",
        return_value=None,
    ):
        from data_processor.poll import loop
        task = asyncio.create_task(
            loop(engine, "p1", poll_interval_s=0.01),
        )
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_loop_processa_extraction_e_continua():
    engine = _make_engine_with_conn()
    fake_ext = MagicMock()
    fake_ext.id = uuid4()

    call_count = {"n": 0}

    def _claim_side_effect(*_a, **_kw):
        call_count["n"] += 1
        return fake_ext if call_count["n"] == 1 else None

    with (
        patch(
            "cnes_infra.storage.extractions_repo.claim_next",
            side_effect=_claim_side_effect,
        ),
        patch(
            "cnes_infra.storage.extractions_repo.complete",
        ) as mock_complete,
    ):
        from data_processor.poll import loop
        task = asyncio.create_task(
            loop(engine, "p1", poll_interval_s=0.01),
        )
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        mock_complete.assert_called_once()


@pytest.mark.asyncio
async def test_loop_loga_e_continua_apos_erro_inesperado():
    engine = _make_engine_with_conn()
    with patch(
        "cnes_infra.storage.extractions_repo.claim_next",
        side_effect=RuntimeError("transient db error"),
    ):
        from data_processor.poll import loop
        task = asyncio.create_task(
            loop(engine, "p1", poll_interval_s=0.01),
        )
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
