"""Smoke tests for poll loop (Gold v2, global multi-tenant worker)."""
from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from cnes_contracts.landing import ClaimedExtraction


def _make_engine() -> MagicMock:
    return MagicMock()


def _make_claimed(job_id=None, tenant_id="354130") -> ClaimedExtraction:
    return ClaimedExtraction(
        job_id=job_id or uuid4(),
        tenant_id=tenant_id,
        source_type="BPA_MAG",
        competencia=date(2026, 1, 1),
        files=[{"minio_key": "x.parquet.gz"}],
        depends_on=[],
    )


@pytest.mark.asyncio
async def test_pull_next_retorna_extraction():
    engine = _make_engine()
    claimed = _make_claimed()
    with patch(
        "cnes_infra.storage.extractions_repo.claim_next",
        return_value=claimed,
    ) as mock_claim:
        from data_processor.poll import pull_next
        ext = await pull_next(engine)
        assert ext is claimed
        mock_claim.assert_called_once()
        kwargs = mock_claim.call_args.kwargs
        assert "tenant_id" not in kwargs


@pytest.mark.asyncio
async def test_pull_next_retorna_none_sem_trabalho():
    engine = _make_engine()
    with patch(
        "cnes_infra.storage.extractions_repo.claim_next",
        return_value=None,
    ):
        from data_processor.poll import pull_next
        ext = await pull_next(engine)
        assert ext is None


@pytest.mark.asyncio
async def test_process_one_chama_set_tenant_id_e_mark_completed():
    engine = _make_engine()
    claimed = _make_claimed(tenant_id="354130")
    with (
        patch("data_processor.poll.set_tenant_id") as mock_set,
        patch(
            "cnes_infra.storage.extractions_repo.mark_completed",
        ) as mock_complete,
    ):
        from data_processor.poll import process_one
        await process_one(engine, claimed, "p1")
        mock_set.assert_called_once_with("354130")
        mock_complete.assert_called_once()
        kwargs = mock_complete.call_args.kwargs
        assert kwargs["job_id"] == claimed.job_id


@pytest.mark.asyncio
async def test_process_one_chama_mark_failed_em_excecao():
    engine = _make_engine()
    claimed = _make_claimed()
    with (
        patch("data_processor.poll.set_tenant_id"),
        patch(
            "cnes_infra.storage.extractions_repo.mark_completed",
            side_effect=RuntimeError("boom"),
        ),
        patch(
            "cnes_infra.storage.extractions_repo.mark_failed",
        ) as mock_fail,
    ):
        from data_processor.poll import process_one
        await process_one(engine, claimed, "p1")
        mock_fail.assert_called_once()
        kwargs = mock_fail.call_args.kwargs
        assert kwargs["job_id"] == claimed.job_id
        assert "boom" in kwargs["reason"]


@pytest.mark.asyncio
async def test_loop_cancela_graciosamente():
    engine = _make_engine()
    with patch(
        "cnes_infra.storage.extractions_repo.claim_next",
        return_value=None,
    ):
        from data_processor.poll import loop
        task = asyncio.create_task(
            loop(engine, processor_id="p1", poll_interval_s=0.01),
        )
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_loop_processa_extraction_e_continua():
    engine = _make_engine()
    claimed = _make_claimed()
    call_count = {"n": 0}

    def _claim_side_effect(*_a, **_kw):
        call_count["n"] += 1
        return claimed if call_count["n"] == 1 else None

    with (
        patch(
            "cnes_infra.storage.extractions_repo.claim_next",
            side_effect=_claim_side_effect,
        ),
        patch("data_processor.poll.set_tenant_id") as mock_set,
        patch(
            "cnes_infra.storage.extractions_repo.mark_completed",
        ) as mock_complete,
    ):
        from data_processor.poll import loop
        task = asyncio.create_task(
            loop(engine, processor_id="p1", poll_interval_s=0.01),
        )
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        mock_set.assert_called_once_with(claimed.tenant_id)
        mock_complete.assert_called_once()
        kwargs = mock_complete.call_args.kwargs
        assert kwargs["job_id"] == claimed.job_id


@pytest.mark.asyncio
async def test_loop_loga_e_continua_apos_erro_inesperado():
    engine = _make_engine()
    with patch(
        "cnes_infra.storage.extractions_repo.claim_next",
        side_effect=RuntimeError("transient db error"),
    ):
        from data_processor.poll import loop
        task = asyncio.create_task(
            loop(engine, processor_id="p1", poll_interval_s=0.01),
        )
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
