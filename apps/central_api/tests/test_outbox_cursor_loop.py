"""Loop da API executa dispatcher e persistência reais entre ciclos."""

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from central_api import deps
from packages.cnes_infra.tests.control_plane.outbox_cursor_support import OutboxHarness, evento


class _SchedulerFinished(Exception):
    pass


async def test_loop_da_api_supera_poison_com_dispatcher_e_sqlite_reais(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = OutboxHarness("sqlite", tmp_path)
    for index in range(100):
        harness.put(evento(f"poison-{index:03}", index))
    harness.put(evento("good", 101))
    sink = harness.sink()
    runtime: Any = SimpleNamespace(control_plane=harness.adapter, audit_sink=sink)
    original_sleep = asyncio.sleep
    cycles = [0]

    async def scheduler(_: float) -> None:
        cycles[0] += 1
        if cycles[0] > 3:
            raise _SchedulerFinished
        await original_sleep(0)

    def indisponivel(event: Any) -> None:
        if event.event_id.startswith("poison-"):
            raise OSError("sink=temporarily_unavailable")

    sink.before = indisponivel
    monkeypatch.setattr(deps.asyncio, "sleep", scheduler)
    with pytest.raises(_SchedulerFinished):
        await asyncio.wait_for(deps._outbox_dispatch_loop(runtime), timeout=5)
    assert harness.stored("good").delivered_at is not None
    assert [record.event_id for record in harness.local_records()] == ["good"]
