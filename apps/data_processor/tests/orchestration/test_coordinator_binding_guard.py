"""Guardas negativas do bind tardio com falhas sintéticas na fronteira SQLite."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from apps.data_processor.tests.orchestration.test_coordinator import (
    _RUN_ID,
    _TENANT,
    _dependencies,
    _execution,
    _full_manifests,
    _seed,
    _stored_dispatch,
)
from apps.data_processor.tests.orchestration.test_coordinator_late_binding import (
    _Race,
    adapter,
    clock,
    executor,
    race,
    store,
)
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState, RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict, ControlPlaneErrorCode
from cnes_domain.ports.processing import CancelRunExecution
from data_processor.orchestration.coordinator import PipelineCoordinator

if TYPE_CHECKING:
    from cnes_domain.control_plane.commands import BindRunDispatch, FinishRunDispatch
    from cnes_domain.control_plane.entities import Run, RunDispatch
    from cnes_domain.ports.processing import StartRunExecution

__all__ = ["adapter", "clock", "executor", "race", "store"]


def _complete_without_fan_in(race: _Race, request: StartRunExecution) -> None:
    for unit_id in request.unit_ids:
        race.worker(notify=False).execute(race.claim(request, unit_id))
    units = race.adapter.list_run_units(_TENANT, _RUN_ID)
    wave = [unit for unit in units if unit.unit_id in request.unit_ids]
    assert {unit.unit_id for unit in wave} == set(request.unit_ids)
    assert all(unit.dispatch_id == request.dispatch_id for unit in wave)
    assert all(unit.state is RunUnitState.SUCCEEDED for unit in wave)
    assert race.run().state is RunState.PROCESSING
    assert race.adapter.get_dataset_pointer(_TENANT, "cnes") is None


def _inject_bind_conflict(
    race: _Race,
    monkeypatch: pytest.MonkeyPatch,
    missing_parent: bool,
) -> tuple[Conflict, list[str]]:
    conflict = Conflict(ControlPlaneErrorCode.PARENT_NOT_PROCESSING)
    sequence: list[str] = []
    read = race.adapter.get_run
    finish = race.adapter.finish_run_dispatch

    def observe_parent(tenant_id: str, run_id: str) -> Run | None:
        monkeypatch.setattr(race.adapter, "get_run", read)
        sequence.append("parent_absent" if missing_parent else "parent_processing")
        assert (tenant_id, run_id) == (_TENANT, _RUN_ID)
        return None if missing_parent else read(tenant_id, run_id)

    def reject_bind(command: BindRunDispatch) -> RunDispatch:
        sequence.append("bind")
        assert command.dispatch_id == race.executor.started[-1].dispatch_id
        monkeypatch.setattr(race.adapter, "get_run", observe_parent)
        raise conflict

    def finish_dispatch(command: FinishRunDispatch) -> RunDispatch:
        sequence.append("finish")
        assert race.adapter.get_run == read
        return finish(command)

    monkeypatch.setattr(race.adapter, "bind_run_dispatch", reject_bind)
    monkeypatch.setattr(race.adapter, "finish_run_dispatch", finish_dispatch)
    return conflict, sequence


@pytest.mark.parametrize("missing_parent", [False, True], ids=["processing", "ausente"])
def test_bind_rejeitado_sem_parent_terminal_preserva_conflito_e_compensa(
    race: _Race,
    monkeypatch: pytest.MonkeyPatch,
    missing_parent: bool,
) -> None:
    _seed(race.adapter, manifests=_full_manifests())
    race.inside_start(lambda request: _complete_without_fan_in(race, request))
    conflict, sequence = _inject_bind_conflict(race, monkeypatch, missing_parent)
    started: list[tuple[object, ...]] = []
    coordinator = PipelineCoordinator(
        _dependencies(race.adapter, race.executor, race.store, race.clock),
        _execution(started=lambda *args: started.append(args)),
    )
    observed: Exception | None = None

    try:
        coordinator.resume(_TENANT, _RUN_ID)
    except Exception as error:
        observed = error

    assert observed is conflict
    assert sequence == [
        "bind",
        "parent_absent" if missing_parent else "parent_processing",
        "finish",
    ]
    assert len(race.executor.started) == 1
    request = race.executor.started[0]
    assert race.executor.canceled == [
        CancelRunExecution(
            tenant_id=_TENANT,
            run_id=_RUN_ID,
            execution_ref=f"exec-{request.dispatch_id}",
        )
    ]
    dispatch = _stored_dispatch(race.adapter)
    assert dispatch.dispatch_id == request.dispatch_id
    assert dispatch.state is DispatchState.TERMINAL
    assert dispatch.terminal_outcome is DispatchOutcome.CANCELED
    assert race.adapter.get_dataset_pointer(_TENANT, "cnes") is None
    assert race.run().state is RunState.PROCESSING
    assert started == []
