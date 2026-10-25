"""Erros posteriores à conclusão comprovada não compensam a execução concluída."""
from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from apps.data_processor.tests.orchestration.test_coordinator_late_binding import (
    _ENVIRONMENT,
    _LEASE_SECONDS,
    _RUN_ID,
    _TENANT,
    _dependencies,
    _execution,
    _permit,
    _Race,
    _stored_dispatch,
    adapter,
    clock,
    executor,
    gate,
    race,
    store,
)
from cnes_domain.control_plane.enums import DispatchOutcome, RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict, ControlPlaneErrorCode
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission, UnitPermitLost
from cnes_domain.ports.processing import ExecutionStatus
from data_processor.orchestration.coordinator import PipelineCoordinator

if TYPE_CHECKING:
    from cnes_domain.ports.environment_gate import ExecutionObservation
    from cnes_domain.ports.processing import StartRunExecution
    from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane

__all__ = ["adapter", "clock", "executor", "gate", "race", "store"]


def _complete_wave(race: _Race, request: StartRunExecution) -> None:
    for unit_id in request.unit_ids:
        race.worker().execute(race.claim(request, unit_id))
    units = race.adapter.list_run_units(_TENANT, _RUN_ID)
    wave = {unit.unit_id: unit for unit in units if unit.unit_id in request.unit_ids}
    assert set(wave) == set(request.unit_ids)
    assert all(unit.dispatch_id == request.dispatch_id for unit in wave.values())
    assert all(unit.state is RunUnitState.SUCCEEDED for unit in wave.values())
    assert race.run().state is RunState.PUBLISHED
    race.executor.set_status(f"exec-{request.dispatch_id}", ExecutionStatus.SUCCEEDED)


def _assert_publication_preserved(race: _Race) -> None:
    assert race.run().state is RunState.PUBLISHED
    pointer = race.adapter.get_dataset_pointer(_TENANT, "cnes")
    assert pointer is not None
    assert pointer.version_id == _RUN_ID
    assert _stored_dispatch(race.adapter).terminal_outcome is not DispatchOutcome.CANCELED
    assert race.executor.canceled == []


def test_erro_started_apos_conclusao_propaga_sem_cancelar_onda(race: _Race) -> None:
    race.prepare_last_wave()
    failure = Conflict(ControlPlaneErrorCode.PARENT_NOT_PROCESSING)

    def reject_started(*args: object) -> None:
        assert race.run().state is RunState.PUBLISHED
        raise failure

    race.inside_start(lambda request: _complete_wave(race, request))
    coordinator = PipelineCoordinator(
        _dependencies(race.adapter, race.executor, race.store, race.clock),
        _execution(started=reject_started),
    )

    with pytest.raises(Conflict, match="parent_not_processing") as captured:
        coordinator.resume(_TENANT, _RUN_ID)

    assert captured.value is failure
    _assert_publication_preserved(race)


def test_reconcile_terminal_antes_do_confirm_nao_cancela_onda_concluida(
    race: _Race, gate: DynamoDBControlPlane,
) -> None:
    race.prepare_last_wave()
    admission = EnvironmentAdmission(gate, race.executor, _ENVIRONMENT, _LEASE_SECONDS)

    def complete_then_reconcile(request: StartRunExecution) -> None:
        assert _permit(gate) is not None
        _complete_wave(race, request)
        admission.reconcile(race.clock.now())
        assert _permit(gate) is None

    race.inside_start(complete_then_reconcile)
    coordinator = PipelineCoordinator(
        _dependencies(race.adapter, race.executor, race.store, race.clock),
        replace(_execution(), admission=admission),
    )

    with pytest.raises(UnitPermitLost, match="reason=unit_permit_lost"):
        coordinator.resume(_TENANT, _RUN_ID)

    assert _permit(gate) is None
    _assert_publication_preserved(race)


def test_erro_probe_no_settle_apos_conclusao_propaga_sem_cancelar_onda(
    race: _Race, gate: DynamoDBControlPlane,
) -> None:
    race.prepare_last_wave()
    admission = EnvironmentAdmission(gate, race.executor, _ENVIRONMENT, _LEASE_SECONDS)
    failure = RuntimeError("probe_unavailable")

    def reject_probe(dispatch_id: str) -> ExecutionObservation:
        assert dispatch_id == race.executor.started[-1].dispatch_id
        raise failure

    def complete_then_lose_probe(request: StartRunExecution) -> None:
        _complete_wave(race, request)
        race.executor.probe = reject_probe

    race.inside_start(complete_then_lose_probe)
    coordinator = PipelineCoordinator(
        _dependencies(race.adapter, race.executor, race.store, race.clock),
        replace(_execution(), admission=admission),
    )

    with pytest.raises(RuntimeError, match="probe_unavailable") as captured:
        coordinator.resume(_TENANT, _RUN_ID)

    assert captured.value is failure
    permit = _permit(gate)
    assert permit is not None
    assert permit.execution_ref == f"exec-{race.executor.started[-1].dispatch_id}"
    _assert_publication_preserved(race)
