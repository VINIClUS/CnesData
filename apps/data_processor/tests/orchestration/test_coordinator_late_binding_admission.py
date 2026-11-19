"""Provas de identidade e liquidação da admissão no late bind."""
from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from apps.data_processor.tests.orchestration.test_coordinator import (
    _RUN_ID,
    _TENANT,
    _dependencies,
    _execution,
    _stored_dispatch,
)
from apps.data_processor.tests.orchestration.test_coordinator_environment_gate import (
    _ENVIRONMENT,
    _LEASE_SECONDS,
    _permit,
)
from cnes_domain.control_plane.commands import BindRunDispatch
from cnes_domain.control_plane.enums import DispatchState, RunState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
from cnes_domain.ports.processing import CancelRunExecution
from data_processor.orchestration.coordinator import PipelineCoordinator

if TYPE_CHECKING:
    from apps.data_processor.tests.orchestration.test_coordinator_late_binding import _Race

pytest_plugins = (
    "apps.data_processor.tests.orchestration.test_coordinator_environment_gate",
    "apps.data_processor.tests.orchestration.test_coordinator_late_binding",
)


def test_outro_conflito_do_bind_nao_e_suprimido_apos_publicar(race: _Race) -> None:
    race.prepare_last_wave()

    def bound_elsewhere(request) -> None:
        race.adapter.bind_run_dispatch(BindRunDispatch(
            tenant_id=_TENANT, run_id=_RUN_ID, dispatch_id=request.dispatch_id,
            execution_ref="exec-concorrente", now=race.clock.now(), lease_seconds=300,
        ))
        for unit_id in request.unit_ids:
            race.worker().execute(race.claim(request, unit_id))
        assert race.run().state is RunState.PUBLISHED

    race.inside_start(bound_elsewhere)

    with pytest.raises(Conflict, match="dispatch_bind_conflict"):
        race.coordinator().resume(_TENANT, _RUN_ID)

    assert race.run().state is RunState.PUBLISHED
    assert _stored_dispatch(race.adapter).terminal_outcome is None
    assert _stored_dispatch(race.adapter).execution_ref == "exec-concorrente"
    assert race.executor.canceled == [
        CancelRunExecution(
            tenant_id=_TENANT, run_id=_RUN_ID,
            execution_ref=f"exec-{race.executor.started[-1].dispatch_id}",
        ),
    ]


def test_bind_conflict_claim_generation_divergente_preserva_row_sem_cancelar(
    race: _Race, gate, monkeypatch: pytest.MonkeyPatch,
) -> None:
    race.prepare_last_wave()

    def bound_elsewhere(request) -> None:
        dispatch = _stored_dispatch(race.adapter)
        assert dispatch.start_claim is not None
        race.adapter.bind_run_dispatch(BindRunDispatch(
            tenant_id=_TENANT, run_id=_RUN_ID, dispatch_id=request.dispatch_id,
            execution_ref="exec-concorrente", now=race.clock.now(), lease_seconds=300,
            claim=dispatch.start_claim,
        ))
        for unit_id in request.unit_ids:
            race.worker().execute(race.claim(request, unit_id))
        assert race.run().state is RunState.PUBLISHED
        forged_claim = dispatch.start_claim.model_copy(
            update={"generation": dispatch.start_claim.generation + 1},
        )
        visible = _stored_dispatch(race.adapter).model_copy(
            update={
                "generation": dispatch.generation + 1,
                "execution_ref": f"exec-{request.dispatch_id}",
                "start_claim": forged_claim,
            },
        )
        monkeypatch.setattr(
            race.adapter, "get_run_dispatch", lambda tenant_id, run_id: visible,
        )

    admission = EnvironmentAdmission(gate, race.executor, _ENVIRONMENT, _LEASE_SECONDS)
    coordinator = PipelineCoordinator(
        _dependencies(race.adapter, race.executor, race.store, race.clock),
        replace(_execution(), admission=admission),
    )
    race.inside_start(bound_elsewhere)

    with pytest.raises(Conflict, match="dispatch_bind_conflict"):
        coordinator.resume(_TENANT, _RUN_ID)

    assert _permit(gate) is not None
    assert _stored_dispatch(race.adapter).execution_ref == "exec-concorrente"
    assert _stored_dispatch(race.adapter).state is DispatchState.STARTED
    assert race.executor.canceled == []
