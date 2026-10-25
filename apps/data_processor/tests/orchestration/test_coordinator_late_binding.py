"""Regressões determinísticas da conclusão síncrona antes do bind do dispatch."""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

import pytest

from apps.data_processor.tests.orchestration.test_coordinator import (
    _RUN_ID,
    _TENANT,
    _complete_dispatch,
    _dependencies,
    _execution,
    _fails_always,
    _full_manifests,
    _local_only_manifests,
    _processor,
    _seed,
    _stored_dispatch,
    adapter,
    clock,
    store,
)
from apps.data_processor.tests.orchestration.test_coordinator_environment_gate import (
    _ENVIRONMENT,
    _LEASE_SECONDS,
    _permit,
    executor,
    gate,
)
from cnes_domain.control_plane.commands import (
    BindRunDispatch,
    ClaimRunUnit,
    FinishRunDispatch,
    ReserveRunDispatch,
)
from cnes_domain.control_plane.enums import (
    DispatchOutcome,
    RunState,
    RunUnitState,
)
from cnes_domain.control_plane.errors import Conflict, ControlPlaneErrorCode
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
from cnes_domain.ports.processing import ExecutionStatus
from data_processor.orchestration.coordinator import PipelineCoordinator
from data_processor.orchestration.unit_worker import (
    UnitWorker,
    UnitWorkerDependencies,
    UnitWorkerPolicy,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from apps.data_processor.tests.orchestration.test_coordinator import (
        _FakeObjectStore,
        _MutableClock,
    )
    from apps.data_processor.tests.orchestration.test_coordinator_environment_gate import (
        _StepFunctions,
    )
    from cnes_domain.control_plane.entities import Run
    from cnes_domain.ports.processing import StartRunExecution
    from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
    from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane

__all__ = ["adapter", "clock", "executor", "gate", "store"]
_OPTIONAL = "CNES_NACIONAL/CNES_VINCULO"


@dataclass
class _Race:
    adapter: SQLiteControlPlane
    executor: _StepFunctions
    store: _FakeObjectStore
    clock: _MutableClock

    def coordinator(self) -> PipelineCoordinator:
        return PipelineCoordinator(
            _dependencies(self.adapter, self.executor, self.store, self.clock), _execution(),
        )

    def prepare_last_wave(self, missing: tuple[str, ...] = ()) -> None:
        manifests = _local_only_manifests() if missing else _full_manifests()
        _seed(self.adapter, manifests=manifests, missing_sources=missing)
        for _ in range(2):
            self.coordinator().resume(_TENANT, _RUN_ID)
            _complete_dispatch(self.adapter, self.executor, self.store, self.clock)

    def worker(self, *, fail: bool = False, notify: bool = True) -> UnitWorker:
        after_persist = (
            lambda unit: self.coordinator().resume(unit.tenant_id, unit.run_id, settle=False)
        ) if notify else lambda unit: None
        return UnitWorker(UnitWorkerDependencies(
            control_plane=self.adapter, store=self.store,
            processor=_fails_always if fail else _processor, clock=self.clock.now,
        ), UnitWorkerPolicy(max_attempts=1, after_persist=after_persist))

    def claim(self, request: StartRunExecution, unit_id: str) -> ClaimRunUnit:
        return ClaimRunUnit(
            tenant_id=_TENANT, run_id=_RUN_ID, unit_id=unit_id,
            dispatch_id=request.dispatch_id, owner="synchronous-worker",
            now=self.clock.now(), lease_seconds=300,
        )

    def inside_start(self, action: Callable[[StartRunExecution], None]) -> None:
        start = self.executor.start
        seen: set[str] = set()

        def synchronous_start(request: StartRunExecution) -> str:
            execution_ref = start(request)
            if request.dispatch_id not in seen:
                seen.add(request.dispatch_id)
                action(request)
            return execution_ref

        self.executor.start = synchronous_start

    def run(self) -> Run:
        return cast("Run", self.adapter.get_run(_TENANT, _RUN_ID))


@pytest.fixture
def race(adapter, executor, store, clock) -> _Race:
    return _Race(adapter, executor, store, clock)


@pytest.mark.parametrize(("missing", "terminal"), [
    ((), RunState.PUBLISHED),
    ((_OPTIONAL,), RunState.PUBLISHED_DEGRADED),
])
def test_publicacao_dentro_do_start_preserva_resultado_sem_compensar(
    race: _Race, missing: tuple[str, ...], terminal: RunState,
) -> None:
    race.prepare_last_wave(missing)

    def complete(request: StartRunExecution) -> None:
        for unit_id in request.unit_ids:
            race.worker().execute(race.claim(request, unit_id))
        assert race.run().state is terminal

    race.inside_start(complete)

    result = race.coordinator().resume(_TENANT, _RUN_ID)

    request = race.executor.started[-1]
    assert result.execution_ref == f"exec-{request.dispatch_id}"
    assert race.run().state is terminal
    assert race.run().missing_sources == missing
    pointer = race.adapter.get_dataset_pointer(_TENANT, "cnes")
    assert pointer is not None
    assert pointer.version_id == _RUN_ID
    units = race.adapter.list_run_units(_TENANT, _RUN_ID)
    assert all(unit.state is RunUnitState.SUCCEEDED for unit in units if unit.dispatch_id)
    assert race.executor.canceled == []
    assert _stored_dispatch(race.adapter).terminal_outcome is not DispatchOutcome.CANCELED


def test_falha_final_dentro_do_start_preserva_run_falho_sem_compensar(race: _Race) -> None:
    race.prepare_last_wave()

    def fail(request: StartRunExecution) -> None:
        for unit_id in request.unit_ids:
            race.worker(fail=True).execute(race.claim(request, unit_id))
        assert race.run().state is RunState.FAILED

    race.inside_start(fail)

    result = race.coordinator().resume(_TENANT, _RUN_ID)

    request = race.executor.started[-1]
    assert result.execution_ref == f"exec-{request.dispatch_id}"
    assert race.run().state is RunState.FAILED
    assert race.adapter.get_dataset_pointer(_TENANT, "cnes") is None
    units = race.adapter.list_run_units(_TENANT, _RUN_ID)
    finished = [unit for unit in units if unit.unit_id in request.unit_ids]
    assert len(finished) == 1
    assert finished[0].state is RunUnitState.FAILED_FINAL
    assert finished[0].dispatch_id == request.dispatch_id
    assert race.executor.canceled == []
    assert _stored_dispatch(race.adapter).terminal_outcome is not DispatchOutcome.CANCELED


def test_onda_completa_falha_aceita_bind_tardio_com_ondas_futuras_pendentes(
    race: _Race,
) -> None:
    _seed(race.adapter, manifests=_full_manifests())

    def fail_completed_wave(request: StartRunExecution) -> None:
        units = race.adapter.list_run_units(_TENANT, _RUN_ID)
        optional = next(unit for unit in units if unit.source_type == "CNES_NACIONAL")
        required = next(unit for unit in units if unit.source_type == "CNES_LOCAL")
        race.worker(notify=False).execute(race.claim(request, optional.unit_id))
        race.worker(fail=True).execute(race.claim(request, required.unit_id))
        persisted = race.adapter.list_run_units(_TENANT, _RUN_ID)
        wave = {unit.unit_id: unit for unit in persisted if unit.unit_id in request.unit_ids}
        assert set(wave) == set(request.unit_ids)
        assert len(wave) == 2
        assert {unit.dispatch_id for unit in wave.values()} == {request.dispatch_id}
        assert wave[optional.unit_id].state is RunUnitState.SUCCEEDED
        assert wave[required.unit_id].state is RunUnitState.FAILED_FINAL
        future = [unit for unit in persisted if unit.unit_id not in request.unit_ids]
        assert future
        assert all(unit.state is RunUnitState.PENDING for unit in future)
        assert race.run().state is RunState.FAILED

    race.inside_start(fail_completed_wave)

    result = race.coordinator().resume(_TENANT, _RUN_ID)

    request = race.executor.started[-1]
    assert result.execution_ref == f"exec-{request.dispatch_id}"
    assert race.run().state is RunState.FAILED
    assert race.adapter.get_dataset_pointer(_TENANT, "cnes") is None
    assert race.executor.canceled == []
    assert _stored_dispatch(race.adapter).terminal_outcome is not DispatchOutcome.CANCELED


def test_bind_tardio_liquida_admissao_apos_executor_concluido(
    race: _Race, gate: DynamoDBControlPlane,
) -> None:
    race.prepare_last_wave()

    def complete(request: StartRunExecution) -> None:
        assert _permit(gate) is not None
        for unit_id in request.unit_ids:
            race.worker().execute(race.claim(request, unit_id))
        race.executor.set_status(f"exec-{request.dispatch_id}", ExecutionStatus.SUCCEEDED)

    admission = EnvironmentAdmission(gate, race.executor, _ENVIRONMENT, _LEASE_SECONDS)
    coordinator = PipelineCoordinator(
        _dependencies(race.adapter, race.executor, race.store, race.clock),
        replace(_execution(), admission=admission),
    )
    race.inside_start(complete)

    result = coordinator.resume(_TENANT, _RUN_ID)

    assert result.execution_ref is not None
    assert race.run().state is RunState.PUBLISHED
    assert _permit(gate) is None
    assert race.executor.canceled == []


def test_onda_sincrona_incompleta_do_run_ainda_processing_faz_bind_normal(race: _Race) -> None:
    _seed(race.adapter, manifests=_full_manifests())

    def partial(request: StartRunExecution) -> None:
        race.worker().execute(race.claim(request, request.unit_ids[0]))

    race.inside_start(partial)

    result = race.coordinator().resume(_TENANT, _RUN_ID)

    dispatch = race.adapter.get_active_run_dispatch(_TENANT, _RUN_ID)
    assert dispatch is not None
    assert dispatch.execution_ref == result.execution_ref
    assert race.run().state is RunState.PROCESSING
    assert race.executor.canceled == []
    states = {unit.state for unit in race.adapter.list_run_units(_TENANT, _RUN_ID)}
    assert {RunUnitState.SUCCEEDED, RunUnitState.PENDING} <= states


def test_run_falho_com_onda_parcial_nao_e_aceito_como_conclusao(race: _Race) -> None:
    _seed(race.adapter, manifests=_full_manifests())

    def fail_one(request: StartRunExecution) -> None:
        units = race.adapter.list_run_units(_TENANT, _RUN_ID)
        required = next(unit for unit in units if unit.source_type == "CNES_LOCAL")
        race.worker(fail=True).execute(race.claim(request, required.unit_id))
        assert race.run().state is RunState.FAILED

    race.inside_start(fail_one)

    with pytest.raises(Conflict, match="parent_not_processing"):
        race.coordinator().resume(_TENANT, _RUN_ID)

    units = race.adapter.list_run_units(_TENANT, _RUN_ID)
    assert any(unit.state is RunUnitState.PENDING for unit in units)
    assert race.run().state is RunState.FAILED
    assert _stored_dispatch(race.adapter).terminal_outcome is DispatchOutcome.CANCELED
    assert len(race.executor.canceled) == 1


@pytest.mark.parametrize("claim", [False, True], ids=["pendente", "leased"])
def test_run_terminal_com_unidade_incompativel_nao_suprime_conflito(
    race: _Race, claim: bool,
) -> None:
    race.prepare_last_wave()

    def terminate(request: StartRunExecution) -> None:
        if claim:
            assert race.adapter.claim_run_unit(race.claim(request, request.unit_ids[0]))
        race.adapter.put_run(race.run().model_copy(update={"state": RunState.FAILED}))

    race.inside_start(terminate)

    with pytest.raises(Conflict, match="parent_not_processing"):
        race.coordinator().resume(_TENANT, _RUN_ID)

    assert race.run().state is RunState.FAILED
    assert race.adapter.get_dataset_pointer(_TENANT, "cnes") is None
    assert _stored_dispatch(race.adapter).terminal_outcome is DispatchOutcome.CANCELED
    assert len(race.executor.canceled) == 1


def test_conclusao_por_outro_dispatch_nao_e_atribuida_ao_start_antigo(race: _Race) -> None:
    race.prepare_last_wave()
    replacements: list[str] = []

    def replaced(request: StartRunExecution) -> None:
        race.adapter.finish_run_dispatch(FinishRunDispatch(
            tenant_id=_TENANT, run_id=_RUN_ID, dispatch_id=request.dispatch_id,
            outcome=DispatchOutcome.FAILED, finished_at=race.clock.now(),
        ))
        replacement = race.adapter.reserve_run_dispatch(ReserveRunDispatch(
            tenant_id=_TENANT, run_id=_RUN_ID, wave_id=request.wave_id,
            unit_ids=request.unit_ids, now=race.clock.now(), lease_seconds=300,
        ))
        replacements.append(replacement.dispatch_id)
        newer = request.model_copy(update={"dispatch_id": replacement.dispatch_id})
        for unit_id in newer.unit_ids:
            race.worker().execute(race.claim(newer, unit_id))
        assert race.run().state is RunState.PUBLISHED

    race.inside_start(replaced)

    with pytest.raises(Conflict):
        race.coordinator().resume(_TENANT, _RUN_ID)

    old = race.executor.started[-1]
    assert replacements != [old.dispatch_id]
    assert [request.execution_ref for request in race.executor.canceled] == [
        f"exec-{old.dispatch_id}",
    ]
    assert _stored_dispatch(race.adapter).dispatch_id == replacements[0]
    assert _stored_dispatch(race.adapter).terminal_outcome is not DispatchOutcome.CANCELED
    assert race.run().state is RunState.PUBLISHED


def test_outro_conflito_do_bind_nao_e_suprimido_apos_publicar(race: _Race) -> None:
    race.prepare_last_wave()

    def bound_elsewhere(request: StartRunExecution) -> None:
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
    assert _stored_dispatch(race.adapter).terminal_outcome is DispatchOutcome.CANCELED
    assert len(race.executor.canceled) == 1


def test_parent_not_processing_do_callback_started_nao_e_suprimido(race: _Race) -> None:
    _seed(race.adapter, manifests=_full_manifests())

    def complete(request: StartRunExecution) -> None:
        for unit_id in request.unit_ids:
            race.worker(notify=False).execute(race.claim(request, unit_id))

    def rejected_after_start(*args: object) -> None:
        race.adapter.put_run(race.run().model_copy(update={"state": RunState.FAILED}))
        raise Conflict(ControlPlaneErrorCode.PARENT_NOT_PROCESSING)

    race.inside_start(complete)
    coordinator = PipelineCoordinator(
        _dependencies(race.adapter, race.executor, race.store, race.clock),
        _execution(started=rejected_after_start),
    )

    with pytest.raises(Conflict, match="parent_not_processing"):
        coordinator.resume(_TENANT, _RUN_ID)

    assert race.run().state is RunState.FAILED
    assert _stored_dispatch(race.adapter).terminal_outcome is DispatchOutcome.CANCELED
    assert len(race.executor.canceled) == 1
