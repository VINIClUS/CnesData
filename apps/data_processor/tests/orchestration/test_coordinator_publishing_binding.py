"""Regressão da publicação em andamento antes do bind e controles sintéticos de guarda."""
from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event, Thread
from typing import TYPE_CHECKING

import pytest

from apps.data_processor.tests.orchestration.test_coordinator import (
    _RUN_ID,
    _TENANT,
    _dependencies,
    _execution,
    _full_manifests,
    _processor,
    _seed,
    _stored_dispatch,
)
from apps.data_processor.tests.orchestration.test_coordinator_late_binding import (
    _OPTIONAL,
    _Race,
    adapter,
    clock,
    executor,
    race,
    store,
)
from cnes_domain.control_plane.commands import FinishRunDispatch, ReserveRunDispatch
from cnes_domain.control_plane.enums import DispatchOutcome, RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict, ControlPlaneErrorCode
from data_processor.orchestration.coordinator import PipelineCoordinator
from data_processor.orchestration.unit_worker import (
    UnitWorker,
    UnitWorkerDependencies,
    UnitWorkerPolicy,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from cnes_domain.ports.processing import StartRunExecution
    from data_processor.orchestration.coordinator import CoordinatorResult
    from data_processor.orchestration.publisher import PublishRequest, PublishResult

__all__ = ["adapter", "clock", "executor", "race", "store"]
_TIMEOUT = 5


@dataclass
class _PublishingBoundary:
    race: _Race
    entered: Event = field(default_factory=Event)
    released: Event = field(default_factory=Event)
    errors: list[BaseException] = field(default_factory=list)
    thread: Thread | None = None
    request: StartRunExecution | None = None
    coordinator: PipelineCoordinator = field(init=False)
    delegate: Callable[[PublishRequest], PublishResult] = field(init=False)

    def __post_init__(self) -> None:
        current = self.race
        dependencies = _dependencies(
            current.adapter, current.executor, current.store, current.clock,
        )
        self.delegate = dependencies.publisher.publish
        dependencies.publisher.publish = self.publish
        self.coordinator = PipelineCoordinator(dependencies, _execution())

    def publish(self, request: PublishRequest) -> PublishResult:
        self.entered.set()
        if not self.released.wait(timeout=_TIMEOUT):
            raise TimeoutError("publisher_barrier_not_released")
        return self.delegate(request)

    def start_worker(self, request: StartRunExecution) -> None:
        self.request = request
        self.thread = Thread(target=self.execute, args=(request,), name="publishing-before-bind")
        self.thread.start()
        if not self.entered.wait(timeout=_TIMEOUT):
            raise TimeoutError("publisher_barrier_not_entered")

    def execute(self, request: StartRunExecution) -> None:
        current = self.race
        worker = UnitWorker(UnitWorkerDependencies(
            control_plane=current.adapter, store=current.store,
            processor=_processor, clock=current.clock.now,
        ), UnitWorkerPolicy(after_persist=lambda unit: self.coordinator.resume(
            unit.tenant_id, unit.run_id, settle=False,
        )))
        try:
            for unit_id in request.unit_ids:
                worker.execute(current.claim(request, unit_id))
        except BaseException as error:
            self.errors.append(error)

    def close(self) -> None:
        self.released.set()
        if self.thread is not None:
            self.thread.join(timeout=_TIMEOUT)
            assert not self.thread.is_alive(), "publishing_worker_thread_leaked"
        assert self.errors == []


def _resume(
    boundary: _PublishingBoundary, recover: bool,
) -> tuple[CoordinatorResult | None, Exception | None]:
    try:
        if recover:
            return boundary.coordinator.recover(limit=1)[0], None
        return boundary.coordinator.resume(_TENANT, _RUN_ID), None
    except Exception as error:
        return None, error


def _assert_publication_blocked(boundary: _PublishingBoundary) -> None:
    current = boundary.race
    request = boundary.request
    assert request is not None
    assert boundary.entered.is_set()
    assert not boundary.released.is_set()
    assert boundary.thread is not None
    assert boundary.thread.is_alive()
    assert boundary.errors == []
    assert current.run().state is RunState.PUBLISHING
    units = current.adapter.list_run_units(_TENANT, _RUN_ID)
    wave = {unit.unit_id: unit for unit in units if unit.unit_id in request.unit_ids}
    assert wave
    assert set(wave) == set(request.unit_ids)
    assert {unit.dispatch_id for unit in wave.values()} == {request.dispatch_id}
    assert all(unit.state is RunUnitState.SUCCEEDED for unit in wave.values())
    assert current.adapter.get_dataset_pointer(_TENANT, "cnes") is None


@pytest.mark.parametrize(("missing", "terminal", "recover"), [
    pytest.param((), RunState.PUBLISHED, False, id="resume-completo"),
    pytest.param((_OPTIONAL,), RunState.PUBLISHED_DEGRADED, False, id="resume-degradado"),
    pytest.param((), RunState.PUBLISHED, True, id="recover-completo"),
    pytest.param((_OPTIONAL,), RunState.PUBLISHED_DEGRADED, True, id="recover-degradado"),
])
def test_publicacao_em_andamento_antes_do_bind_preserva_execucao(
    race: _Race, missing: tuple[str, ...], terminal: RunState, recover: bool,
) -> None:
    race.prepare_last_wave(missing)
    boundary = _PublishingBoundary(race)
    race.inside_start(boundary.start_worker)

    try:
        result, error = _resume(boundary, recover)
        _assert_publication_blocked(boundary)
        before_release = _stored_dispatch(race.adapter)
        canceled_before_release = tuple(race.executor.canceled)
    finally:
        boundary.close()

    assert race.run().state is terminal
    assert race.run().missing_sources == missing
    pointer = race.adapter.get_dataset_pointer(_TENANT, "cnes")
    assert pointer is not None
    assert pointer.version_id == _RUN_ID
    assert error is None, (
        f"bind_rejected_while_publishing error={error!r} "
        f"dispatch_outcome={before_release.terminal_outcome} "
        f"cancellations={len(canceled_before_release)}"
    )
    assert result is not None
    request = race.executor.started[-1]
    assert result.execution_ref == f"exec-{request.dispatch_id}"
    assert canceled_before_release == ()
    assert race.executor.canceled == []
    assert before_release.terminal_outcome is not DispatchOutcome.CANCELED
    assert _stored_dispatch(race.adapter).terminal_outcome is not DispatchOutcome.CANCELED


def _seed_publishing_guard(current: _Race) -> None:
    current.adapter.put_run(current.run().model_copy(update={"state": RunState.PUBLISHING}))


def _assert_compensated_without_pointer(current: _Race) -> None:
    request = current.executor.started[-1]
    assert current.run().state is RunState.PUBLISHING
    assert [item.execution_ref for item in current.executor.canceled] == [
        f"exec-{request.dispatch_id}",
    ]
    assert current.adapter.get_dataset_pointer(_TENANT, "cnes") is None


def test_publishing_com_onda_parcial_nao_suprime_parent_not_processing(race: _Race) -> None:
    """Controle de guarda: PUBLISHING incompatível é semeado pela API pública."""
    _seed(race.adapter, manifests=_full_manifests())

    def partial(request: StartRunExecution) -> None:
        race.worker(notify=False).execute(race.claim(request, request.unit_ids[0]))
        _seed_publishing_guard(race)

    race.inside_start(partial)

    with pytest.raises(Conflict, match="parent_not_processing") as rejected:
        race.coordinator().resume(_TENANT, _RUN_ID)

    assert rejected.value.code is ControlPlaneErrorCode.PARENT_NOT_PROCESSING
    request = race.executor.started[-1]
    units = race.adapter.list_run_units(_TENANT, _RUN_ID)
    wave = [unit for unit in units if unit.unit_id in request.unit_ids]
    assert len(wave) == len(request.unit_ids) == 2
    assert {unit.state for unit in wave} == {RunUnitState.SUCCEEDED, RunUnitState.PENDING}
    _assert_compensated_without_pointer(race)
    assert _stored_dispatch(race.adapter).terminal_outcome is DispatchOutcome.CANCELED


@pytest.mark.parametrize("claim", [False, True], ids=["pendente", "leased"])
def test_publishing_com_unidade_nao_terminal_nao_suprime_parent_not_processing(
    race: _Race, claim: bool,
) -> None:
    """Controle de guarda: PUBLISHING incompatível é semeado pela API pública."""
    race.prepare_last_wave()

    def incomplete(request: StartRunExecution) -> None:
        if claim:
            race.adapter.claim_run_unit(race.claim(request, request.unit_ids[0]))
        _seed_publishing_guard(race)

    race.inside_start(incomplete)

    with pytest.raises(Conflict, match="parent_not_processing") as rejected:
        race.coordinator().resume(_TENANT, _RUN_ID)

    assert rejected.value.code is ControlPlaneErrorCode.PARENT_NOT_PROCESSING
    request = race.executor.started[-1]
    units = race.adapter.list_run_units(_TENANT, _RUN_ID)
    wave = [unit for unit in units if unit.unit_id in request.unit_ids]
    assert len(wave) == len(request.unit_ids) == 1
    expected = RunUnitState.LEASED if claim else RunUnitState.PENDING
    assert wave[0].state is expected
    _assert_compensated_without_pointer(race)
    assert _stored_dispatch(race.adapter).terminal_outcome is DispatchOutcome.CANCELED


def test_publishing_concluido_por_outro_dispatch_nao_suprime_conflito(race: _Race) -> None:
    """Controle de guarda: PUBLISHING é semeado após conclusão real pelo novo dispatch."""
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
            race.worker(notify=False).execute(race.claim(newer, unit_id))
        _seed_publishing_guard(race)

    race.inside_start(replaced)

    with pytest.raises(Conflict, match="dispatch_finish_conflict") as rejected:
        race.coordinator().resume(_TENANT, _RUN_ID)

    assert rejected.value.code is ControlPlaneErrorCode.DISPATCH_FINISH_CONFLICT
    cause = rejected.value.__context__
    assert isinstance(cause, Conflict)
    assert cause.code is ControlPlaneErrorCode.PARENT_NOT_PROCESSING
    request = race.executor.started[-1]
    assert replacements != [request.dispatch_id]
    units = race.adapter.list_run_units(_TENANT, _RUN_ID)
    wave = [unit for unit in units if unit.unit_id in request.unit_ids]
    assert len(wave) == len(request.unit_ids)
    assert {unit.dispatch_id for unit in wave} == set(replacements)
    assert all(unit.state is RunUnitState.SUCCEEDED for unit in wave)
    _assert_compensated_without_pointer(race)
    assert _stored_dispatch(race.adapter).dispatch_id == replacements[0]
    assert _stored_dispatch(race.adapter).terminal_outcome is not DispatchOutcome.CANCELED
