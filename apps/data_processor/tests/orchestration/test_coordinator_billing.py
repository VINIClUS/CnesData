"""TDD da ordem de billing no PipelineCoordinator: reserva, policy, start, bind e started."""
from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from cnes_domain.billing.errors import EntitlementDenied
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState
from cnes_domain.ports.processing import ExecutionPermit
from data_processor.orchestration.coordinator import PipelineCoordinator

from .test_coordinator import (
    _RUN_ID,
    _TENANT,
    _complete_dispatch,
    _dependencies,
    _execution,
    _full_manifests,
    _seed,
    _stored_dispatch,
    adapter,
    clock,
    executor,
    store,
)

_READY_UNITS = 2
_WAVE_ORDER = ["reserve", "policy", "start", "bind", "started"]
__all__ = ["adapter", "clock", "executor", "store"]


@dataclass
class _Recorder:
    events: list[str] = field(default_factory=list)
    limits: list[int] = field(default_factory=list)
    permits: list[ExecutionPermit] = field(default_factory=list)
    seen: list[ExecutionPermit] = field(default_factory=list)
    clamp: int | None = None
    policy_error: Exception | None = None
    started_error: Exception | None = None

    def policy(self, run, dispatch, requested_limit) -> ExecutionPermit:
        self.events.append("policy")
        self.limits.append(requested_limit)
        if self.policy_error is not None:
            raise self.policy_error
        permit = ExecutionPermit(
            tenant_id=run.tenant_id, run_id=run.run_id, policy_version=3, fencing_token=5,
            max_concurrency=self.clamp or requested_limit,
        )
        self.permits.append(permit)
        return permit

    def started(self, run, request, execution_ref, permit) -> None:
        self.events.append("started")
        self.seen.append(permit)
        if self.started_error is not None:
            raise self.started_error


@pytest.fixture
def recorder() -> _Recorder:
    return _Recorder()


def _instrument(adapter, executor, recorder) -> None:
    reserve, bind, finish = (
        adapter.reserve_run_dispatch, adapter.bind_run_dispatch, adapter.finish_run_dispatch,
    )
    start, cancel = executor.start, executor.cancel

    def _wrap(name, original):
        def _call(arg):
            recorder.events.append(name)
            return original(arg)
        return _call

    adapter.reserve_run_dispatch = _wrap("reserve", reserve)
    adapter.bind_run_dispatch = _wrap("bind", bind)
    adapter.finish_run_dispatch = _wrap("finish", finish)
    executor.start = _wrap("start", start)
    executor.cancel = _wrap("cancel", cancel)


def _coordinator(adapter, executor, store, clock, recorder, limit: int = 2):
    _seed(adapter, manifests=_full_manifests())
    _instrument(adapter, executor, recorder)
    execution = _execution(policy=recorder.policy, started=recorder.started, limit=limit)
    return PipelineCoordinator(_dependencies(adapter, executor, store, clock), execution)


def test_dispatch_e_reservado_antes_da_policy_e_started_vem_apos_start_e_bind(
    adapter, executor, store, clock, recorder
):
    coordinator = _coordinator(adapter, executor, store, clock, recorder)

    coordinator.resume(_TENANT, _RUN_ID)

    assert recorder.events == _WAVE_ORDER


@pytest.mark.parametrize(("limit", "expected"), [(5, _READY_UNITS), (1, 1)])
def test_policy_recebe_minimo_entre_unidades_prontas_e_limite_do_deployment(
    adapter, executor, store, clock, recorder, limit, expected
):
    coordinator = _coordinator(adapter, executor, store, clock, recorder, limit=limit)

    coordinator.resume(_TENANT, _RUN_ID)

    assert recorder.limits == [expected]
    assert executor.started[0].max_concurrency == expected


def test_started_recebe_a_mesma_instancia_de_permit_devolvida_pela_policy(
    adapter, executor, store, clock, recorder
):
    coordinator = _coordinator(adapter, executor, store, clock, recorder)

    coordinator.resume(_TENANT, _RUN_ID)

    assert len(recorder.permits) == 1
    assert recorder.seen[0] is recorder.permits[0]


def test_policy_que_reduz_concorrencia_limita_o_start(adapter, executor, store, clock, recorder):
    recorder.clamp = 1
    coordinator = _coordinator(adapter, executor, store, clock, recorder, limit=4)

    coordinator.resume(_TENANT, _RUN_ID)

    assert recorder.limits == [_READY_UNITS]
    assert executor.started[0].max_concurrency == 1


def test_policy_negada_nao_inicia_execucao_nem_vincula(
    adapter, executor, store, clock, recorder
):
    recorder.policy_error = EntitlementDenied("reason=quota_exhausted")
    coordinator = _coordinator(adapter, executor, store, clock, recorder)

    with pytest.raises(EntitlementDenied):
        coordinator.resume(_TENANT, _RUN_ID)

    assert recorder.events == ["reserve", "policy"]
    assert executor.started == []
    assert _stored_dispatch(adapter).state is DispatchState.RESERVED


def test_started_falho_cancela_execucao_finaliza_dispatch_e_propaga(
    adapter, executor, store, clock, recorder
):
    recorder.started_error = RuntimeError("callback=down")
    coordinator = _coordinator(adapter, executor, store, clock, recorder)

    with pytest.raises(RuntimeError, match="callback=down"):
        coordinator.resume(_TENANT, _RUN_ID)

    failed = _stored_dispatch(adapter)
    assert recorder.events[-3:] == ["started", "cancel", "finish"]
    assert failed.terminal_outcome is DispatchOutcome.CANCELED
    assert [request.execution_ref for request in executor.canceled] == [failed.execution_ref]


def test_dispatch_reservado_pendente_e_reaproveitado_na_retomada(
    adapter, executor, store, clock, recorder
):
    recorder.policy_error = EntitlementDenied("reason=quota_exhausted")
    coordinator = _coordinator(adapter, executor, store, clock, recorder)
    with pytest.raises(EntitlementDenied):
        coordinator.resume(_TENANT, _RUN_ID)
    pending = _stored_dispatch(adapter)
    recorder.policy_error = None
    recorder.events.clear()

    coordinator.resume(_TENANT, _RUN_ID)

    active = adapter.get_active_run_dispatch(_TENANT, _RUN_ID)
    assert "reserve" not in recorder.events
    assert (active.dispatch_id, active.generation) == (pending.dispatch_id, pending.generation)
    assert active.state is DispatchState.STARTED
    assert len(recorder.limits) == 2


def test_tres_ondas_sequenciais_geram_bindings_distintos_sem_sobreposicao(
    adapter, executor, store, clock, recorder
):
    coordinator = _coordinator(adapter, executor, store, clock, recorder)
    bindings = []

    for _ in range(3):
        coordinator.resume(_TENANT, _RUN_ID)
        active = adapter.get_active_run_dispatch(_TENANT, _RUN_ID)
        bindings.append((
            active.wave_id, active.dispatch_id, active.generation, active.execution_ref
        ))
        _complete_dispatch(adapter, executor, store, clock)

    wave_ids, dispatch_ids, _, execution_refs = zip(*bindings, strict=True)
    assert len(set(bindings)) == 3
    assert len(set(wave_ids)) == len(set(dispatch_ids)) == len(set(execution_refs)) == 3
    assert recorder.events == [
        *_WAVE_ORDER, "finish", *_WAVE_ORDER, "finish", *_WAVE_ORDER,
    ]
    assert len(executor.started) == 3
    assert len(recorder.limits) == 3
