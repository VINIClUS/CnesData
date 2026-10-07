"""TDD da ordem de billing no RunPlanningService: reserva, policy, start, bind e started."""
from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from apps.central_api.tests.services.test_run_planning import (
    _NOW,
    _RUN_ID,
    _TENANT,
    _FakeExecutor,
    _FakeObjectStore,
    _MutableClock,
    _run,
    _seed_full_chain,
    _service,
    _stored_dispatch,
)
from cnes_domain.billing.errors import EntitlementDenied
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState
from cnes_domain.ports.processing import ExecutionPermit
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane

_READY_UNITS = 2


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
def clock() -> _MutableClock:
    return _MutableClock(_NOW)


@pytest.fixture
def adapter(tmp_path, clock) -> SQLiteControlPlane:
    control_plane = SQLiteControlPlane(tmp_path / "cp.db", clock.now)
    control_plane.initialize()
    return control_plane


@pytest.fixture
def store() -> _FakeObjectStore:
    return _FakeObjectStore()


@pytest.fixture
def executor() -> _FakeExecutor:
    return _FakeExecutor()


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


def _launcher(adapter, executor, store, clock, recorder, limit: int = 2):
    adapter.put_run(_run())
    _seed_full_chain(adapter, store)
    _instrument(adapter, executor, recorder)
    return _service(
        adapter, executor, store, clock,
        policy=recorder.policy, started=recorder.started, limit=limit,
    )


def test_dispatch_e_reservado_antes_da_policy_e_started_vem_apos_start_e_bind(
    adapter, executor, store, clock, recorder
):
    service = _launcher(adapter, executor, store, clock, recorder)

    service.launch(_TENANT, _RUN_ID)

    assert recorder.events == ["reserve", "policy", "start", "bind", "started"]


@pytest.mark.parametrize(("limit", "expected"), [(5, _READY_UNITS), (1, 1)])
def test_policy_recebe_minimo_entre_unidades_prontas_e_limite_do_deployment(
    adapter, executor, store, clock, recorder, limit, expected
):
    service = _launcher(adapter, executor, store, clock, recorder, limit=limit)

    service.launch(_TENANT, _RUN_ID)

    assert recorder.limits == [expected]
    assert executor.started[0].max_concurrency == expected


def test_started_recebe_a_mesma_instancia_de_permit_devolvida_pela_policy(
    adapter, executor, store, clock, recorder
):
    service = _launcher(adapter, executor, store, clock, recorder)

    service.launch(_TENANT, _RUN_ID)

    assert len(recorder.permits) == 1
    assert recorder.seen[0] is recorder.permits[0]


def test_policy_que_reduz_concorrencia_limita_o_start(adapter, executor, store, clock, recorder):
    recorder.clamp = 1
    service = _launcher(adapter, executor, store, clock, recorder, limit=4)

    service.launch(_TENANT, _RUN_ID)

    assert recorder.limits == [_READY_UNITS]
    assert executor.started[0].max_concurrency == 1


def test_policy_negada_nao_inicia_execucao_nem_vincula(
    adapter, executor, store, clock, recorder
):
    recorder.policy_error = EntitlementDenied("reason=quota_exhausted")
    service = _launcher(adapter, executor, store, clock, recorder)

    with pytest.raises(EntitlementDenied):
        service.launch(_TENANT, _RUN_ID)

    assert recorder.events == ["reserve", "policy"]
    assert executor.started == []
    assert _stored_dispatch(adapter).state is DispatchState.RESERVED


def test_started_falho_cancela_execucao_finaliza_dispatch_e_propaga(
    adapter, executor, store, clock, recorder
):
    recorder.started_error = RuntimeError("callback=down")
    service = _launcher(adapter, executor, store, clock, recorder)

    with pytest.raises(RuntimeError, match="callback=down"):
        service.launch(_TENANT, _RUN_ID)

    failed = _stored_dispatch(adapter)
    assert recorder.events[-3:] == ["started", "cancel", "finish"]
    assert failed.terminal_outcome is DispatchOutcome.CANCELED
    assert [request.execution_ref for request in executor.canceled] == [failed.execution_ref]


def test_dispatch_reservado_pendente_e_reaproveitado_na_retomada(
    adapter, executor, store, clock, recorder
):
    recorder.policy_error = EntitlementDenied("reason=quota_exhausted")
    service = _launcher(adapter, executor, store, clock, recorder)
    with pytest.raises(EntitlementDenied):
        service.launch(_TENANT, _RUN_ID)
    pending = _stored_dispatch(adapter)
    recorder.policy_error = None
    recorder.events.clear()

    service.launch(_TENANT, _RUN_ID)

    active = adapter.get_active_run_dispatch(_TENANT, _RUN_ID)
    assert "reserve" not in recorder.events
    assert (active.dispatch_id, active.generation) == (pending.dispatch_id, pending.generation)
    assert active.state is DispatchState.STARTED
    assert len(recorder.limits) == 2
