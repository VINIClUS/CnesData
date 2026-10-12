"""Guardas do dispatch no RunPlanningService: identidade, reentrada, reconcile e perfis."""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest
from botocore.exceptions import ReadTimeoutError

from apps.central_api.tests.services.test_run_planning import (
    _COMPETENCIA,
    _NOW,
    _RUN_ID,
    _TENANT,
    _run,
    _seed_full_chain,
)
from apps.central_api.tests.services.test_run_planning_environment_gate import (
    _ENDPOINT,
    _ENVIRONMENT,
    _GRACE,
    _LEASE_SECONDS,
    _NEXT,
    _ORPHAN,
    _ORPHAN_REF,
    _active,
    _claim,
    _held,
    _hold,
    _holder,
    _permit,
    _planned,
    adapter,
    clock,
    executor,
    gate,
    service,
    store,
)
from central_api.services.run_planning import RunPlanningService
from cnes_domain.control_plane.entities import RawManifestRecord
from cnes_domain.control_plane.enums import DispatchState, RunState
from cnes_domain.control_plane.environment_gate import ClosePromotionFence
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
from cnes_domain.ports.environment_gate import (
    UNGATED_ADMISSION,
    DeferralReason,
    DispatchDeferred,
    UnitAdmission,
)
from cnes_domain.ports.processing import ExecutionCallbacks, ExecutionPermit, ExecutionStatus
from cnes_infra.executor.step_functions import ProcessorExecutionUnavailable
from data_processor.orchestration.coordinator import noop_execution_started

if TYPE_CHECKING:
    from datetime import datetime

    from apps.central_api.tests.services.test_run_planning import (
        _FakeObjectStore,
        _MutableClock,
    )
    from apps.central_api.tests.services.test_run_planning_environment_gate import (
        _StepFunctions,
    )
    from cnes_domain.control_plane.entities import Run, RunDispatch, RunUnit
    from cnes_domain.control_plane.environment_gate import PermitHolder
    from cnes_domain.ports.environment_gate import ExecutionObservation
    from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
    from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane

__all__ = ["adapter", "clock", "executor", "gate", "service", "store"]
_FENCE_DEFERRED = [(RunState.PROCESSING, DeferralReason.FENCE_CLOSED)]
_WAITING_RUN_IDS = ("run-1", "run-2", "run-3")


@dataclass
class _AdmissionRecorder:
    """`DispatchAdmissionPort` que só registra as chamadas recebidas."""

    calls: list[str] = field(default_factory=list[str])

    def admit(self, holder: PermitHolder, now: datetime) -> UnitAdmission | DispatchDeferred:
        self.calls.append("admit")
        return UnitAdmission(None, None)

    def confirm(self, admission: UnitAdmission, execution_ref: str, now: datetime) -> None:
        self.calls.append("confirm")

    def settle(self, holder: PermitHolder, now: datetime) -> None:
        self.calls.append("settle")

    def reconcile(self, now: datetime) -> None:
        self.calls.append("reconcile")


def _foreign(run: Run, dispatch: RunDispatch, requested_limit: int) -> ExecutionPermit:
    return ExecutionPermit(
        tenant_id=run.tenant_id, run_id="run-foreign", max_concurrency=requested_limit,
        policy_version=0, fencing_token=0,
    )


def _throttled(dispatch_id: str) -> ExecutionObservation:
    raise ProcessorExecutionUnavailable("ThrottlingException")


def _waiting_with_closed_fence(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, gate: DynamoDBControlPlane,
    clock: _MutableClock,
) -> None:
    adapter.put_run(_run(state=RunState.WAITING_INPUTS))
    _seed_full_chain(adapter, store)
    _hold(gate, clock, _ORPHAN_REF)
    assert gate.close_promotion_fence(ClosePromotionFence(_ENVIRONMENT, "release-2", clock.now()))


def _cancel_requested_with_claimed_unit(
    adapter: SQLiteControlPlane, service: RunPlanningService, clock: _MutableClock,
) -> None:
    service.launch(_TENANT, _RUN_ID)
    adapter.claim_run_unit(_claim(_active(adapter), clock))
    canceling = cast("Run", adapter.get_run(_TENANT, _RUN_ID))
    adapter.put_run(canceling.model_copy(update={"state": RunState.CANCEL_REQUESTED}))


def _with_execution(service: RunPlanningService, **changes: Any) -> RunPlanningService:
    execution = replace(service._execution, **changes)
    return RunPlanningService(service._dependencies, execution, service._clock)


def _ambiguous_launch(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    production: RunPlanningService,
) -> RunDispatch:
    _planned(adapter, store)
    executor.start_error = ReadTimeoutError(endpoint_url=_ENDPOINT)
    with pytest.raises(ReadTimeoutError):
        production.launch(_TENANT, _RUN_ID)
    executor.start_error = None
    return _active(adapter)


def _close_fence(gate: DynamoDBControlPlane, clock: _MutableClock) -> None:
    assert gate.close_promotion_fence(ClosePromotionFence(_ENVIRONMENT, "release-3", clock.now()))


def _accepted_local_manifest() -> RawManifestRecord:
    return RawManifestRecord(
        tenant_id=_TENANT, manifest_id="manifest-trigger",
        manifest_key=f"raw/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/local-1/manifest.json",
        agent_id="agent-1", source_type="CNES_LOCAL", file_subtype="CNES_VINCULO",
        competencia=_COMPETENCIA, snapshot_mode="FULL", snapshot_id="local-1",
        base_snapshot_id=None, sequence=1, previous_manifest_sha256=None,
        manifest_sha256="a" * 64, created_at=_NOW,
    )


def test_identidade_divergente_nao_adquire_o_permit(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService,
) -> None:
    _planned(adapter, store)
    callbacks = ExecutionCallbacks(_foreign, noop_execution_started)
    foreign = _with_execution(service, callbacks=callbacks)

    with pytest.raises(ValueError, match="execution_permit_identity_mismatch"):
        foreign.launch(_TENANT, _RUN_ID)

    assert executor.calls == []
    assert _permit(gate) is None


def test_erro_ambiguo_na_reentrada_tardia_nao_libera(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService, clock: _MutableClock,
) -> None:
    production = _with_execution(service, dispatch_lease_seconds=3600)
    _planned(adapter, store)
    executor.start_error = ReadTimeoutError(endpoint_url=_ENDPOINT)
    with pytest.raises(ReadTimeoutError):
        production.launch(_TENANT, _RUN_ID)
    dispatch = _active(adapter)
    clock.advance(timedelta(minutes=30))

    with pytest.raises(ReadTimeoutError):
        production.launch(_TENANT, _RUN_ID)

    assert _active(adapter).dispatch_id == dispatch.dispatch_id
    assert executor.calls == ["start", "probe", "start", "probe"]
    assert (_held(gate).holder, _held(gate).acquired_at) == (_holder(dispatch), clock.now())
    executor.statuses[f"exec-{dispatch.dispatch_id}"] = ExecutionStatus.RUNNING
    rival = EnvironmentAdmission(gate, executor, _ENVIRONMENT, _LEASE_SECONDS)
    assert rival.admit(_NEXT, clock.now()) == DispatchDeferred(DeferralReason.PERMIT_HELD)


def test_reentrada_sem_bind_com_fence_fechado_nao_inicia_execucao_nova(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService, clock: _MutableClock,
) -> None:
    production = _with_execution(service, dispatch_lease_seconds=3600)
    dispatch = _ambiguous_launch(adapter, store, executor, production)
    anchor = _held(gate).acquired_at
    clock.advance(timedelta(minutes=5))
    _close_fence(gate, clock)
    clock.advance(timedelta(minutes=1))

    results = production.recover(limit=10)

    assert [(result.run.state, result.execution_ref, result.deferred) for result in results] == [
        (RunState.PROCESSING, None, DeferralReason.FENCE_CLOSED),
    ]
    assert (executor.calls.count("start"), executor.statuses) == (1, {})
    assert (_held(gate).holder, _held(gate).acquired_at) == (_holder(dispatch), anchor)
    clock.advance(anchor + _GRACE - clock.now())
    assert [result.deferred for result in production.recover(limit=10)] == [
        DeferralReason.FENCE_CLOSED,
    ]
    assert _permit(gate) is None


def test_reentrada_sem_bind_com_execucao_viva_e_fence_fechado_retoma_a_mesma_execucao(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService, clock: _MutableClock,
) -> None:
    production = _with_execution(service, dispatch_lease_seconds=3600)
    dispatch = _ambiguous_launch(adapter, store, executor, production)
    reference = f"exec-{dispatch.dispatch_id}"
    executor.statuses[reference] = ExecutionStatus.RUNNING
    _close_fence(gate, clock)
    clock.advance(timedelta(minutes=1))

    result = production.launch(_TENANT, _RUN_ID)

    assert (result.execution_ref, result.deferred) == (reference, None)
    assert executor.calls == ["start", "probe", "probe", "start"]
    assert executor.statuses == {reference: ExecutionStatus.RUNNING}
    assert (_held(gate).holder, _held(gate).execution_ref) == (_holder(dispatch), reference)


def test_erro_de_probe_num_run_nao_deixa_os_seguintes_em_waiting_inputs(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    service: RunPlanningService, monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    for run_id in _WAITING_RUN_IDS:
        adapter.put_run(_run(state=RunState.WAITING_INPUTS, run_id=run_id))
    _seed_full_chain(adapter, store)
    monkeypatch.setattr(executor, "probe", _throttled)

    service.on_raw_manifest_accepted(_accepted_local_manifest())

    runs = [cast("Run", adapter.get_run(_TENANT, run_id)) for run_id in _WAITING_RUN_IDS]
    assert [run.state for run in runs] == [RunState.PROCESSING] * 3
    assert executor.calls == ["start"]
    failures = [
        record for record in caplog.records
        if record.getMessage().startswith("accepted_manifest_launch_failed")
    ]
    assert [record.getMessage() for record in failures] == [
        f"accepted_manifest_launch_failed tenant_id={_TENANT} run_id={run_id}"
        for run_id in _WAITING_RUN_IDS[1:]
    ]
    assert all(
        cast("Any", record.exc_info)[0] is ProcessorExecutionUnavailable for record in failures
    )


def test_reconcile_abre_a_passada_da_api_antes_de_listar_os_runs(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, service: RunPlanningService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = _AdmissionRecorder()
    list_recoverable_runs = adapter.list_recoverable_runs

    def _list(now: datetime, limit: int) -> tuple[Run, ...]:
        recorder.calls.append("list")
        return list_recoverable_runs(now, limit)

    monkeypatch.setattr(adapter, "list_recoverable_runs", _list)
    adapter.put_run(_run(state=RunState.WAITING_INPUTS))
    _seed_full_chain(adapter, store)

    _with_execution(service, admission=recorder).recover(limit=10)

    assert recorder.calls == ["reconcile", "list", "admit", "confirm"]


def test_recover_libera_o_permit_da_execucao_encerrada_com_fence_fechado(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService, clock: _MutableClock,
) -> None:
    _waiting_with_closed_fence(adapter, store, gate, clock)
    executor.statuses[_ORPHAN_REF] = ExecutionStatus.SUCCEEDED

    results = service.recover(limit=10)

    assert [(result.run.state, result.deferred) for result in results] == _FENCE_DEFERRED
    assert executor.calls == ["probe"]
    assert _permit(gate) is None


def test_erro_do_reconcile_falha_o_recover_da_api_depois_de_lancar_os_runs(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, executor: _StepFunctions,
    gate: DynamoDBControlPlane, service: RunPlanningService, clock: _MutableClock,
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    _waiting_with_closed_fence(adapter, store, gate, clock)
    monkeypatch.setattr(executor, "probe", _throttled)

    with pytest.raises(ProcessorExecutionUnavailable, match="ThrottlingException"):
        service.recover(limit=10)

    assert cast("Run", adapter.get_run(_TENANT, _RUN_ID)).state is RunState.PROCESSING
    assert _active(adapter).state is DispatchState.RESERVED
    assert _held(gate).holder == _ORPHAN
    assert "unit_permit_reconcile_failed" in caplog.text


def test_dispatch_desabilitado_nao_liquida_a_unidade_reivindicada(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, service: RunPlanningService,
    clock: _MutableClock,
) -> None:
    _planned(adapter, store)
    _cancel_requested_with_claimed_unit(adapter, service, clock)
    recorder = _AdmissionRecorder()
    plan_only = RunPlanningService(
        service._dependencies, replace(service._execution, admission=recorder), service._clock,
        dispatch_enabled=False,
    )

    assert plan_only.launch(_TENANT, _RUN_ID).run.state is RunState.CANCELED
    assert recorder.calls == []


def test_dispatch_desabilitado_nao_reconcilia_o_gate(service: RunPlanningService) -> None:
    recorder = _AdmissionRecorder()
    plan_only = RunPlanningService(
        service._dependencies, replace(service._execution, admission=recorder), service._clock,
        dispatch_enabled=False,
    )

    assert plan_only.recover(limit=10) == ()
    assert recorder.calls == []


def test_cancelamento_sem_gate_nao_rele_as_unidades(
    adapter: SQLiteControlPlane, store: _FakeObjectStore, service: RunPlanningService,
    clock: _MutableClock, monkeypatch: pytest.MonkeyPatch,
) -> None:
    ungated = _with_execution(service, admission=UNGATED_ADMISSION)
    _planned(adapter, store)
    _cancel_requested_with_claimed_unit(adapter, ungated, clock)
    reads: list[str] = []
    list_run_units = adapter.list_run_units

    def _list(tenant_id: str, run_id: str) -> tuple[RunUnit, ...]:
        reads.append(run_id)
        return list_run_units(tenant_id, run_id)

    monkeypatch.setattr(adapter, "list_run_units", _list)

    assert ungated.launch(_TENANT, _RUN_ID).run.state is RunState.CANCELED
    assert reads == []
