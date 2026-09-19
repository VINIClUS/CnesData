"""Launches and recovers Runs: raw-chain reconstruction, planning, dispatch, cancellation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from cnes_contracts.manifests.raw import RawManifest
from cnes_domain.control_plane.commands import (
    BindRunDispatch,
    FinalizeRunCancellation,
    FinishRunDispatch,
    PutRunUnits,
    ReserveRunDispatch,
    TransitionRun,
)
from cnes_domain.control_plane.entities import OutboxEvent
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState, RunState
from cnes_domain.control_plane.queries import (
    RawIdentity,
    RawManifestChainQuery,
    WaitingRunsForDependencyQuery,
)
from cnes_domain.orchestration.planner import (
    PlanRequest,
    RawManifestRef,
    RunPlan,
    execution_request,
    logical_wave_id,
    plan_run,
    ready_units,
)
from cnes_domain.ports.processing import CancelRunExecution, ExecutionStatus

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_domain.control_plane.entities import ManifestRef, RawManifestRecord, Run, RunDispatch
    from cnes_domain.orchestration.source_catalog import SourceCatalog
    from cnes_domain.ports.control_plane import ControlPlanePort, TypedRawQueryPort
    from cnes_domain.ports.object_store import ObjectStorePort
    from cnes_domain.ports.processing import (
        ExecutionPolicyConfig,
        ProcessorExecutorPort,
        StartRunExecution,
    )

    class _ControlPlane(ControlPlanePort, TypedRawQueryPort, Protocol):
        pass

_READ_ONLY_STATES = frozenset({
    RunState.PUBLISHING, RunState.PUBLISHED, RunState.PUBLISHED_DEGRADED,
    RunState.FAILED, RunState.CANCELED,
})
_PLANNABLE_STATES = frozenset({RunState.PLANNED, RunState.WAITING_INPUTS})
_STATUS_OUTCOME = {
    ExecutionStatus.SUCCEEDED: DispatchOutcome.SUCCEEDED,
    ExecutionStatus.FAILED: DispatchOutcome.FAILED,
    ExecutionStatus.CANCELED: DispatchOutcome.CANCELED,
}


@dataclass(frozen=True, slots=True)
class RunPlanningDependencies:
    control_plane: _ControlPlane
    object_store: ObjectStorePort
    executor: ProcessorExecutorPort
    source_catalog: SourceCatalog


@dataclass(frozen=True, slots=True)
class RunLaunchResult:
    run: Run
    plan: RunPlan | None
    execution_ref: str | None


def _build_event(run: Run, event_type: str, now: datetime) -> OutboxEvent:
    return OutboxEvent(
        tenant_id=run.tenant_id, event_id=f"{event_type}:{run.tenant_id}:{run.run_id}",
        event_type=event_type, aggregate_id=run.run_id,
        payload={"dataset_name": run.dataset_name}, created_at=now, delivered_at=None,
    )


def _raw_manifest_ref(store: ObjectStorePort, ref: ManifestRef) -> RawManifestRef:
    with store.open(ref.manifest_key) as stream:
        payload = stream.read()
    manifest = RawManifest.model_validate_json(payload)
    if manifest.manifest_id != ref.manifest_id:
        raise ValueError("raw_manifest_id_mismatch")
    canonical = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    if canonical != payload:
        raise ValueError("raw_manifest_not_canonical")
    return RawManifestRef(
        manifest_id=manifest.manifest_id, manifest_key=ref.manifest_key,
        source_type=manifest.source_type.value, file_subtype=manifest.file_subtype,
        partition=manifest.competencia,
    )


def _raw_chain(dependencies: RunPlanningDependencies, run: Run) -> tuple[RawManifestRef, ...]:
    definition = dependencies.source_catalog.for_pipeline(run.dataset_name)
    refs: list[RawManifestRef] = []
    for dependency in definition.dependencies:
        identity = RawIdentity(
            run.tenant_id, dependency.source_type, dependency.file_subtype, run.competencia
        )
        chain = dependencies.control_plane.query_raw_manifest_chain(RawManifestChainQuery(identity))
        refs.extend(_raw_manifest_ref(dependencies.object_store, ref) for ref in chain)
    return tuple(refs)


def _reserve_next_wave(
    control_plane: _ControlPlane, plan: RunPlan, execution: ExecutionPolicyConfig, now: datetime
) -> RunDispatch | None:
    ready = ready_units(plan, now)
    if not ready:
        return None
    wave_id = logical_wave_id(ready)
    unit_ids = tuple(sorted(unit.unit_id for unit in ready))
    return control_plane.reserve_run_dispatch(ReserveRunDispatch(
        tenant_id=plan.run.tenant_id, run_id=plan.run.run_id, wave_id=wave_id,
        unit_ids=unit_ids, now=now, lease_seconds=execution.dispatch_lease_seconds,
    ))


def _settle_started(
    control_plane: _ControlPlane, executor: ProcessorExecutorPort,
    plan: RunPlan, dispatch: RunDispatch, now: datetime,
) -> RunDispatch | None:
    status = executor.status(dispatch.execution_ref)
    if status is ExecutionStatus.RUNNING:
        return dispatch
    control_plane.finish_run_dispatch(FinishRunDispatch(
        tenant_id=plan.run.tenant_id, run_id=plan.run.run_id, dispatch_id=dispatch.dispatch_id,
        outcome=_STATUS_OUTCOME[status], finished_at=now,
    ))
    return None


def _start_and_bind(
    control_plane: _ControlPlane, executor: ProcessorExecutorPort,
    execution: ExecutionPolicyConfig, plan: RunPlan, dispatch: RunDispatch, now: datetime,
) -> str:
    run = plan.run
    permit = execution.callbacks.policy(run, dispatch, execution.deployment_limit)
    if permit.tenant_id != run.tenant_id or permit.run_id != run.run_id:
        raise ValueError("execution_permit_identity_mismatch")
    request: StartRunExecution = execution_request(plan, dispatch, permit.max_concurrency)
    execution_ref = executor.start(request)
    bound = control_plane.bind_run_dispatch(BindRunDispatch(
        tenant_id=run.tenant_id, run_id=run.run_id, dispatch_id=dispatch.dispatch_id,
        execution_ref=execution_ref, now=now, lease_seconds=execution.dispatch_lease_seconds,
    ))
    execution.callbacks.started(run, request, execution_ref, permit)
    return bound.execution_ref


def _dispatch_protocol(
    control_plane: _ControlPlane, executor: ProcessorExecutorPort,
    execution: ExecutionPolicyConfig, plan: RunPlan, now: datetime,
) -> str | None:
    active = control_plane.get_active_run_dispatch(plan.run.tenant_id, plan.run.run_id)
    if active is not None and active.state is DispatchState.STARTED:
        active = _settle_started(control_plane, executor, plan, active, now)
        if active is not None:
            return active.execution_ref
    if active is None:
        active = _reserve_next_wave(control_plane, plan, execution, now)
        if active is None:
            return None
    return _start_and_bind(control_plane, executor, execution, plan, active, now)


class RunPlanningService:
    def __init__(
        self, dependencies: RunPlanningDependencies, execution: ExecutionPolicyConfig,
        clock: Callable[[], datetime],
    ) -> None:
        self._dependencies = dependencies
        self._execution = execution
        self._clock = clock

    def launch(self, tenant_id: str, run_id: str) -> RunLaunchResult:
        control_plane = self._dependencies.control_plane
        run = control_plane.get_run(tenant_id, run_id)
        if run is None:
            raise ValueError("run_not_found")
        if run.state in _READ_ONLY_STATES:
            return RunLaunchResult(run=run, plan=None, execution_ref=None)
        now = self._clock()
        if run.state is RunState.CANCEL_REQUESTED:
            return self._cancel(run, now)
        if run.state in _PLANNABLE_STATES:
            return self._plan_and_dispatch(run, now)
        if run.state is RunState.PROCESSING:
            return self._resume_processing(run, now)
        raise ValueError(f"unsupported_run_state:{run.state}")

    def _wait_for_inputs(self, run: Run, missing_required: tuple[str, ...], now: datetime) -> Run:
        if run.state is RunState.WAITING_INPUTS:
            return run
        event = _build_event(run, "run.waiting_inputs", now)
        return self._dependencies.control_plane.transition_run(TransitionRun(
            tenant_id=run.tenant_id, run_id=run.run_id, expected_state=RunState.PLANNED,
            new_state=RunState.WAITING_INPUTS, missing_sources=missing_required,
        ), event)

    def _to_processing(self, run: Run, missing_optional: tuple[str, ...], now: datetime) -> Run:
        event = _build_event(run, "run.processing", now)
        return self._dependencies.control_plane.transition_run(TransitionRun(
            tenant_id=run.tenant_id, run_id=run.run_id, expected_state=run.state,
            new_state=RunState.PROCESSING, missing_sources=missing_optional,
        ), event)

    def _plan_and_dispatch(self, run: Run, now: datetime) -> RunLaunchResult:
        control_plane = self._dependencies.control_plane
        definition = self._dependencies.source_catalog.for_pipeline(run.dataset_name)
        if definition.dependencies != run.dependencies:
            raise ValueError("dependency_definition_mismatch")
        manifests = _raw_chain(self._dependencies, run)
        plan = plan_run(PlanRequest(
            run=run, manifests=manifests, deployment_limit=self._execution.deployment_limit
        ))
        if plan.missing_required:
            waiting = self._wait_for_inputs(run, plan.missing_required, now)
            return RunLaunchResult(run=waiting, plan=None, execution_ref=None)
        persisted = control_plane.put_run_units(PutRunUnits(
            tenant_id=run.tenant_id, run_id=run.run_id, expected_run_state=run.state,
            units=plan.units,
        ))
        processing = self._to_processing(run, plan.missing_optional, now)
        full_plan = RunPlan(
            run=processing, units=persisted, missing_required=(),
            missing_optional=plan.missing_optional,
            deployment_limit=self._execution.deployment_limit,
        )
        execution_ref = _dispatch_protocol(
            control_plane, self._dependencies.executor, self._execution, full_plan, now
        )
        return RunLaunchResult(run=processing, plan=full_plan, execution_ref=execution_ref)

    def _resume_processing(self, run: Run, now: datetime) -> RunLaunchResult:
        control_plane = self._dependencies.control_plane
        units = control_plane.list_run_units(run.tenant_id, run.run_id)
        plan = RunPlan(
            run=run, units=units, missing_required=(), missing_optional=run.missing_sources,
            deployment_limit=self._execution.deployment_limit,
        )
        execution_ref = _dispatch_protocol(
            control_plane, self._dependencies.executor, self._execution, plan, now
        )
        return RunLaunchResult(run=run, plan=plan, execution_ref=execution_ref)

    def _cancel(self, run: Run, now: datetime) -> RunLaunchResult:
        # get_active_run_dispatch/claim_run_unit sao escopados a Run PROCESSING; uma vez
        # CANCEL_REQUESTED nenhum dispatch e visivel, entao o cancel e sempre best-effort.
        self._dependencies.executor.cancel(CancelRunExecution(
            tenant_id=run.tenant_id, run_id=run.run_id, execution_ref=None,
        ))
        event = _build_event(run, "run.canceled", now)
        canceled = self._dependencies.control_plane.finalize_run_cancellation(
            FinalizeRunCancellation(
                tenant_id=run.tenant_id, run_id=run.run_id,
                expected_state=RunState.CANCEL_REQUESTED, canceled_at=now,
            ), event,
        )
        return RunLaunchResult(run=canceled, plan=None, execution_ref=None)

    def recover(self, limit: int = 100) -> tuple[RunLaunchResult, ...]:
        now = self._clock()
        candidates = self._dependencies.control_plane.list_recoverable_runs(now, limit)
        return tuple(self.launch(run.tenant_id, run.run_id) for run in candidates)

    def on_raw_manifest_accepted(self, record: RawManifestRecord, limit: int = 100) -> None:
        identity = RawIdentity(
            record.tenant_id, record.source_type, record.file_subtype, record.competencia
        )
        query = WaitingRunsForDependencyQuery(identity, limit)
        candidates = self._dependencies.control_plane.query_waiting_runs_for_dependency(query)
        for run in candidates:
            self.launch(run.tenant_id, run.run_id)


__all__ = ["RunLaunchResult", "RunPlanningDependencies", "RunPlanningService"]
