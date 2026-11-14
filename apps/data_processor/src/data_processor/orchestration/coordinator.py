"""Reserve -> start -> bind dispatch protocol; fan-in driven Run state machine."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from cnes_domain.control_plane.commands import FinalizeRunCancellation, TransitionRun
from cnes_domain.control_plane.entities import OutboxEvent
from cnes_domain.control_plane.enums import RunState
from cnes_domain.orchestration.dispatch_admission import settle_run_dispatches
from cnes_domain.orchestration.fan_in import decide_fan_in
from cnes_domain.orchestration.mode_canary import require_context
from cnes_domain.orchestration.planner import RunPlan
from cnes_domain.ports.environment_gate import UNGATED_ADMISSION, DeferralReason, DispatchDeferred
from cnes_domain.ports.processing import CancelRunExecution, ExecutionPermit
from data_processor.orchestration.coordinator_dispatch_start import (
    _dispatch_protocol,
)
from data_processor.orchestration.coordinator_dispatch_start import (
    _start_and_bind as _start_and_bind_compat,
)
from data_processor.orchestration.publisher import PublishRequest

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_domain.control_plane.entities import Run, RunDispatch, RunUnit
    from cnes_domain.orchestration.mode_canary import ModeCanaryBinding
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.processing import (
        ExecutionPolicyConfig,
        ProcessorExecutorPort,
        StartRunExecution,
    )
    from data_processor.orchestration.publisher import DatasetPublisher

_TERMINAL_RUN_STATES = frozenset(
    {
        RunState.PUBLISHED,
        RunState.PUBLISHED_DEGRADED,
        RunState.FAILED,
        RunState.CANCELED,
    }
)
_RECOVERABLE_RUN_STATES = frozenset(
    {
        RunState.PROCESSING,
        RunState.PUBLISHING,
        RunState.CANCEL_REQUESTED,
    }
)
logger = logging.getLogger(__name__)
_start_and_bind = _start_and_bind_compat


def _processor_recoverable_runs(
    control_plane: ControlPlanePort,
    now: datetime,
    limit: int,
    skipped: set[tuple[str, str]],
) -> tuple[Run, ...]:
    if limit <= 0:
        return ()
    query_limit = limit
    while True:
        candidates = control_plane.list_recoverable_runs(now, query_limit)
        selected = tuple(
            run for run in candidates
            if run.state in _RECOVERABLE_RUN_STATES
            and (run.tenant_id, run.run_id) not in skipped
        )
        if len(selected) >= limit or len(candidates) < query_limit:
            return selected[:limit]
        query_limit *= 2


def allow_execution(run: Run, dispatch: RunDispatch, requested_limit: int) -> ExecutionPermit:
    del dispatch
    return ExecutionPermit(
        tenant_id=run.tenant_id, run_id=run.run_id, max_concurrency=requested_limit,
        policy_version=0, fencing_token=0, binding_context=None,
    )


def noop_execution_started(
    run: Run, request: StartRunExecution, execution_ref: str, permit: ExecutionPermit
) -> None:
    del run, request, execution_ref, permit


@dataclass(frozen=True, slots=True)
class CoordinatorDependencies:
    control_plane: ControlPlanePort
    executor: ProcessorExecutorPort
    publisher: DatasetPublisher
    clock: Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class CoordinatorResult:
    state: RunState
    execution_ref: str | None
    published: bool
    deferred: DeferralReason | None = None


class RecoveryFailed(RuntimeError):
    """Passada de recovery que falhou em runs ou no reconcile (a causa); carrega os resultados."""

    def __init__(
        self,
        results: tuple[CoordinatorResult, ...],
        failed: int,
        *,
        reconcile_failed: bool = False,
    ) -> None:
        super().__init__(f"failed={failed} recovered={len(results)}")
        self.results = results
        self.failed = failed
        self.reconcile_failed = reconcile_failed


def _build_event(run: Run, event_type: str, now: datetime) -> OutboxEvent:
    return OutboxEvent(
        tenant_id=run.tenant_id, event_id=f"{event_type}:{run.tenant_id}:{run.run_id}",
        event_type=event_type, aggregate_id=run.run_id,
        payload={"dataset_name": run.dataset_name}, created_at=now, delivered_at=None,
    )


def _fail_run(
    control_plane: ControlPlanePort, run: Run, missing: tuple[str, ...], now: datetime
) -> Run:
    event = _build_event(run, "run.failed", now)
    return control_plane.transition_run(TransitionRun(
        tenant_id=run.tenant_id, run_id=run.run_id, expected_state=RunState.PROCESSING,
        new_state=RunState.FAILED, missing_sources=missing,
    ), event)


def _transition_to_publishing(
    control_plane: ControlPlanePort, run: Run, missing: tuple[str, ...], now: datetime
) -> Run:
    event = _build_event(run, "run.publishing", now)
    return control_plane.transition_run(TransitionRun(
        tenant_id=run.tenant_id, run_id=run.run_id, expected_state=RunState.PROCESSING,
        new_state=RunState.PUBLISHING, missing_sources=missing,
    ), event)


def _publish_now(
    dependencies: CoordinatorDependencies, run: Run, units: tuple[RunUnit, ...], now: datetime
) -> CoordinatorResult:
    control_plane = dependencies.control_plane
    pointer = control_plane.get_dataset_pointer(run.tenant_id, run.dataset_name)
    expected_version_id = None if pointer is None else pointer.version_id
    dependencies.publisher.publish(PublishRequest(
        run=run, units=units, expected_version_id=expected_version_id, now=now,
    ))
    published_run = cast("Run", control_plane.get_run(run.tenant_id, run.run_id))
    return CoordinatorResult(state=published_run.state, execution_ref=None, published=True)


def _cancel(dependencies: CoordinatorDependencies, run: Run, now: datetime) -> CoordinatorResult:
    dependencies.executor.cancel(CancelRunExecution(
        tenant_id=run.tenant_id, run_id=run.run_id, execution_ref=None,
    ))
    event = _build_event(run, "run.canceled", now)
    canceled = dependencies.control_plane.finalize_run_cancellation(FinalizeRunCancellation(
        tenant_id=run.tenant_id, run_id=run.run_id,
        expected_state=RunState.CANCEL_REQUESTED, canceled_at=now,
    ), event)
    return CoordinatorResult(state=canceled.state, execution_ref=None, published=False)


def _processing_result(outcome: str | DispatchDeferred | None) -> CoordinatorResult:
    if isinstance(outcome, DispatchDeferred):
        return CoordinatorResult(
            state=RunState.PROCESSING, execution_ref=None, published=False,
            deferred=outcome.reason,
        )
    return CoordinatorResult(state=RunState.PROCESSING, execution_ref=outcome, published=False)


class PipelineCoordinator:
    def __init__(
        self,
        dependencies: CoordinatorDependencies,
        execution: ExecutionPolicyConfig,
        mode_canary: ModeCanaryBinding | None = None,
    ) -> None:
        self._dependencies = dependencies
        self._execution = execution
        self._mode_canary = mode_canary
    def resume(self, tenant_id: str, run_id: str, *, settle: bool = True) -> CoordinatorResult:
        """Args: settle: False no after_persist, que roda dentro da própria execução.

        Returns: Estado do Run; com settle, o Run que termina liquida o permit dos dispatches.
        Raises: ValueError: Run inexistente ou em estado não suportado.
            SyntheticContextRequired: Namespace reservado fora do binding instalado.
        """
        require_context(self._mode_canary, tenant_id, run_id)
        run = self._dependencies.control_plane.get_run(tenant_id, run_id)
        if run is None:
            raise ValueError("run_not_found")
        if run.state in _TERMINAL_RUN_STATES:
            return CoordinatorResult(state=run.state, execution_ref=None, published=False)
        now = self._dependencies.clock()
        result = self._advance(run, now)
        if settle and result.state in _TERMINAL_RUN_STATES:
            self._settle_run(run, now)
        return result
    def _advance(self, run: Run, now: datetime) -> CoordinatorResult:
        if run.state is RunState.CANCEL_REQUESTED:
            return _cancel(self._dependencies, run, now)
        if run.state is RunState.PUBLISHING:
            units = self._dependencies.control_plane.list_run_units(run.tenant_id, run.run_id)
            return _publish_now(self._dependencies, run, units, now)
        if run.state is RunState.PROCESSING:
            return self._resume_processing(run, now)
        raise ValueError(f"unsupported_run_state:{run.state}")
    def _settle_run(self, run: Run, now: datetime) -> None:
        admission = self._execution.admission
        if admission is UNGATED_ADMISSION:
            return
        units = self._dependencies.control_plane.list_run_units(run.tenant_id, run.run_id)
        settle_run_dispatches(admission, units, now)
    def _resume_processing(self, run: Run, now: datetime) -> CoordinatorResult:
        control_plane = self._dependencies.control_plane
        units = control_plane.list_run_units(run.tenant_id, run.run_id)
        plan = RunPlan(
            run=run, units=units, missing_required=(), missing_optional=run.missing_sources,
            deployment_limit=self._execution.deployment_limit,
        )
        decision = decide_fan_in(plan)
        if decision.state is RunState.FAILED:
            failed = _fail_run(control_plane, run, decision.missing_sources, now)
            return CoordinatorResult(state=failed.state, execution_ref=None, published=False)
        if decision.state is RunState.PUBLISHING:
            publishing = _transition_to_publishing(
                control_plane, run, decision.missing_sources, now
            )
            return _publish_now(self._dependencies, publishing, units, now)
        outcome = _dispatch_protocol(
            control_plane, self._dependencies.executor, self._execution, plan, now
        )
        return _processing_result(outcome)

    def recover(self, limit: int = 100) -> tuple[CoordinatorResult, ...]:
        now = self._dependencies.clock()
        try:
            self._execution.admission.reconcile(now)
        except Exception as error:
            logger.exception("unit_permit_reconcile_failed")
            results, failed = self._recover_runs(now, limit)
            raise RecoveryFailed(results, failed, reconcile_failed=True) from error
        results, failed = self._recover_runs(now, limit)
        if failed:
            raise RecoveryFailed(results, failed)
        return results

    def _recover_runs(
        self, now: datetime, limit: int,
    ) -> tuple[tuple[CoordinatorResult, ...], int]:
        control_plane = self._dependencies.control_plane
        skipped: set[tuple[str, str]] = set()
        results: list[CoordinatorResult] = []
        failed = 0
        while len(results) < limit:
            candidates = _processor_recoverable_runs(
                control_plane, now, limit - len(results), skipped
            )
            if not candidates:
                break
            for run in candidates:
                skipped.add((run.tenant_id, run.run_id))
                try:
                    results.append(self.resume(run.tenant_id, run.run_id))
                except Exception:
                    failed += 1
                    logger.exception(
                        "recover_run_error tenant_id=%s run_id=%s",
                        run.tenant_id,
                        run.run_id,
                    )
        return tuple(results), failed


__all__ = [
    "CoordinatorDependencies",
    "CoordinatorResult",
    "PipelineCoordinator",
    "RecoveryFailed",
    "allow_execution",
    "noop_execution_started",
]
