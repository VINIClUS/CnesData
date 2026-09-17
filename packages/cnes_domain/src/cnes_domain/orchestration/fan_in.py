"""Fan-in decision: aggregate RunUnit state into a Run-level outcome."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from cnes_domain.control_plane.enums import RunStage, RunState, RunUnitState

if TYPE_CHECKING:
    from cnes_domain.control_plane.entities import RunUnit
    from cnes_domain.orchestration.planner import RunPlan


@dataclass(frozen=True, slots=True)
class FanInDecision:
    state: RunState
    missing_sources: tuple[str, ...]
    publish_ready: bool


def _dependency_key(unit: RunUnit) -> str:
    return f"{unit.source_type}/{unit.file_subtype}"


def _missing_sources(plan: RunPlan) -> tuple[str, ...]:
    degraded = {
        _dependency_key(unit)
        for unit in plan.units
        if unit.state is RunUnitState.SUCCEEDED_DEGRADED
    }
    return tuple(sorted(set(plan.missing_optional) | degraded))


def _has_failed(plan: RunPlan) -> bool:
    required = {
        (dep.source_type, dep.file_subtype) for dep in plan.run.dependencies if dep.required
    }
    for unit in plan.units:
        if unit.state is not RunUnitState.FAILED_FINAL:
            continue
        if unit.stage is not RunStage.NORMALIZE:
            return True
        if (unit.source_type, unit.file_subtype) in required:
            return True
    return False


def _materialize_succeeded(plan: RunPlan) -> bool:
    materialize = next(unit for unit in plan.units if unit.stage is RunStage.MATERIALIZE)
    return materialize.state is RunUnitState.SUCCEEDED


def decide_fan_in(plan: RunPlan) -> FanInDecision:
    missing = _missing_sources(plan)
    if plan.missing_required:
        return FanInDecision(RunState.WAITING_INPUTS, missing, False)
    if _has_failed(plan):
        return FanInDecision(RunState.FAILED, missing, False)
    if _materialize_succeeded(plan):
        return FanInDecision(RunState.PUBLISHING, missing, True)
    return FanInDecision(RunState.PROCESSING, missing, False)
