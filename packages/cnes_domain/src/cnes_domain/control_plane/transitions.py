"""Legal control-plane state transitions."""

from cnes_domain.control_plane.entities import Job, Run, RunUnit
from cnes_domain.control_plane.enums import JobState, RunStage, RunState, RunUnitState
from cnes_domain.control_plane.errors import InvalidTransition

_JOB_TRANSITIONS = {
    JobState.PENDING: {JobState.LEASED},
    JobState.LEASED: {
        JobState.SUCCEEDED,
        JobState.FAILED_RETRYABLE,
        JobState.FAILED_FINAL,
        JobState.CANCEL_REQUESTED,
    },
    JobState.FAILED_RETRYABLE: {JobState.LEASED},
    JobState.CANCEL_REQUESTED: {JobState.CANCELED},
}

_RUN_TRANSITIONS = {
    RunState.PLANNED: {
        RunState.WAITING_INPUTS,
        RunState.PROCESSING,
        RunState.CANCEL_REQUESTED,
    },
    RunState.WAITING_INPUTS: {
        RunState.PROCESSING,
        RunState.FAILED,
        RunState.CANCEL_REQUESTED,
    },
    RunState.PROCESSING: {
        RunState.PUBLISHING,
        RunState.FAILED,
        RunState.CANCEL_REQUESTED,
    },
    RunState.PUBLISHING: {
        RunState.PUBLISHED,
        RunState.PUBLISHED_DEGRADED,
        RunState.FAILED,
    },
    RunState.CANCEL_REQUESTED: {RunState.CANCELED},
}

_UNIT_TRANSITIONS = {
    RunUnitState.PENDING: {RunUnitState.LEASED},
    RunUnitState.LEASED: {
        RunUnitState.SUCCEEDED,
        RunUnitState.SUCCEEDED_DEGRADED,
        RunUnitState.FAILED_RETRYABLE,
        RunUnitState.FAILED_FINAL,
    },
    RunUnitState.FAILED_RETRYABLE: {RunUnitState.LEASED},
}

_UNIT_NONTERMINAL = {
    RunUnitState.PENDING,
    RunUnitState.LEASED,
    RunUnitState.FAILED_RETRYABLE,
}


def _transition_error(old_state: object, new_state: object) -> InvalidTransition:
    return InvalidTransition(f"transition={old_state}->{new_state}")


def transition_job(job: Job, new_state: JobState) -> Job:
    if new_state not in _JOB_TRANSITIONS.get(job.state, set()):
        raise _transition_error(job.state.value, new_state.value)
    return Job.model_validate(job.model_dump() | {"state": new_state})


def transition_run(run: Run, new_state: RunState) -> Run:
    if new_state not in _RUN_TRANSITIONS.get(run.state, set()):
        raise _transition_error(run.state.value, new_state.value)
    return Run.model_validate(run.model_dump() | {"state": new_state})


def _validate_parent(unit: RunUnit, parent_run: Run | None) -> Run:
    if parent_run is None:
        raise InvalidTransition("parent_run_required")
    if (unit.tenant_id, unit.run_id) != (parent_run.tenant_id, parent_run.run_id):
        raise InvalidTransition("parent_run_mismatch")
    return parent_run


def _validate_degradation(unit: RunUnit, parent_run: Run | None) -> None:
    parent = _validate_parent(unit, parent_run)
    if unit.stage is not RunStage.NORMALIZE:
        raise InvalidTransition("degraded_normalize_required")
    if not unit.error_code:
        raise InvalidTransition("degraded_error_required")
    if unit.output_manifests:
        raise InvalidTransition("degraded_outputs_forbidden")
    matching = (
        dependency
        for dependency in parent.dependencies
        if (dependency.source_type, dependency.file_subtype)
        == (unit.source_type, unit.file_subtype)
    )
    dependency = next(matching, None)
    if dependency is None or dependency.required:
        raise InvalidTransition("optional_dependency_required")


def _validate_cancellation(unit: RunUnit, parent_run: Run | None) -> None:
    parent = _validate_parent(unit, parent_run)
    if parent.state is not RunState.CANCEL_REQUESTED:
        raise InvalidTransition("parent_run_not_canceling")


def transition_run_unit(
    unit: RunUnit,
    new_state: RunUnitState,
    parent_run: Run | None = None,
) -> RunUnit:
    if new_state is RunUnitState.CANCELED and unit.state in _UNIT_NONTERMINAL:
        _validate_cancellation(unit, parent_run)
    elif new_state in _UNIT_TRANSITIONS.get(unit.state, set()):
        if new_state is RunUnitState.SUCCEEDED_DEGRADED:
            _validate_degradation(unit, parent_run)
    else:
        raise _transition_error(unit.state.value, new_state.value)
    return RunUnit.model_validate(unit.model_dump() | {"state": new_state})
