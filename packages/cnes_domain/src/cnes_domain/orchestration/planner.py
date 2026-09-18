"""Deterministic three-stage DAG planning: fan-out and dispatch requests."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import TYPE_CHECKING

from cnes_domain.control_plane.entities import ManifestRef, RunUnit
from cnes_domain.control_plane.enums import DispatchState, RunStage, RunUnitState
from cnes_domain.control_plane.ids import RunUnitIdentity, unit_id
from cnes_domain.ports.processing import StartRunExecution

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.control_plane.entities import Run, RunDependency, RunDispatch

_SEPARATOR = "\x1f"
_MAX_UNITS = 20
_CANDIDATE_STATES = {
    RunUnitState.PENDING,
    RunUnitState.FAILED_RETRYABLE,
    RunUnitState.LEASED,
}
_SATISFIED_PREDECESSOR_STATES = {RunUnitState.SUCCEEDED, RunUnitState.SUCCEEDED_DEGRADED}
_ACTIVE_DISPATCH_STATES = {DispatchState.RESERVED, DispatchState.STARTED}


@dataclass(frozen=True, slots=True)
class RawManifestRef:
    manifest_id: str
    manifest_key: str
    source_type: str
    file_subtype: str
    partition: str


@dataclass(frozen=True, slots=True)
class PlanRequest:
    run: Run
    manifests: tuple[RawManifestRef, ...]
    deployment_limit: int


@dataclass(frozen=True, slots=True)
class RunPlan:
    run: Run
    units: tuple[RunUnit, ...]
    missing_required: tuple[str, ...]
    missing_optional: tuple[str, ...]
    deployment_limit: int


def _dependency_key(source_type: str, file_subtype: str) -> str:
    return f"{source_type}/{file_subtype}"


def _group_manifests(
    manifests: tuple[RawManifestRef, ...],
    dependencies: tuple[RunDependency, ...],
) -> dict[tuple[str, str], tuple[RawManifestRef, ...]]:
    declared = {(dep.source_type, dep.file_subtype) for dep in dependencies}
    seen_ids: set[str] = set()
    seen_keys: set[str] = set()
    partitions: dict[tuple[str, str], str] = {}
    groups: dict[tuple[str, str], list[RawManifestRef]] = {}
    for manifest in manifests:
        key = (manifest.source_type, manifest.file_subtype)
        if key not in declared:
            raise ValueError("undeclared_dependency")
        if manifest.manifest_id in seen_ids or manifest.manifest_key in seen_keys:
            raise ValueError("duplicate_manifest_ref")
        if key in partitions and partitions[key] != manifest.partition:
            raise ValueError("mixed_partition_chain")
        seen_ids.add(manifest.manifest_id)
        seen_keys.add(manifest.manifest_key)
        partitions[key] = manifest.partition
        groups.setdefault(key, []).append(manifest)
    return {key: tuple(refs) for key, refs in groups.items()}


def _new_unit(
    run: Run,
    identity: RunUnitIdentity,
    depends_on_unit_ids: tuple[str, ...],
    input_manifests: tuple[ManifestRef, ...],
) -> RunUnit:
    is_normalize = identity.stage is RunStage.NORMALIZE
    return RunUnit(
        tenant_id=run.tenant_id,
        run_id=run.run_id,
        unit_id=unit_id(identity),
        stage=identity.stage,
        source_type=identity.source_type if is_normalize else None,
        file_subtype=identity.file_subtype if is_normalize else None,
        partition=identity.partition,
        depends_on_unit_ids=depends_on_unit_ids,
        input_manifests=input_manifests,
        state=RunUnitState.PENDING,
        attempt=0,
        fencing_token=0,
        lease_owner=None,
        lease_until=None,
        dispatch_id=None,
        output_manifests=(),
        error_code=None,
    )


def _normalize_unit(run: Run, key: tuple[str, str], refs: tuple[RawManifestRef, ...]) -> RunUnit:
    source_type, file_subtype = key
    identity = RunUnitIdentity(
        run.run_id, RunStage.NORMALIZE, source_type, file_subtype, refs[0].partition
    )
    input_manifests = tuple(
        ManifestRef(manifest_id=ref.manifest_id, manifest_key=ref.manifest_key)
        for ref in sorted(refs, key=lambda ref: ref.manifest_id)
    )
    return _new_unit(run, identity, (), input_manifests)


def _downstream_unit(run: Run, stage: RunStage, depends_on_unit_ids: tuple[str, ...]) -> RunUnit:
    return _new_unit(run, RunUnitIdentity(run.run_id, stage), depends_on_unit_ids, ())


def _build_units(
    run: Run, groups: dict[tuple[str, str], tuple[RawManifestRef, ...]]
) -> tuple[RunUnit, ...]:
    if not groups:
        raise ValueError("empty_normalize_set")
    if len(groups) + 2 > _MAX_UNITS:
        raise ValueError("unit_budget_exceeded")
    ordered = sorted(
        groups.items(), key=lambda item: (item[0][0], item[0][1], item[1][0].partition)
    )
    normalize_units = tuple(_normalize_unit(run, key, refs) for key, refs in ordered)
    reconcile = _downstream_unit(
        run, RunStage.RECONCILE, tuple(unit.unit_id for unit in normalize_units)
    )
    materialize = _downstream_unit(run, RunStage.MATERIALIZE, (reconcile.unit_id,))
    return (*normalize_units, reconcile, materialize)


def plan_run(request: PlanRequest) -> RunPlan:
    run = request.run
    groups = _group_manifests(request.manifests, run.dependencies)
    required = {(dep.source_type, dep.file_subtype) for dep in run.dependencies if dep.required}
    optional = {
        (dep.source_type, dep.file_subtype) for dep in run.dependencies if not dep.required
    }
    missing_required = tuple(sorted(_dependency_key(*key) for key in required - groups.keys()))
    if missing_required:
        return RunPlan(run, (), missing_required, (), request.deployment_limit)
    missing_optional = tuple(sorted(_dependency_key(*key) for key in optional - groups.keys()))
    units = _build_units(run, groups)
    return RunPlan(run, units, (), missing_optional, request.deployment_limit)


def _lease_is_live(unit: RunUnit, now: datetime) -> bool:
    return (
        unit.state is RunUnitState.LEASED
        and unit.lease_until is not None
        and unit.lease_until > now
    )


def _is_ready(unit: RunUnit, by_id: dict[str, RunUnit]) -> bool:
    if unit.state not in _CANDIDATE_STATES:
        return False
    return all(
        by_id[dependency_id].state in _SATISFIED_PREDECESSOR_STATES
        for dependency_id in unit.depends_on_unit_ids
    )


def ready_units(plan: RunPlan, now: datetime) -> tuple[RunUnit, ...]:
    if any(_lease_is_live(unit, now) for unit in plan.units):
        return ()
    by_id = {unit.unit_id: unit for unit in plan.units}
    return tuple(unit for unit in plan.units if _is_ready(unit, by_id))


def logical_wave_id(units: tuple[RunUnit, ...]) -> str:
    if not units:
        raise ValueError("empty_wave")
    ordered_ids = sorted(unit.unit_id for unit in units)
    digest = sha256(_SEPARATOR.join(ordered_ids).encode()).hexdigest()
    return digest[:16]


def _validate_dispatch(plan: RunPlan, dispatch: RunDispatch) -> None:
    if dispatch.state not in _ACTIVE_DISPATCH_STATES:
        raise ValueError("invalid_dispatch_state")
    if (dispatch.tenant_id, dispatch.run_id) != (plan.run.tenant_id, plan.run.run_id):
        raise ValueError("dispatch_identity_mismatch")
    by_id = {unit.unit_id: unit for unit in plan.units}
    if any(dispatch_unit_id not in by_id for dispatch_unit_id in dispatch.unit_ids):
        raise ValueError("dispatch_unit_missing")
    dispatch_units = tuple(by_id[dispatch_unit_id] for dispatch_unit_id in dispatch.unit_ids)
    if logical_wave_id(dispatch_units) != dispatch.wave_id:
        raise ValueError("wave_mismatch")


def execution_request(
    plan: RunPlan, dispatch: RunDispatch, max_concurrency: int
) -> StartRunExecution:
    _validate_dispatch(plan, dispatch)
    if plan.deployment_limit < 1 or max_concurrency < 1:
        raise ValueError("positive_limit_required")
    concurrency = min(len(dispatch.unit_ids), plan.deployment_limit, max_concurrency)
    return StartRunExecution(
        tenant_id=dispatch.tenant_id,
        run_id=dispatch.run_id,
        wave_id=dispatch.wave_id,
        dispatch_id=dispatch.dispatch_id,
        unit_ids=dispatch.unit_ids,
        max_concurrency=concurrency,
    )
