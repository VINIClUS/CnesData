"""Local (SQLite + filesystem) composition root for central_api."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from central_api.services.delta_policy import DeltaPolicy
from central_api.services.raw_ingestion import RawIngestionService
from central_api.services.run_planning import RunPlanningDependencies, RunPlanningService
from cnes_domain.control_plane.entities import Tenant
from cnes_domain.orchestration.source_catalog import build_source_catalog
from cnes_domain.ports.processing import ExecutionCallbacks, ExecutionPermit, ExecutionPolicyConfig
from cnes_domain.profiles import ProfileNotImplemented, RuntimeProfile
from cnes_infra.audit.local_sink import LocalAuditSink
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.executor.local_pool import LocalWorkerPool
from cnes_infra.object_store import FilesystemObjectStore

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_domain.control_plane.entities import Run, RunDispatch
    from cnes_domain.orchestration.source_catalog import SourceCatalog
    from cnes_domain.ports.audit import AuditSinkPort
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.object_store import ObjectStorePort
    from cnes_domain.ports.processing import (
        ProcessorExecutorPort,
        StartRunExecution,
    )
    from cnes_domain.profiles import ProfileSettings

_DEPLOYMENT_LIMIT = 4
_DISPATCH_LEASE_SECONDS = 300
_WORKER_OWNER = "central_api"


def _local_execution_policy(
    run: Run, dispatch: RunDispatch, requested_limit: int
) -> ExecutionPermit:
    del dispatch
    return ExecutionPermit(
        tenant_id=run.tenant_id, run_id=run.run_id, max_concurrency=requested_limit,
        policy_version=0, fencing_token=0, binding_context=None,
    )


def _local_execution_started(
    run: Run, request: StartRunExecution, execution_ref: str, permit: ExecutionPermit
) -> None:
    del run, request, execution_ref, permit


def _unit_execution_forbidden(message: object) -> None:
    # central_api never executes units: a fabricated None/RunUnit handler would silently
    # mark work as CANCELED/SUCCEEDED through LocalWorkerPool.status. Raising keeps the
    # dispatch FAILED until CND-064 injects the real handler.
    del message
    raise NotImplementedError("processor_owns_unit_execution")


@dataclass(frozen=True, slots=True)
class LocalRuntime:
    control_plane: ControlPlanePort
    object_store: ObjectStorePort
    executor: ProcessorExecutorPort
    audit_sink: AuditSinkPort
    raw_ingestion: RawIngestionService
    source_catalog: SourceCatalog
    run_planning: RunPlanningService


def _seed_tenant(control_plane: ControlPlanePort, settings: ProfileSettings, now: datetime) -> None:
    control_plane.put_tenant(Tenant(
        tenant_id=settings.tenant_id, municipality_name=f"tenant-{settings.tenant_id}",
        created_at=now,
    ))


def build_local_runtime(settings: ProfileSettings, clock: Callable[[], datetime]) -> LocalRuntime:
    if settings.profile is RuntimeProfile.AWS:
        raise ProfileNotImplemented("aws_runtime_plan_required")
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    state_dir = settings.data_dir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    control_plane = SQLiteControlPlane(state_dir / "cnesdata.sqlite3", clock)
    control_plane.initialize()
    _seed_tenant(control_plane, settings, clock())
    objects_root = settings.data_dir / "objects"
    objects_root.mkdir(parents=True, exist_ok=True)
    object_store = FilesystemObjectStore(objects_root)
    audit_sink = LocalAuditSink(settings.data_dir)
    executor = LocalWorkerPool(
        handler=_unit_execution_forbidden, owner=_WORKER_OWNER, clock=clock,
        lease_seconds=_DISPATCH_LEASE_SECONDS,
    )
    source_catalog = build_source_catalog()
    execution = ExecutionPolicyConfig(
        _DEPLOYMENT_LIMIT, _DISPATCH_LEASE_SECONDS,
        ExecutionCallbacks(_local_execution_policy, _local_execution_started),
    )
    run_planning = RunPlanningService(
        RunPlanningDependencies(
            control_plane=control_plane, object_store=object_store, executor=executor,
            source_catalog=source_catalog,
        ),
        execution, clock, dispatch_enabled=False,
    )
    raw_ingestion = RawIngestionService(
        control_plane, object_store, DeltaPolicy(),
        accepted_manifest=run_planning.on_raw_manifest_accepted,
    )
    return LocalRuntime(
        control_plane=control_plane, object_store=object_store, executor=executor,
        audit_sink=audit_sink, raw_ingestion=raw_ingestion, source_catalog=source_catalog,
        run_planning=run_planning,
    )


__all__ = ["LocalRuntime", "build_local_runtime"]
