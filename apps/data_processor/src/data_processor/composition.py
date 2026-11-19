"""Composição local do data_processor e exports dos helpers de fontes."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from cnes_domain.control_plane.entities import Tenant
from cnes_domain.ports.processing import ExecutionPolicyConfig
from cnes_domain.profiles import ProfileNotImplemented, RuntimeProfile
from cnes_infra.audit.local_sink import LocalAuditSink
from cnes_infra.billing import LOCAL_BILLING_SETTINGS, BillingSettings, build_execution_callbacks
from cnes_infra.billing.wiring import BillingGateResources
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.executor.local_pool import LocalWorkerPool
from cnes_infra.object_store import FilesystemObjectStore
from data_processor.orchestration.coordinator import (
    CoordinatorDependencies,
    PipelineCoordinator,
    noop_execution_started,
)
from data_processor.orchestration.publisher import DatasetPublisher
from data_processor.orchestration.unit_handler import RunUnitCommandHandler
from data_processor.orchestration.unit_worker import (
    UnitWorker,
    UnitWorkerDependencies,
    UnitWorkerPolicy,
)
from data_processor.pipeline.stage_processor import StageProcessor
from data_processor.processor_wiring import (
    UnsupportedSourceType,
    build_publication_policy,
    build_source_registry,
    normalize_cnes,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_domain.control_plane.entities import RunUnit
    from cnes_domain.ports.audit import AuditSinkPort
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.object_store import ObjectStorePort
    from cnes_domain.profiles import ProfileSettings
    from data_processor.pipeline.source_registry import SourceRegistry

_DEPLOYMENT_LIMIT = 4
_DISPATCH_LEASE_SECONDS = 300
_WORKER_OWNER = "data_processor"


@dataclass(frozen=True, slots=True)
class LocalProcessorRuntime:
    control_plane: ControlPlanePort
    object_store: ObjectStorePort
    audit_sink: AuditSinkPort
    executor: LocalWorkerPool
    publisher: DatasetPublisher
    source_registry: SourceRegistry
    stage_processor: StageProcessor
    coordinator: PipelineCoordinator
    unit_worker: UnitWorker
    unit_handler: RunUnitCommandHandler


def _seed_tenant(control_plane: ControlPlanePort, settings: ProfileSettings, now: datetime) -> None:
    control_plane.put_tenant(Tenant(
        tenant_id=cast("str", settings.tenant_id), municipality_name=f"tenant-{settings.tenant_id}",
        created_at=now,
    ))


def build_local_processor_runtime(
    settings: ProfileSettings, clock: Callable[[], datetime],
    billing: BillingSettings = LOCAL_BILLING_SETTINGS,
) -> LocalProcessorRuntime:
    if settings.profile is RuntimeProfile.AWS:
        raise ProfileNotImplemented("aws_runtime_plan_required")
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.state_db.parent.mkdir(parents=True, exist_ok=True)
    control_plane = SQLiteControlPlane(settings.state_db, clock)
    control_plane.initialize()
    _seed_tenant(control_plane, settings, clock())
    settings.objects_dir.mkdir(parents=True, exist_ok=True)
    object_store = FilesystemObjectStore(settings.objects_dir)
    audit_sink = LocalAuditSink(settings.data_dir)
    source_registry = build_source_registry()
    stage_processor = StageProcessor(control_plane, object_store, source_registry, clock)
    resources = BillingGateResources(clock, _DEPLOYMENT_LIMIT)
    policy = build_publication_policy(billing, control_plane, resources, clock)
    publisher = DatasetPublisher(
        store=object_store, control_plane=control_plane, publication_policy=policy,
    )
    callbacks = build_execution_callbacks(billing, control_plane, resources, noop_execution_started)
    execution = ExecutionPolicyConfig(_DEPLOYMENT_LIMIT, _DISPATCH_LEASE_SECONDS, callbacks)
    # LocalWorkerPool requires the handler in its constructor, but the handler exists only
    # after UnitWorker (which depends on the coordinator for after_persist). The lambda
    # closes over `unit_handler`, assigned later in this scope; LocalWorkerPool invokes
    # it only from `.start()`, which runs after this function returns.
    executor = LocalWorkerPool(
        handler=lambda message: unit_handler.handle(message),
        owner=_WORKER_OWNER, clock=clock, lease_seconds=_DISPATCH_LEASE_SECONDS,
    )
    coordinator = PipelineCoordinator(
        CoordinatorDependencies(
            control_plane=control_plane, executor=executor, publisher=publisher, clock=clock,
        ),
        execution,
    )
    def _after_persist(unit: RunUnit) -> None:
        coordinator.resume(unit.tenant_id, unit.run_id, settle=False)
    unit_worker = UnitWorker(
        UnitWorkerDependencies(
            control_plane=control_plane, store=object_store, processor=stage_processor,
            clock=clock,
        ),
        UnitWorkerPolicy(after_persist=_after_persist),
    )
    unit_handler = RunUnitCommandHandler(unit_worker)
    return LocalProcessorRuntime(
        control_plane=control_plane, object_store=object_store, audit_sink=audit_sink,
        executor=executor, publisher=publisher, source_registry=source_registry,
        stage_processor=stage_processor,
        coordinator=coordinator, unit_worker=unit_worker, unit_handler=unit_handler,
    )


__all__ = [
    "LocalProcessorRuntime", "UnsupportedSourceType", "build_local_processor_runtime",
    "build_source_registry", "normalize_cnes",
]
