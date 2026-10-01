"""Composition roots for the data_processor worker: local and aws profiles."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from cnes_contracts.manifests.raw import SourceType
from cnes_domain.billing.publication import (
    BillingPublicationPolicy,
    PublicationPolicyDependencies,
)
from cnes_domain.control_plane.entities import Tenant
from cnes_domain.orchestration.source_catalog import build_source_catalog
from cnes_domain.ports.processing import ExecutionPolicyConfig
from cnes_domain.profiles import ProfileNotImplemented, RuntimeProfile, parse_profile
from cnes_infra.audit.local_sink import LocalAuditSink
from cnes_infra.aws import AwsRuntimeSettings, build_aws_runtime, create_aws_clients
from cnes_infra.billing import LOCAL_BILLING_SETTINGS, BillingSettings, build_execution_callbacks
from cnes_infra.billing.wiring import BillingGateResources, build_entitlement_gate
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.executor.local_pool import LocalWorkerPool
from cnes_infra.executor.step_functions import StepFunctionsExecutor, validate_state_machine
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
from data_processor.pipeline.materialize_cnes import materialize_cnes
from data_processor.pipeline.normalize_cnes_local import normalize_cnes_local
from data_processor.pipeline.normalize_cnes_nacional import normalize_cnes_nacional
from data_processor.pipeline.reconcile_cnes import reconcile_cnes
from data_processor.pipeline.source_registry import SourcePipeline, SourceRegistry
from data_processor.pipeline.stage_processor import StageProcessor
from data_processor.recovery import ProcessorRecovery
from data_processor.sources.bpa.normalize import normalize_bpa
from data_processor.sources.bpa.reconcile import reconcile_bpa
from data_processor.sources.bpa.serving import materialize_bpa
from data_processor.sources.sia.normalize import normalize_sia
from data_processor.sources.sia.reconcile import reconcile_sia
from data_processor.sources.sia.serving import materialize_sia
from data_processor.sources.sihd.normalize import normalize_sihd
from data_processor.sources.sihd.reconcile import reconcile_sihd
from data_processor.sources.sihd.serving import materialize_sihd

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from boto3.session import Session

    from cnes_contracts.manifests.processing import NormalizeRequest, NormalizeResult
    from cnes_domain.billing.ports import ClockPort
    from cnes_domain.control_plane.entities import RunUnit
    from cnes_domain.orchestration.source_catalog import SourceCatalog
    from cnes_domain.ports.audit import AuditSinkPort
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.object_store import ObjectStorePort
    from cnes_domain.ports.processing import (
        ExecutionCallbacks,
        ExecutionStarted,
        ProcessorExecutorPort,
    )
    from cnes_domain.profiles import ProfileSettings
    from cnes_infra.aws import AwsClients, AwsRuntimeComponents

_DEPLOYMENT_LIMIT = 4
_DISPATCH_LEASE_SECONDS = 300
_WORKER_OWNER = "data_processor"


class UnsupportedSourceType(ValueError):
    pass


def _utc_now() -> datetime:
    return datetime.now(UTC)


def normalize_cnes(request: NormalizeRequest, store: ObjectStorePort) -> NormalizeResult:
    if request.source_type is SourceType.CNES_LOCAL:
        return normalize_cnes_local(request, store)
    if request.source_type is SourceType.CNES_NACIONAL:
        return normalize_cnes_nacional(request, store)
    raise UnsupportedSourceType(request.source_type)


def build_source_registry(catalog: SourceCatalog | None = None) -> SourceRegistry:
    resolved = catalog if catalog is not None else build_source_catalog()
    bundles = (
        SourcePipeline(
            definition=resolved.for_pipeline("cnes"), normalize=normalize_cnes,
            reconcile=reconcile_cnes, materialize=materialize_cnes,
        ),
        SourcePipeline(
            definition=resolved.for_pipeline("sihd"), normalize=normalize_sihd,
            reconcile=reconcile_sihd, materialize=materialize_sihd,
        ),
        SourcePipeline(
            definition=resolved.for_pipeline("bpa"), normalize=normalize_bpa,
            reconcile=reconcile_bpa, materialize=materialize_bpa,
        ),
        SourcePipeline(
            definition=resolved.for_pipeline("sia"), normalize=normalize_sia,
            reconcile=reconcile_sia, materialize=materialize_sia,
        ),
    )
    return SourceRegistry(resolved, bundles)


@dataclass(frozen=True, slots=True)
class LocalProcessorRuntime:
    control_plane: ControlPlanePort
    object_store: ObjectStorePort
    audit_sink: AuditSinkPort
    executor: ProcessorExecutorPort
    publisher: DatasetPublisher
    source_registry: SourceRegistry
    stage_processor: StageProcessor
    coordinator: PipelineCoordinator
    unit_worker: UnitWorker
    unit_handler: RunUnitCommandHandler


@dataclass(frozen=True, slots=True)
class AwsProcessorServices:
    recovery: ProcessorRecovery
    recovery_batch_size: int


@dataclass(frozen=True, slots=True)
class ProcessorRuntimeComponents:
    control_plane: ControlPlanePort
    object_store: ObjectStorePort
    executor: ProcessorExecutorPort
    publisher: DatasetPublisher
    source_registry: SourceRegistry
    stage_processor: StageProcessor
    coordinator: PipelineCoordinator
    unit_worker: UnitWorker
    unit_handler: RunUnitCommandHandler
    services: AwsProcessorServices | None

    @classmethod
    def from_local(cls, runtime: LocalProcessorRuntime) -> ProcessorRuntimeComponents:
        return cls(
            control_plane=runtime.control_plane, object_store=runtime.object_store,
            executor=runtime.executor, publisher=runtime.publisher,
            source_registry=runtime.source_registry, stage_processor=runtime.stage_processor,
            coordinator=runtime.coordinator, unit_worker=runtime.unit_worker,
            unit_handler=runtime.unit_handler, services=None,
        )


@dataclass(frozen=True, slots=True)
class _ProcessorBilling:
    callbacks: ExecutionCallbacks
    policy: BillingPublicationPolicy


def _publication_policy(
    billing: BillingSettings, control_plane: ControlPlanePort,
    resources: BillingGateResources, clock: ClockPort,
) -> BillingPublicationPolicy:
    return BillingPublicationPolicy(PublicationPolicyDependencies(
        control_plane, build_entitlement_gate(billing, resources), clock,
        billing.execution_mode,
    ))


def _seed_tenant(control_plane: ControlPlanePort, settings: ProfileSettings, now: datetime) -> None:
    control_plane.put_tenant(Tenant(
        tenant_id=settings.tenant_id, municipality_name=f"tenant-{settings.tenant_id}",
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
    policy = _publication_policy(billing, control_plane, resources, clock)
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
        coordinator.resume(unit.tenant_id, unit.run_id)

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
        executor=executor,
        publisher=publisher, source_registry=source_registry, stage_processor=stage_processor,
        coordinator=coordinator, unit_worker=unit_worker, unit_handler=unit_handler,
    )


def build_processor_runtime(
    profile: str, values: Mapping[str, str], session: Session,
    execution_started: ExecutionStarted = noop_execution_started,
) -> ProcessorRuntimeComponents:
    """Args: profile local|aws; values: ambiente; session: boto3; execution_started: callback.
    Returns: Componentes canônicos do processor para o profile pedido.
    Raises: ValueError: profile desconhecido ou configuração aws inválida.
    """
    if profile == RuntimeProfile.LOCAL:
        billing = BillingSettings.from_mapping(values)
        local = build_local_processor_runtime(parse_profile(values), _utc_now, billing)
        return ProcessorRuntimeComponents.from_local(local)
    if profile != RuntimeProfile.AWS:
        raise ValueError("profile=unknown")
    settings = AwsRuntimeSettings.from_mapping(values)
    clients = create_aws_clients(settings, session)
    billing = BillingSettings.from_mapping(values)
    core = build_aws_runtime(settings, clients, _utc_now, billing)
    resources = BillingGateResources(
        _utc_now, settings.processor_max_concurrency, clients.dynamodb,
        settings.control_plane_table,
    )
    callbacks = build_execution_callbacks(
        billing, core.control_plane, resources, execution_started,
    )
    policy = _publication_policy(billing, core.control_plane, resources, _utc_now)
    return _build_aws_processor_runtime(
        settings, clients, core, _ProcessorBilling(callbacks, policy),
    )


def _build_aws_processor_runtime(
    settings: AwsRuntimeSettings, clients: AwsClients,
    core: AwsRuntimeComponents, billing: _ProcessorBilling,
) -> ProcessorRuntimeComponents:
    _validate_runtime(settings, clients)
    executor = StepFunctionsExecutor(clients.step_functions, settings.state_machine_arn)
    publisher = DatasetPublisher(
        store=core.object_store, control_plane=core.control_plane,
        publication_policy=billing.policy,
    )
    source_registry = build_source_registry(build_source_catalog())
    stage_processor = StageProcessor(
        core.control_plane, core.object_store, source_registry, _utc_now,
    )
    coordinator = PipelineCoordinator(
        CoordinatorDependencies(
            control_plane=core.control_plane, executor=executor, publisher=publisher,
            clock=_utc_now,
        ),
        ExecutionPolicyConfig(
            settings.processor_max_concurrency, settings.processor_lease_seconds,
            billing.callbacks,
        ),
    )
    unit_worker = UnitWorker(
        UnitWorkerDependencies(
            control_plane=core.control_plane, store=core.object_store,
            processor=stage_processor, clock=_utc_now,
        ),
        UnitWorkerPolicy(
            after_persist=lambda unit: coordinator.resume(unit.tenant_id, unit.run_id),
        ),
    )
    return ProcessorRuntimeComponents(
        control_plane=core.control_plane, object_store=core.object_store, executor=executor,
        publisher=publisher, source_registry=source_registry, stage_processor=stage_processor,
        coordinator=coordinator, unit_worker=unit_worker,
        unit_handler=RunUnitCommandHandler(unit_worker),
        services=AwsProcessorServices(
            recovery=ProcessorRecovery(core.control_plane, coordinator, _utc_now),
            recovery_batch_size=settings.processor_recovery_batch_size,
        ),
    )


def _validate_runtime(settings: AwsRuntimeSettings, clients: AwsClients) -> None:
    validate_state_machine(
        clients.step_functions, settings.state_machine_arn,
        settings.processor_container_name, settings.processor_lease_seconds,
    )


__all__ = [
    "AwsProcessorServices",
    "LocalProcessorRuntime",
    "ProcessorRuntimeComponents",
    "UnsupportedSourceType",
    "build_local_processor_runtime",
    "build_processor_runtime",
    "build_source_registry",
    "normalize_cnes",
]
