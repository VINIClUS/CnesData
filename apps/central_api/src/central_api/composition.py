"""Composition roots for central_api: local (SQLite + filesystem) and aws profiles."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from typing import TYPE_CHECKING

from central_api.auth.aws_oidc import MembershipAuthorizer
from central_api.services.billing_gates import ApiBillingGates, TenantAccountResolver
from central_api.services.delta_policy import DeltaPolicy
from central_api.services.raw_ingestion import RawIngestionService
from central_api.services.run_authorization import RunAuthorizationService
from central_api.services.run_planning import RunPlanningDependencies, RunPlanningService
from central_api.services.serving_access import LocalServingAccess
from central_api.services.serving_entitlement import EntitledServingAccess
from central_api.serving.aws_signed import S3SignedServingAccess, SignedServingSettings
from cnes_domain.control_plane.entities import Tenant
from cnes_domain.orchestration.source_catalog import build_source_catalog
from cnes_domain.ports.processing import ExecutionPolicyConfig
from cnes_domain.profiles import ProfileNotImplemented, RuntimeProfile, parse_profile
from cnes_infra.audit.local_sink import LocalAuditSink
from cnes_infra.auth.dynamodb_memberships import DynamoDBMembershipCandidates
from cnes_infra.aws import AwsRuntimeSettings, build_aws_runtime, create_aws_clients
from cnes_infra.billing import (
    LOCAL_BILLING_SETTINGS,
    BillingGateResources,
    BillingSettings,
    BillingStorage,
    build_billing_enforcement,
    build_execution_callbacks,
)
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.executor.local_pool import LocalWorkerPool
from cnes_infra.executor.step_functions import StepFunctionsExecutor, validate_state_machine
from cnes_infra.object_store import FilesystemObjectStore

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from boto3.session import Session

    from cnes_domain.control_plane.entities import RawManifestRecord, Run
    from cnes_domain.orchestration.source_catalog import SourceCatalog
    from cnes_domain.ports.audit import AuditSinkPort
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.object_store import ObjectStorePort
    from cnes_domain.ports.processing import (
        ExecutionPermit,
        ExecutionStarted,
        ProcessorExecutorPort,
        StartRunExecution,
    )
    from cnes_domain.profiles import ProfileSettings
    from cnes_infra.aws import AwsClients, AwsRuntimeComponents

_DEPLOYMENT_LIMIT = 4
_DISPATCH_LEASE_SECONDS = 300
_WORKER_OWNER = "central_api"


def _utc_now() -> datetime:
    return datetime.now(UTC)


def noop_execution_started(
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
    run_authorization: RunAuthorizationService | None = None
    billing_gates: ApiBillingGates | None = None


@dataclass(frozen=True, slots=True)
class _AwsBilling:
    settings: BillingSettings
    execution_started: ExecutionStarted


@dataclass(frozen=True, slots=True)
class AwsApiServices:
    membership_authorizer: MembershipAuthorizer
    serving_access: S3SignedServingAccess
    billing_storage: BillingStorage | None = None


@dataclass(frozen=True, slots=True)
class RuntimeComponents:
    control_plane: ControlPlanePort
    object_store: ObjectStorePort
    executor: ProcessorExecutorPort
    audit_sink: AuditSinkPort
    raw_ingestion: RawIngestionService
    source_catalog: SourceCatalog
    run_planning: RunPlanningService
    services: AwsApiServices | None
    run_authorization: RunAuthorizationService | None = None
    billing_gates: ApiBillingGates | None = None

    @classmethod
    def from_local(cls, runtime: LocalRuntime) -> RuntimeComponents:
        return cls(
            control_plane=runtime.control_plane, object_store=runtime.object_store,
            executor=runtime.executor, audit_sink=runtime.audit_sink,
            raw_ingestion=runtime.raw_ingestion, source_catalog=runtime.source_catalog,
            run_planning=runtime.run_planning, services=None,
            run_authorization=runtime.run_authorization,
            billing_gates=runtime.billing_gates,
        )


def _seed_tenant(control_plane: ControlPlanePort, settings: ProfileSettings, now: datetime) -> None:
    control_plane.put_tenant(Tenant(
        tenant_id=settings.tenant_id, municipality_name=f"tenant-{settings.tenant_id}",
        created_at=now,
    ))


def build_local_runtime(
    settings: ProfileSettings, clock: Callable[[], datetime],
    billing: BillingSettings = LOCAL_BILLING_SETTINGS,
) -> LocalRuntime:
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
    executor = LocalWorkerPool(
        handler=_unit_execution_forbidden, owner=_WORKER_OWNER, clock=clock,
        lease_seconds=_DISPATCH_LEASE_SECONDS,
    )
    source_catalog = build_source_catalog()
    execution = ExecutionPolicyConfig(
        _DEPLOYMENT_LIMIT, _DISPATCH_LEASE_SECONDS,
        build_execution_callbacks(
            billing, control_plane, BillingGateResources(clock, _DEPLOYMENT_LIMIT),
            noop_execution_started,
        ),
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
    gates = api_billing_gates(billing, BillingGateResources(clock, _DEPLOYMENT_LIMIT))
    return LocalRuntime(
        control_plane=control_plane, object_store=object_store, executor=executor,
        audit_sink=audit_sink, raw_ingestion=raw_ingestion, source_catalog=source_catalog,
        run_planning=run_planning,
        run_authorization=RunAuthorizationService(gates.gate, control_plane, run_planning),
        billing_gates=gates,
    )


def api_billing_gates(
    billing: BillingSettings, resources: BillingGateResources,
) -> ApiBillingGates:
    """Args: billing: Modo de billing; resources: Relógio, limite e DynamoDB.
    Returns: Gate, capacidade e resolvedor de conta de uma única composição por modo.
    """
    enforcement = build_billing_enforcement(billing, resources)
    mode = billing.execution_mode
    catalog = None
    if billing.enforced:
        client, table = resources.dynamodb_client, resources.table_name
        catalog = DynamoBillingCatalog(client, table, resources.clock)
    accounts = TenantAccountResolver(mode, catalog)
    return ApiBillingGates(
        mode, enforcement.gate, enforcement.capacity, accounts, enforcement.audit,
    )


def build_runtime(
    profile: str, values: Mapping[str, str], session: Session,
    execution_started: ExecutionStarted = noop_execution_started,
) -> RuntimeComponents:
    """Args: profile local|aws; values: ambiente; session: boto3; execution_started: callback.
    Returns: Componentes da API consumidos também pelo Billing.
    Raises: ValueError: profile desconhecido ou configuração aws inválida.
    """
    if profile == RuntimeProfile.LOCAL:
        billing = BillingSettings.from_mapping(values)
        local = build_local_runtime(parse_profile(values), _utc_now, billing)
        return RuntimeComponents.from_local(local)
    if profile != RuntimeProfile.AWS:
        raise ValueError("profile=unknown")
    settings = AwsRuntimeSettings.from_mapping(values)
    clients = create_aws_clients(settings, session)
    billing = BillingSettings.from_mapping(values)
    core = build_aws_runtime(settings, clients, _utc_now, billing)
    return _build_aws_api_runtime(
        settings, clients, core, _AwsBilling(billing, execution_started),
    )


def _build_aws_api_runtime(
    settings: AwsRuntimeSettings, clients: AwsClients,
    core: AwsRuntimeComponents, billing: _AwsBilling,
) -> RuntimeComponents:
    _validate_runtime(settings, clients)
    executor = StepFunctionsExecutor(clients.step_functions, settings.state_machine_arn)
    source_catalog = build_source_catalog()
    run_planning = RunPlanningService(
        RunPlanningDependencies(
            control_plane=core.control_plane, object_store=core.object_store,
            executor=executor, source_catalog=source_catalog,
        ),
        _execution_config(settings, clients, core, billing), _utc_now,
    )
    raw_ingestion = RawIngestionService(
        core.control_plane, core.object_store, DeltaPolicy(),
        accepted_manifest=partial(_notify_accepted, run_planning),
    )
    gates = api_billing_gates(billing.settings, _gate_resources(settings, clients))
    return RuntimeComponents(
        control_plane=core.control_plane, object_store=core.object_store, executor=executor,
        audit_sink=core.audit_sink, raw_ingestion=raw_ingestion, source_catalog=source_catalog,
        run_planning=run_planning, services=_aws_api_services(settings, clients, core, gates),
        run_authorization=RunAuthorizationService(gates.gate, core.control_plane, run_planning),
        billing_gates=gates,
    )


def _aws_api_services(
    settings: AwsRuntimeSettings, clients: AwsClients, core: AwsRuntimeComponents,
    gates: ApiBillingGates,
) -> AwsApiServices:
    candidates = DynamoDBMembershipCandidates(clients.dynamodb, settings.control_plane_table)
    serving = S3SignedServingAccess(
        entitled_serving_access(core.control_plane, core.object_store, gates),
        core.object_store,
        clients.s3,
        SignedServingSettings(settings.data_bucket, settings.serving_url_ttl_seconds),
    )
    return AwsApiServices(
        MembershipAuthorizer(core.control_plane, candidates), serving,
        BillingStorage(clients.dynamodb, settings.control_plane_table),
    )


def entitled_serving_access(
    control_plane: ControlPlanePort, object_store: ObjectStorePort, gates: ApiBillingGates,
) -> EntitledServingAccess:
    """Returns: Acesso serving com o gate de entitlement antes de emitir URL ou stream."""
    inner = LocalServingAccess(control_plane, object_store)
    return EntitledServingAccess(inner, gates, control_plane, _utc_now)


def _notify_accepted(run_planning: RunPlanningService, record: RawManifestRecord) -> None:
    run_planning.on_raw_manifest_accepted(record)


def _validate_runtime(settings: AwsRuntimeSettings, clients: AwsClients) -> None:
    validate_state_machine(
        clients.step_functions, settings.state_machine_arn,
        settings.processor_container_name, settings.processor_lease_seconds,
    )


def _gate_resources(settings: AwsRuntimeSettings, clients: AwsClients) -> BillingGateResources:
    return BillingGateResources(
        _utc_now, settings.processor_max_concurrency, clients.dynamodb,
        settings.control_plane_table,
    )


def _execution_config(
    settings: AwsRuntimeSettings, clients: AwsClients,
    core: AwsRuntimeComponents, billing: _AwsBilling,
) -> ExecutionPolicyConfig:
    return ExecutionPolicyConfig(
        settings.processor_max_concurrency, settings.processor_lease_seconds,
        build_execution_callbacks(
            billing.settings, core.control_plane, _gate_resources(settings, clients),
            billing.execution_started,
        ),
    )


__all__ = [
    "AwsApiServices",
    "LocalRuntime",
    "RuntimeComponents",
    "api_billing_gates",
    "build_local_runtime",
    "build_runtime",
    "entitled_serving_access",
    "noop_execution_started",
]
