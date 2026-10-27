"""Composição específica dos modos de unidade e recuperação AWS."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from botocore.config import Config

from cnes_domain.ports.processing import ExecutionPolicyConfig
from cnes_infra.audit.s3_object_lock_sink import S3ObjectLockAuditSink
from cnes_infra.aws.runtime import (
    build_aws_processor_core,
    build_dispatch_admission,
    create_aws_clients,
)
from cnes_infra.aws.settings import AwsRuntimeConfigurationError
from cnes_infra.billing import build_execution_callbacks
from cnes_infra.billing.wiring import BillingGateResources
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.executor.step_functions import (
    ProductionNetworkPolicy,
    StepFunctionsExecutor,
    validate_production_state_machine,
    validate_state_machine,
)
from data_processor.orchestration.coordinator import CoordinatorDependencies, PipelineCoordinator
from data_processor.orchestration.publisher import DatasetPublisher
from data_processor.orchestration.unit_handler import RunUnitCommandHandler
from data_processor.orchestration.unit_worker import (
    UnitWorker,
    UnitWorkerDependencies,
    UnitWorkerPolicy,
)
from data_processor.pipeline.stage_processor import StageProcessor
from data_processor.processor_wiring import build_publication_policy, build_source_registry
from data_processor.recovery import ProcessorRecovery

if TYPE_CHECKING:
    from boto3.session import Session
    from botocore.client import BaseClient

    from cnes_domain.billing.ports import ClockPort
    from cnes_domain.ports.audit import AuditSinkPort
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.object_store import ObjectStorePort
    from cnes_domain.ports.processing import ExecutionStarted
    from cnes_infra.aws.processor_settings import (
        AwsAuditDispatchSettings,
        AwsRecoverySettings,
        AwsUnitSettings,
    )
    from cnes_infra.aws.runtime import AwsClients
    from cnes_infra.billing.settings import BillingSettings


@dataclass(frozen=True, slots=True)
class AwsExecutionPolicy:
    billing: BillingSettings
    execution_started: ExecutionStarted


@dataclass(frozen=True, slots=True)
class AwsUnitRuntime:
    unit_handler: RunUnitCommandHandler


@dataclass(frozen=True, slots=True)
class AwsRecoveryRuntime:
    recovery: ProcessorRecovery


@dataclass(frozen=True, slots=True)
class AwsAuditRuntime:
    control_plane: ControlPlanePort
    sink: AuditSinkPort
    clock: ClockPort


type AwsModeRuntime = AwsUnitRuntime | AwsRecoveryRuntime | AwsAuditRuntime


@dataclass(frozen=True, slots=True)
class _AwsAssembly:
    control_plane: ControlPlanePort
    object_store: ObjectStorePort
    coordinator: PipelineCoordinator


def _build_assembly(
    settings: AwsUnitSettings, clients: AwsClients, clock: ClockPort, policy: AwsExecutionPolicy,
) -> _AwsAssembly:
    core = build_aws_processor_core(settings, clients, clock, policy.billing)
    resources = BillingGateResources(
        clock, settings.processor_max_concurrency, clients.dynamodb, settings.control_plane_table,
    )
    callbacks = build_execution_callbacks(
        policy.billing, core.control_plane, resources, policy.execution_started,
    )
    publication = build_publication_policy(policy.billing, core.control_plane, resources, clock)
    _validate_runtime(settings, clients)
    executor = StepFunctionsExecutor(clients.step_functions, settings.state_machine_arn)
    publisher = DatasetPublisher(
        store=core.object_store, control_plane=core.control_plane, publication_policy=publication,
    )
    coordinator = PipelineCoordinator(
        CoordinatorDependencies(core.control_plane, executor, publisher, clock),
        ExecutionPolicyConfig(
            settings.processor_max_concurrency, settings.processor_lease_seconds, callbacks,
            build_dispatch_admission(settings, clients, core.control_plane),
        ),
    )
    return _AwsAssembly(core.control_plane, core.object_store, coordinator)


def build_aws_unit_runtime(
    settings: AwsUnitSettings, session: Session, clock: ClockPort, policy: AwsExecutionPolicy,
) -> AwsUnitRuntime:
    """Args: settings: Configuração. session: Sessão. clock: Relógio. policy: Billing/callback.
    Returns: Handler canônico com retomada após persistência.
    """
    assembly = _build_assembly(settings, create_aws_clients(settings, session), clock, policy)
    processor = StageProcessor(
        assembly.control_plane, assembly.object_store, build_source_registry(), clock,
    )
    worker = UnitWorker(
        UnitWorkerDependencies(assembly.control_plane, assembly.object_store, processor, clock),
        UnitWorkerPolicy(after_persist=lambda unit: assembly.coordinator.resume(
            unit.tenant_id, unit.run_id, settle=False,
        )),
    )
    return AwsUnitRuntime(RunUnitCommandHandler(worker))


def build_aws_recovery_runtime(
    settings: AwsRecoverySettings, session: Session, clock: ClockPort, policy: AwsExecutionPolicy,
) -> AwsRecoveryRuntime:
    """Args: settings: Configuração. session: Sessão. clock: Relógio. policy: Billing/callback.
    Returns: Recuperação canônica, sem pipeline de unidade.
    """
    assembly = _build_assembly(settings, create_aws_clients(settings, session), clock, policy)
    return AwsRecoveryRuntime(
        ProcessorRecovery(assembly.control_plane, assembly.coordinator, clock),
    )


def build_aws_audit_runtime(
    settings: AwsAuditDispatchSettings, session: Session, clock: ClockPort,
) -> AwsAuditRuntime:
    """Compõe somente o control plane e o sink Object Lock do despacho."""
    dynamodb = cast("BaseClient", session.client(  # pyright: ignore[reportUnknownMemberType]
        "dynamodb", region_name=settings.region, endpoint_url=settings.dynamodb_endpoint_url,
    ))
    s3 = cast("BaseClient", session.client(  # pyright: ignore[reportUnknownMemberType]
        "s3",
        region_name=settings.region,
        endpoint_url=settings.service_endpoint_url,
        config=Config(
            signature_version="s3v4",
            s3=({"addressing_style": "virtual"} if settings.service_endpoint_url is None else None),
        ),
    ))
    control_plane = DynamoDBControlPlane(
        client=dynamodb, table_name=settings.control_plane_table, clock=clock,
    )
    try:
        sink = S3ObjectLockAuditSink(
            client=s3, bucket=settings.audit_bucket,
            retention_days=settings.audit_retention_days,
        )
    except ValueError as error:
        if str(error) in {"bucket=invalid", "object_lock=disabled", "retention_days=invalid"}:
            raise AwsRuntimeConfigurationError(str(error)) from error
        raise
    return AwsAuditRuntime(control_plane, sink, clock)


def _validate_runtime(settings: AwsUnitSettings, clients: AwsClients) -> None:
    if not settings.production_policy:
        validate_state_machine(
            clients.step_functions, settings.state_machine_arn,
            settings.processor_container_name, settings.processor_lease_seconds,
        )
        return
    if settings.security_group_id is None:
        raise AwsRuntimeConfigurationError("production_security_group_required")
    validate_production_state_machine(
        clients.step_functions, settings.state_machine_arn,
        ProductionNetworkPolicy(
            settings.processor_container_name, settings.processor_lease_seconds,
            settings.public_subnet_ids, settings.security_group_id,
        ),
    )
