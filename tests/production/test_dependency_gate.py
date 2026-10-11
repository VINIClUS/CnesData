"""Gate dos contratos integrados exigidos pelo profile de produção AWS."""

from __future__ import annotations

import inspect
from dataclasses import fields
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

import pytest

from central_api.composition import _aws_api_services, entitled_serving_access
from central_api.services.run_planning import RunPlanningDependencies, RunPlanningService
from central_api.services.serving_access import LocalServingAccess
from central_api.services.serving_entitlement import EntitledServingAccess
from central_api.serving.aws_signed import S3SignedServingAccess
from cnes_domain.control_plane.entities import OutboxEvent
from cnes_domain.control_plane.enums import RunState
from cnes_domain.outbox_dispatcher import DispatchResult, dispatch_once
from cnes_infra.audit.s3_object_lock_sink import S3ObjectLockAuditSink
from cnes_infra.aws import AwsClients, AwsRuntimeComponents, AwsRuntimeSettings, build_aws_runtime
from cnes_infra.billing import LOCAL_BILLING_SETTINGS
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.executor.step_functions import StepFunctionsExecutor, validate_state_machine
from data_processor.orchestration.coordinator import (
    CoordinatorDependencies,
    PipelineCoordinator,
    RecoveryFailed,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from central_api.services.billing_gates import ApiBillingGates
    from cnes_domain.ports.audit import AuditSinkPort
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.object_store import ObjectStorePort
    from cnes_domain.ports.processing import ExecutionPolicyConfig, ProcessorExecutorPort
    from data_processor.orchestration.publisher import DatasetPublisher

NOW = datetime(2026, 10, 10, 12, tzinfo=UTC)
STATE_MACHINE_ARN = "arn:aws:states:us-east-1:000000000000:stateMachine:cnesdata-test"
EMPTY = inspect.Parameter.empty
POSITIONAL = inspect.Parameter.POSITIONAL_OR_KEYWORD
KEYWORD_ONLY = inspect.Parameter.KEYWORD_ONLY


def _aws_values() -> dict[str, str]:
    return {
        "PROFILE": "aws",
        "AUTH_MODE": "oidc",
        "AWS_REGION": "us-east-1",
        "AWS_CONTROL_PLANE_TABLE": "cnesdata-test-control-plane",
        "AWS_DATA_BUCKET": "cnesdata-test-data",
        "AWS_AUDIT_BUCKET": "cnesdata-test-audit",
        "AWS_STATE_MACHINE_ARN": STATE_MACHINE_ARN,
        "AWS_PROCESSOR_CONTAINER_NAME": "processor",
        "AWS_AUDIT_RETENTION_DAYS": "365",
        "OIDC_ISSUER": "https://id.example.test",
        "OIDC_AUDIENCE": "cnesdata-dashboard",
    }


def _signature(
    target: Callable[..., Any],
) -> tuple[tuple[str, inspect._ParameterKind, object], ...]:
    return tuple(
        (name, parameter.kind, parameter.default)
        for name, parameter in inspect.signature(target).parameters.items()
    )


def test_fixa_assinaturas_de_settings_executor_e_validacao() -> None:
    assert _signature(AwsRuntimeSettings.from_mapping) == (("values", POSITIONAL, EMPTY),)
    assert _signature(StepFunctionsExecutor) == (
        ("client", POSITIONAL, EMPTY),
        ("state_machine_arn", POSITIONAL, EMPTY),
    )
    assert _signature(validate_state_machine) == (
        ("client", POSITIONAL, EMPTY),
        ("state_machine_arn", POSITIONAL, EMPTY),
        ("processor_container_name", POSITIONAL, EMPTY),
        ("lease_seconds", POSITIONAL, EMPTY),
    )


def test_fixa_assinaturas_de_persistencia_e_serving() -> None:
    assert _signature(DynamoDBControlPlane) == (
        ("client", POSITIONAL, EMPTY),
        ("table_name", POSITIONAL, EMPTY),
        ("clock", POSITIONAL, EMPTY),
        ("billing", POSITIONAL, LOCAL_BILLING_SETTINGS),
    )
    assert _signature(LocalServingAccess) == (
        ("control_plane", POSITIONAL, EMPTY),
        ("object_store", POSITIONAL, EMPTY),
    )
    assert _signature(S3ObjectLockAuditSink) == (
        ("client", POSITIONAL, EMPTY),
        ("bucket", POSITIONAL, EMPTY),
        ("retention_days", POSITIONAL, EMPTY),
        ("expected_bucket_owner", KEYWORD_ONLY, None),
    )


def test_fixa_assinaturas_de_recovery_e_outbox() -> None:
    assert _signature(PipelineCoordinator.recover) == (
        ("self", POSITIONAL, EMPTY),
        ("limit", POSITIONAL, 100),
    )
    assert _signature(RunPlanningService.recover) == (
        ("self", POSITIONAL, EMPTY),
        ("limit", POSITIONAL, 100),
    )
    assert _signature(dispatch_once) == (
        ("control_plane", POSITIONAL, EMPTY),
        ("sink", POSITIONAL, EMPTY),
        ("now", POSITIONAL, EMPTY),
        ("limit", POSITIONAL, 100),
    )


def test_settings_ignoram_credenciais_da_provider_chain() -> None:
    credential_keys = {
        "AWS_ACCESS_KEY_ID": "test-access",
        "AWS_SECRET_ACCESS_KEY": "test-secret",
        "AWS_SESSION_TOKEN": "test-token",
        "AWS_PROFILE": "test-profile",
        "AWS_ROLE_ARN": "arn:aws:iam::000000000000:role/test",
        "AWS_WEB_IDENTITY_TOKEN_FILE": "/run/secrets/aws/token",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "/credentials",
    }

    settings = AwsRuntimeSettings.from_mapping(_aws_values() | credential_keys)

    static_credential_fields = {
        "access_key_id",
        "secret_access_key",
        "session_token",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "profile",
        "role_arn",
        "web_identity_token_file",
        "container_credentials_relative_uri",
    }
    field_names = {field.name for field in fields(settings)}
    assert field_names.isdisjoint(static_credential_fields)
    assert all(not hasattr(settings, name) for name in static_credential_fields)


def test_executor_expoe_apenas_transporte_publico_canonico() -> None:
    public_methods = {
        name
        for name, member in inspect.getmembers(StepFunctionsExecutor, inspect.isfunction)
        if not name.startswith("_")
    }

    assert public_methods == {"start", "cancel", "status"}


class _ObjectLockClient:
    def get_object_lock_configuration(self, **kwargs: object) -> dict[str, object]:
        assert kwargs == {"Bucket": "cnesdata-test-audit"}
        return {"ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}}


def test_runtime_aws_compoe_plane_store_e_audit_sink_canonicos() -> None:
    clients = AwsClients(
        dynamodb=cast("Any", object()),
        s3=cast("Any", _ObjectLockClient()),
        step_functions=cast("Any", object()),
    )

    runtime = build_aws_runtime(
        AwsRuntimeSettings.from_mapping(_aws_values()), clients, lambda: NOW
    )

    assert isinstance(runtime.control_plane, DynamoDBControlPlane)
    assert isinstance(runtime.audit_sink, S3ObjectLockAuditSink)


def test_composicao_aws_preserva_local_entitlement_e_assinatura_s3() -> None:
    control_plane = cast("ControlPlanePort", object())
    object_store = cast("ObjectStorePort", object())
    gates = cast("ApiBillingGates", object())
    settings = AwsRuntimeSettings.from_mapping(_aws_values())
    clients = AwsClients(
        dynamodb=cast("Any", object()),
        s3=cast("Any", object()),
        step_functions=cast("Any", object()),
    )
    core = AwsRuntimeComponents(
        control_plane=control_plane,
        object_store=object_store,
        audit_sink=cast("AuditSinkPort", object()),
    )

    entitled = entitled_serving_access(control_plane, object_store, gates)
    services = _aws_api_services(settings, clients, core, gates)

    assert isinstance(entitled, EntitledServingAccess)
    assert isinstance(entitled._inner, LocalServingAccess)
    assert isinstance(services.serving_access, S3SignedServingAccess)
    assert isinstance(services.serving_access._access_policy, EntitledServingAccess)
    assert isinstance(services.serving_access._access_policy._inner, LocalServingAccess)


class _RecoveryControlPlane:
    def __init__(self, runs: tuple[object, ...] = ()) -> None:
        self.runs = runs
        self.calls: list[tuple[datetime, int]] = []

    def list_recoverable_runs(self, now: datetime, limit: int) -> tuple[object, ...]:
        self.calls.append((now, limit))
        return self.runs

    def get_run(self, tenant_id: str, run_id: str) -> None:
        del tenant_id, run_id


class _RecoverableRun:
    tenant_id = "354130"
    run_id = "run-failed"
    state = RunState.PROCESSING


def _pipeline_coordinator(control_plane: _RecoveryControlPlane) -> PipelineCoordinator:
    dependencies = CoordinatorDependencies(
        control_plane=cast("ControlPlanePort", control_plane),
        executor=cast("ProcessorExecutorPort", object()),
        publisher=cast("DatasetPublisher", object()),
        clock=lambda: NOW,
    )
    return PipelineCoordinator(dependencies, cast("ExecutionPolicyConfig", object()))


def test_pipeline_recovery_limita_control_plane_e_agrega_falha() -> None:
    control_plane = _RecoveryControlPlane((_RecoverableRun(),))

    with pytest.raises(RecoveryFailed, match=r"^failed=1 recovered=0$"):
        _pipeline_coordinator(control_plane).recover()

    assert control_plane.calls == [(NOW, 100), (NOW, 100)]


def test_run_planning_recovery_limita_control_plane() -> None:
    control_plane = _RecoveryControlPlane()
    dependencies = RunPlanningDependencies(
        control_plane=cast("Any", control_plane),
        object_store=cast("ObjectStorePort", object()),
        executor=cast("ProcessorExecutorPort", object()),
        source_catalog=cast("Any", object()),
    )
    service = RunPlanningService(
        dependencies,
        cast("ExecutionPolicyConfig", object()),
        lambda: NOW,
    )

    assert service.recover() == ()
    assert control_plane.calls == [(NOW, 100)]


class _OutboxControlPlane:
    def __init__(self, event: OutboxEvent, timeline: list[str]) -> None:
        self.event = event
        self.timeline = timeline

    def pending_outbox(self, limit: int) -> tuple[OutboxEvent, ...]:
        self.timeline.append(f"pending:{limit}")
        return (self.event,)

    def mark_outbox_delivered(self, event_id: str, delivered_at: datetime) -> None:
        assert delivered_at == NOW
        self.timeline.append(f"mark:{event_id}")


class _AuditSink:
    def __init__(self, timeline: list[str]) -> None:
        self.timeline = timeline

    def append(self, event: OutboxEvent) -> None:
        self.timeline.append(f"append:{event.event_id}")


def test_dispatch_anexa_auditoria_antes_de_marcar_entrega() -> None:
    timeline: list[str] = []
    event = OutboxEvent(
        tenant_id="354130",
        event_id="event-1",
        event_type="run.published",
        aggregate_id="run-1",
        payload={},
        created_at=NOW,
        delivered_at=None,
    )
    control_plane = _OutboxControlPlane(event, timeline)

    result = dispatch_once(
        cast("ControlPlanePort", control_plane),
        cast("AuditSinkPort", _AuditSink(timeline)),
        NOW,
    )

    assert result == DispatchResult(delivered=1, failed=0)
    assert timeline == ["pending:100", "append:event-1", "mark:event-1"]
