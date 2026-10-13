"""Composição do perfil aws do processor sobre os componentes canônicos."""
from __future__ import annotations

import ast
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import Mock, patch

import pytest

from cnes_domain.billing import BillingConcurrencyPolicy
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
from cnes_domain.orchestration.source_catalog import SourceCatalog
from cnes_domain.ports.environment_gate import UNGATED_ADMISSION
from cnes_domain.profiles import parse_profile
from cnes_infra.aws import AwsClients, AwsRuntimeConfigurationError, AwsRuntimeSettings
from cnes_infra.billing import BillingSettings
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.executor.step_functions import (
    IncompatibleStateMachine,
    StepFunctionsExecutionProbe,
    StepFunctionsExecutor,
)
from cnes_infra.object_store import S3ObjectStore
from data_processor import composition
from data_processor.composition import (
    AwsProcessorServices,
    LocalProcessorRuntime,
    build_local_processor_runtime,
    build_processor_runtime,
)
from data_processor.orchestration.coordinator import (
    PipelineCoordinator,
    noop_execution_started,
)
from data_processor.orchestration.publisher import DatasetPublisher
from data_processor.orchestration.unit_handler import RunUnitCommandHandler
from data_processor.orchestration.unit_worker import UnitWorker
from data_processor.pipeline.source_registry import SourceRegistry
from data_processor.pipeline.stage_processor import StageProcessor
from data_processor.recovery import ProcessorRecovery
from packages.cnes_infra.tests.aws.production_values import (
    PRODUCTION_STATE_MACHINE_ARN,
    PRODUCTION_VALUES,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

STATE_MACHINE_ARN = "arn:aws:states:us-east-1:000000000000:stateMachine:cnesdata-test"
EXECUTION_ARN = "arn:aws:states:us-east-1:000000000000:execution:cnesdata-test:2222222222222222"
STATE_MACHINE_FIXTURE = (
    Path(__file__).parents[3]
    / "packages/cnes_infra/tests/fixtures/step_functions/standard_inline_ecs.json"
)
PRODUCTION_FIXTURE = STATE_MACHINE_FIXTURE.with_name("production_public_ip.json")
PROCESSOR_COMPOSITION = Path(composition.__file__)
PROCESSOR_FIELDS = (
    "control_plane", "object_store", "executor", "publisher", "source_registry",
    "stage_processor", "coordinator", "unit_worker", "unit_handler",
)
ECS_ENVELOPE = {
    "TENANT_ID": "354130",
    "RUN_ID": "run-01",
    "WAVE_ID": "1111111111111111",
    "DISPATCH_ID": "2222222222222222",
    "UNIT_ID": "unit-01",
    "EXECUTION_OWNER": EXECUTION_ARN,
    "LEASE_SECONDS": "300",
}


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
        "AWS_PROCESSOR_MAX_CONCURRENCY": "8",
        "AWS_PROCESSOR_LEASE_SECONDS": "300",
        "AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "100",
        "AWS_AUDIT_RETENTION_DAYS": "365",
        "OIDC_ISSUER": "https://id.example.test",
        "OIDC_AUDIENCE": "cnesdata-dashboard",
    }


def _local_values() -> dict[str, str]:
    return {"PROFILE": "local", "TENANT_ID": "354130", "DATA_DIR": "/srv/cnesdata"}


@pytest.fixture
def session() -> Mock:
    return _session()


def _session(fixture: Path = STATE_MACHINE_FIXTURE) -> Mock:
    step_functions = Mock(name="stepfunctions")
    step_functions.describe_state_machine.return_value = {
        "type": "STANDARD",
        "definition": fixture.read_text(encoding="utf-8"),
    }
    s3 = Mock(name="s3")
    s3.get_object_lock_configuration.return_value = {
        "ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}
    }
    clients = {"dynamodb": Mock(name="dynamodb"), "s3": s3, "stepfunctions": step_functions}
    session = Mock(name="session")
    session.client.side_effect = lambda service, **_: clients[service]
    session.clients = clients
    return session


def _local_processor_runtime() -> LocalProcessorRuntime:
    fields = (*PROCESSOR_FIELDS, "audit_sink")
    return LocalProcessorRuntime(**{field: Mock(name=field) for field in fields})


def test_processor_local_delega_ao_runtime_cnd(session: Mock) -> None:
    local = _local_processor_runtime()
    with patch(
        "data_processor.composition.build_local_processor_runtime", return_value=local,
    ) as build_local:
        runtime = build_processor_runtime("local", _local_values(), session)

    build_local.assert_called_once_with(
        parse_profile(_local_values()), composition._utc_now,
        BillingSettings.from_mapping(_local_values()),
    )
    for field in PROCESSOR_FIELDS:
        assert getattr(runtime, field) is getattr(local, field)
    assert runtime.services is None
    session.client.assert_not_called()


def test_processor_aws_reusa_executor_publisher_e_registry_canonicos(session: Mock) -> None:
    catalog = Mock(spec=SourceCatalog)
    registry = Mock(spec=SourceRegistry)
    with (
        patch("data_processor.composition.build_source_catalog", return_value=catalog),
        patch(
            "data_processor.composition.build_source_registry", return_value=registry,
        ) as build_registry,
    ):
        runtime = build_processor_runtime("aws", _aws_values() | ECS_ENVELOPE, session)

    assert isinstance(runtime.control_plane, DynamoDBControlPlane)
    assert isinstance(runtime.object_store, S3ObjectStore)
    assert isinstance(runtime.executor, StepFunctionsExecutor)
    assert isinstance(runtime.publisher, DatasetPublisher)
    assert runtime.source_registry is registry
    build_registry.assert_called_once_with(catalog)
    assert isinstance(runtime.stage_processor, StageProcessor)
    assert isinstance(runtime.coordinator, PipelineCoordinator)
    assert isinstance(runtime.unit_worker, UnitWorker)
    assert isinstance(runtime.unit_handler, RunUnitCommandHandler)
    assert runtime.coordinator._dependencies.executor is runtime.executor
    assert runtime.coordinator._dependencies.publisher is runtime.publisher
    assert runtime.unit_handler._worker is runtime.unit_worker
    session.clients["stepfunctions"].describe_state_machine.assert_called_once_with(
        stateMachineArn=STATE_MACHINE_ARN,
    )


def test_processor_aws_expoe_recovery_limitado(session: Mock) -> None:
    runtime = build_processor_runtime("aws", _aws_values(), session)

    assert isinstance(runtime.services, AwsProcessorServices)
    assert isinstance(runtime.services.recovery, ProcessorRecovery)
    assert runtime.services.recovery_batch_size == 100
    assert runtime.services.recovery._coordinator is runtime.coordinator
    assert runtime.services.recovery._control_plane is runtime.control_plane


def test_processor_aws_encadeia_execution_started_apos_o_binding(session: Mock) -> None:
    started = Mock(name="execution_started")

    runtime = build_processor_runtime("aws", _aws_values(), session, started)

    execution = runtime.coordinator._execution
    assert cast("Any", execution.callbacks.started).downstream is started
    assert isinstance(execution.callbacks.policy, BillingConcurrencyPolicy)
    assert (execution.deployment_limit, execution.dispatch_lease_seconds) == (8, 300)


def test_processor_aws_usa_noop_por_padrao(session: Mock) -> None:
    runtime = build_processor_runtime("aws", _aws_values(), session)

    assert (
        cast("Any", runtime.coordinator._execution.callbacks.started).downstream
        is noop_execution_started
    )


def test_processor_aws_retoma_o_coordinator_apos_persistir_unidade(session: Mock) -> None:
    runtime = build_processor_runtime("aws", _aws_values(), session)
    unit = Mock(tenant_id="354130", run_id="run-01")

    with patch.object(PipelineCoordinator, "resume") as resume:
        runtime.unit_worker._policy.after_persist(unit)

    resume.assert_called_once_with("354130", "run-01", settle=False)


def test_processor_local_retoma_sem_liquidar_apos_persistir_unidade(tmp_path: Path) -> None:
    settings = parse_profile({"TENANT_ID": "354130", "DATA_DIR": str(tmp_path)})
    runtime = build_local_processor_runtime(settings, lambda: datetime(2026, 10, 12, tzinfo=UTC))
    unit = Mock(tenant_id="354130", run_id="run-01")

    with patch.object(PipelineCoordinator, "resume") as resume:
        runtime.unit_worker._policy.after_persist(unit)

    resume.assert_called_once_with("354130", "run-01", settle=False)


def test_processor_aws_falha_fechado_sem_oidc(session: Mock) -> None:
    with pytest.raises(AwsRuntimeConfigurationError, match="auth_mode_must_be_oidc"):
        build_processor_runtime("aws", _aws_values() | {"AUTH_MODE": "local"}, session)

    session.client.assert_not_called()


def test_processor_aws_producao_valida_rede_publica_exata() -> None:
    session = _session(PRODUCTION_FIXTURE)

    runtime = build_processor_runtime("aws", PRODUCTION_VALUES, session)

    execution = runtime.coordinator._execution
    assert (execution.deployment_limit, execution.dispatch_lease_seconds) == (1, 3600)
    session.clients["stepfunctions"].describe_state_machine.assert_called_once_with(
        stateMachineArn=PRODUCTION_STATE_MACHINE_ARN,
    )


def test_processor_aws_generico_coordena_sem_gate_de_ambiente(session: Mock) -> None:
    runtime = build_processor_runtime("aws", _aws_values(), session)

    assert runtime.coordinator._execution.admission is UNGATED_ADMISSION


def test_processor_aws_producao_admite_pelo_gate_e_probe_do_executor() -> None:
    session = _session(PRODUCTION_FIXTURE)

    runtime = build_processor_runtime("aws", PRODUCTION_VALUES, session)

    admission = runtime.coordinator._execution.admission
    assert isinstance(admission, EnvironmentAdmission)
    assert admission._gate is runtime.control_plane
    assert (admission._environment, admission._lease_seconds) == ("prod", 3600)
    probe, executor = admission._probe, runtime.executor
    assert isinstance(probe, StepFunctionsExecutionProbe)
    assert isinstance(executor, StepFunctionsExecutor)
    assert probe._client is executor._client
    assert probe._state_machine_arn == executor._state_machine_arn
    assert probe._state_machine_arn == PRODUCTION_STATE_MACHINE_ARN


@pytest.mark.parametrize(
    ("values", "fixture"),
    [(PRODUCTION_VALUES, STATE_MACHINE_FIXTURE), (_aws_values(), PRODUCTION_FIXTURE)],
    ids=["producao-com-ip-privado", "generico-com-ip-publico"],
)
def test_processor_aws_nao_aceita_o_modo_de_rede_do_outro_perfil(
    values: Mapping[str, str], fixture: Path,
) -> None:
    with pytest.raises(IncompatibleStateMachine, match=r"^assign_public_ip_mismatch$"):
        build_processor_runtime("aws", values, _session(fixture))


def test_processor_aws_producao_sem_security_group_falha_antes_da_aws() -> None:
    settings = replace(
        AwsRuntimeSettings.from_mapping(PRODUCTION_VALUES), security_group_id=None,
    )
    step_functions = Mock(name="stepfunctions")
    clients = AwsClients(dynamodb=Mock(), s3=Mock(), step_functions=step_functions)

    with pytest.raises(
        AwsRuntimeConfigurationError, match=r"^production_security_group_required$",
    ):
        composition._validate_runtime(settings, clients)

    step_functions.describe_state_machine.assert_not_called()


@pytest.mark.parametrize("profile", ["", "gcp", "AWS"])
def test_processor_perfil_desconhecido_falha(session: Mock, profile: str) -> None:
    with pytest.raises(ValueError, match="profile=unknown"):
        build_processor_runtime(profile, _aws_values(), session)

    session.client.assert_not_called()


def test_builders_e_helpers_tem_corpo_menor_que_cinquenta_linhas() -> None:
    names = {"build_processor_runtime", "_build_aws_processor_runtime", "_validate_runtime"}
    tree = ast.parse(PROCESSOR_COMPOSITION.read_text(encoding="utf-8"))
    functions = {node.name: node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}

    for name in names:
        node = functions[name]
        body_lines = cast("int", node.end_lineno) - node.body[0].lineno + 1
        assert body_lines < 50, (name, body_lines)
