"""Composição do perfil aws do processor sobre os componentes canônicos."""
from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import Mock, patch

import pytest

from cnes_domain.billing import BillingConcurrencyPolicy
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
from cnes_domain.ports.environment_gate import UNGATED_ADMISSION
from cnes_domain.profiles import parse_profile
from cnes_infra.aws import AwsRuntimeConfigurationError
from cnes_infra.aws.processor_settings import AwsRecoverySettings, AwsUnitSettings
from cnes_infra.billing import BillingSettings
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.executor.step_functions import (
    IncompatibleStateMachine,
    StepFunctionsExecutionProbe,
    StepFunctionsExecutor,
)
from cnes_infra.object_store import S3ObjectStore
from data_processor import composition
from data_processor.aws_composition import (
    AwsExecutionPolicy,
    build_aws_recovery_runtime,
    build_aws_unit_runtime,
)
from data_processor.composition import build_local_processor_runtime
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


@pytest.fixture
def session() -> Mock:
    return _session()


def _session(
    fixture: Path = STATE_MACHINE_FIXTURE, lease_seconds: int | None = None,
) -> Mock:
    step_functions = Mock(name="stepfunctions")
    step_functions.describe_state_machine.return_value = {
        "type": "STANDARD",
        "definition": fixture.read_text(encoding="utf-8"),
    }
    if lease_seconds is not None:
        definition = json.loads(step_functions.describe_state_machine.return_value["definition"])
        task = definition["States"]["RunUnits"]["ItemProcessor"]["States"]["RunProcessor"]
        environment = task["Parameters"]["Overrides"]["ContainerOverrides"][0]["Environment"]
        next(item for item in environment if item["Name"] == "LEASE_SECONDS")["Value"] = str(
            lease_seconds,
        )
        step_functions.describe_state_machine.return_value["definition"] = json.dumps(definition)
    s3 = Mock(name="s3")
    s3.get_object_lock_configuration.return_value = {
        "ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}
    }
    clients = {"dynamodb": Mock(name="dynamodb"), "s3": s3, "stepfunctions": step_functions}
    session = Mock(name="session")
    session.client.side_effect = lambda service, **_: clients[service]
    session.clients = clients
    return session


@pytest.fixture
def recovery_session() -> Mock:
    return _session(lease_seconds=3600)


def test_processor_local_compoe_runtime_canonico_sem_aws(session: Mock, tmp_path: Path) -> None:
    values = {"PROFILE": "local", "TENANT_ID": "354130", "DATA_DIR": str(tmp_path)}
    runtime = build_local_processor_runtime(
        parse_profile(values), lambda: datetime(2026, 10, 12, tzinfo=UTC),
        BillingSettings.from_mapping(values),
    )

    assert isinstance(runtime.coordinator, PipelineCoordinator)
    assert isinstance(runtime.publisher, DatasetPublisher)
    assert isinstance(runtime.source_registry, SourceRegistry)
    assert isinstance(runtime.stage_processor, StageProcessor)
    assert isinstance(runtime.unit_worker, UnitWorker)
    assert isinstance(runtime.unit_handler, RunUnitCommandHandler)
    assert runtime.coordinator._dependencies.executor is runtime.executor
    assert runtime.coordinator._dependencies.publisher is runtime.publisher
    assert runtime.unit_handler._worker is runtime.unit_worker
    session.client.assert_not_called()
    runtime.executor.close()


def test_processor_aws_reusa_executor_publisher_e_registry_canonicos(session: Mock) -> None:
    values = _aws_values()
    runtime = build_aws_unit_runtime(
        AwsUnitSettings.from_mapping(values), session,
        lambda: datetime(2026, 10, 12, tzinfo=UTC),
        AwsExecutionPolicy(BillingSettings.from_mapping(values), noop_execution_started),
    )
    worker = runtime.unit_handler._worker
    stage = cast("StageProcessor", worker._dependencies.processor)
    registry = stage._source_registry

    assert isinstance(worker._dependencies.control_plane, DynamoDBControlPlane)
    assert isinstance(worker._dependencies.store, S3ObjectStore)
    assert isinstance(stage, StageProcessor)
    assert isinstance(worker, UnitWorker)
    assert isinstance(runtime.unit_handler, RunUnitCommandHandler)
    assert isinstance(registry, SourceRegistry)
    assert [registry.for_pipeline(name).pipeline_id for name in ("cnes", "sihd", "bpa", "sia")] == [
        "cnes", "sihd", "bpa", "sia",
    ]
    assert registry.for_pipeline("cnes").normalize is composition.normalize_cnes
    assert stage._control_plane is worker._dependencies.control_plane
    assert stage._source_store is worker._dependencies.store
    session.clients["stepfunctions"].describe_state_machine.assert_called_once_with(
        stateMachineArn=STATE_MACHINE_ARN,
    )


def test_processor_aws_expoe_recovery_limitado(recovery_session: Mock) -> None:
    values = _aws_values() | {"AWS_PROCESSOR_LEASE_SECONDS": "3600"}
    runtime = build_aws_recovery_runtime(
        AwsRecoverySettings.from_mapping(values), recovery_session,
        lambda: datetime(2026, 10, 12, tzinfo=UTC),
        AwsExecutionPolicy(BillingSettings.from_mapping(values), noop_execution_started),
    )
    recovery = runtime.recovery
    coordinator = recovery._coordinator

    assert isinstance(recovery, ProcessorRecovery)
    assert runtime.processor_lease_seconds == 3600
    assert isinstance(coordinator, PipelineCoordinator)
    assert isinstance(coordinator._dependencies.executor, StepFunctionsExecutor)
    assert isinstance(coordinator._dependencies.publisher, DatasetPublisher)
    assert recovery._control_plane is coordinator._dependencies.control_plane
    assert AwsRecoverySettings.from_mapping(values).processor_recovery_batch_size == 100


def test_processor_aws_encadeia_execution_started_apos_o_binding(recovery_session: Mock) -> None:
    started = Mock(name="execution_started")

    runtime = build_aws_recovery_runtime(
        AwsRecoverySettings.from_mapping(_aws_values() | {"AWS_PROCESSOR_LEASE_SECONDS": "3600"}),
        recovery_session,
        lambda: datetime(2026, 10, 12, tzinfo=UTC),
        AwsExecutionPolicy(BillingSettings.from_mapping(_aws_values()), started),
    )

    execution = runtime.recovery._coordinator._execution
    assert cast("Any", execution.callbacks.started).downstream is started
    assert isinstance(execution.callbacks.policy, BillingConcurrencyPolicy)
    assert (execution.deployment_limit, execution.dispatch_lease_seconds) == (8, 3600)


def test_processor_aws_usa_noop_por_padrao(recovery_session: Mock) -> None:
    runtime = build_aws_recovery_runtime(
        AwsRecoverySettings.from_mapping(_aws_values() | {"AWS_PROCESSOR_LEASE_SECONDS": "3600"}),
        recovery_session,
        lambda: datetime(2026, 10, 12, tzinfo=UTC),
        AwsExecutionPolicy(BillingSettings.from_mapping(_aws_values()), noop_execution_started),
    )

    assert (
        cast("Any", runtime.recovery._coordinator._execution.callbacks.started).downstream
        is noop_execution_started
    )


def test_processor_aws_retoma_o_coordinator_apos_persistir_unidade(session: Mock) -> None:
    values = _aws_values()
    runtime = build_aws_unit_runtime(
        AwsUnitSettings.from_mapping(values), session,
        lambda: datetime(2026, 10, 12, tzinfo=UTC),
        AwsExecutionPolicy(BillingSettings.from_mapping(values), noop_execution_started),
    )
    unit = Mock(tenant_id="354130", run_id="run-01")

    with patch.object(PipelineCoordinator, "resume") as resume:
        runtime.unit_handler._worker._policy.after_persist(unit)

    resume.assert_called_once_with("354130", "run-01", settle=False)


def test_processor_local_retoma_sem_liquidar_apos_persistir_unidade(tmp_path: Path) -> None:
    settings = parse_profile({"TENANT_ID": "354130", "DATA_DIR": str(tmp_path)})
    runtime = build_local_processor_runtime(settings, lambda: datetime(2026, 10, 12, tzinfo=UTC))
    unit = Mock(tenant_id="354130", run_id="run-01")

    with patch.object(PipelineCoordinator, "resume") as resume:
        runtime.unit_worker._policy.after_persist(unit)

    resume.assert_called_once_with("354130", "run-01", settle=False)


@pytest.mark.parametrize("auth", [None, "local", "malformed"])
def test_processor_aws_aceita_configuracao_sem_campos_da_api(
    session: Mock, auth: str | None,
) -> None:
    values = {key: value for key, value in _aws_values().items()
              if key not in {"AUTH_MODE", "OIDC_ISSUER", "OIDC_AUDIENCE", "AWS_AUDIT_BUCKET",
                             "AWS_AUDIT_RETENTION_DAYS"}}
    if auth is not None:
        values |= {"AUTH_MODE": auth, "OIDC_ISSUER": "invalid", "AWS_AUDIT_RETENTION_DAYS": "bad"}
    runtime = build_aws_unit_runtime(
        AwsUnitSettings.from_mapping(values), session,
        lambda: datetime(2026, 10, 12, tzinfo=UTC),
        AwsExecutionPolicy(BillingSettings.from_mapping(values), noop_execution_started),
    )

    assert isinstance(runtime.unit_handler, RunUnitCommandHandler)
    session.clients["stepfunctions"].describe_state_machine.assert_called_once_with(
        stateMachineArn=STATE_MACHINE_ARN,
    )


def test_processor_aws_producao_valida_rede_publica_exata() -> None:
    session = _session(PRODUCTION_FIXTURE)

    runtime = build_aws_recovery_runtime(
        AwsRecoverySettings.from_mapping(PRODUCTION_VALUES),
        session,
        lambda: datetime(2026, 10, 12, tzinfo=UTC),
        AwsExecutionPolicy(BillingSettings.from_mapping(PRODUCTION_VALUES), noop_execution_started),
    )

    execution = runtime.recovery._coordinator._execution
    assert (execution.deployment_limit, execution.dispatch_lease_seconds) == (1, 3600)
    session.clients["stepfunctions"].describe_state_machine.assert_called_once_with(
        stateMachineArn=PRODUCTION_STATE_MACHINE_ARN,
    )


def test_processor_aws_generico_coordena_sem_gate_de_ambiente(recovery_session: Mock) -> None:
    runtime = build_aws_recovery_runtime(
        AwsRecoverySettings.from_mapping(_aws_values() | {"AWS_PROCESSOR_LEASE_SECONDS": "3600"}),
        recovery_session,
        lambda: datetime(2026, 10, 12, tzinfo=UTC),
        AwsExecutionPolicy(BillingSettings.from_mapping(_aws_values()), noop_execution_started),
    )

    assert runtime.recovery._coordinator._execution.admission is UNGATED_ADMISSION


def test_processor_aws_producao_admite_pelo_gate_e_probe_do_executor() -> None:
    session = _session(PRODUCTION_FIXTURE)

    runtime = build_aws_recovery_runtime(
        AwsRecoverySettings.from_mapping(PRODUCTION_VALUES),
        session,
        lambda: datetime(2026, 10, 12, tzinfo=UTC),
        AwsExecutionPolicy(BillingSettings.from_mapping(PRODUCTION_VALUES), noop_execution_started),
    )

    admission = runtime.recovery._coordinator._execution.admission
    assert isinstance(admission, EnvironmentAdmission)
    assert admission._gate is runtime.recovery._control_plane
    assert (admission._environment, admission._lease_seconds) == ("prod", 3600)
    probe, executor = admission._probe, runtime.recovery._coordinator._dependencies.executor
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
        build_aws_unit_runtime(
            AwsUnitSettings.from_mapping(values), _session(fixture),
            lambda: datetime(2026, 10, 12, tzinfo=UTC),
            AwsExecutionPolicy(BillingSettings.from_mapping(values), noop_execution_started),
        )


def test_processor_aws_producao_sem_security_group_falha_antes_da_aws(session: Mock) -> None:
    values = {key: value for key, value in PRODUCTION_VALUES.items()
              if key != "AWS_SECURITY_GROUP_ID"}

    with pytest.raises(
        AwsRuntimeConfigurationError, match=r"^production_security_group_id=required$",
    ):
        build_aws_unit_runtime(
            AwsUnitSettings.from_mapping(values), session,
            lambda: datetime(2026, 10, 12, tzinfo=UTC),
            AwsExecutionPolicy(BillingSettings.from_mapping(values), noop_execution_started),
        )

    session.client.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", ["", "gcp", "legacy"])
async def test_processor_perfil_desconhecido_falha(
    session: Mock, profile: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from data_processor import main as app

    monkeypatch.setenv("PROFILE", profile)
    with (
        patch.object(app, "_setup_logging"),
        patch.object(app, "init_telemetry"),
        patch.object(app, "Session", session),
    ):
        assert await app.main() == 1

    session.assert_not_called()
    session.client.assert_not_called()


def test_builders_e_helpers_tem_corpo_menor_que_cinquenta_linhas() -> None:
    from data_processor import aws_composition, processor_wiring

    targets = {
        Path(aws_composition.__file__): {"build_aws_unit_runtime", "build_aws_recovery_runtime",
                                         "_validate_runtime"},
        Path(processor_wiring.__file__): {"build_source_registry", "build_publication_policy"},
        PROCESSOR_COMPOSITION: {"build_local_processor_runtime"},
    }
    for path, names in targets.items():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        functions = {node.name: node for node in ast.walk(tree)
                     if isinstance(node, ast.FunctionDef)}
        for name in names:
            node = functions[name]
            body_lines = cast("int", node.end_lineno) - node.body[0].lineno + 1
            assert body_lines < 50, (name, body_lines)


@pytest.mark.asyncio
async def test_main_normaliza_perfil_aws_maiusculo_no_roteamento(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from data_processor import main as app

    monkeypatch.setenv("PROFILE", "AWS")
    with patch.object(app, "_run_aws_profile", return_value=0) as run:
        assert await app.main() == 0

    run.assert_called_once_with()
