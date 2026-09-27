"""Composição do perfil aws da API: runtime, lifespan e fail-closed."""
from __future__ import annotations

import ast
import asyncio
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import ANY, Mock, patch, sentinel

import pytest
from fastapi.testclient import TestClient

from central_api import composition
from central_api.auth import MembershipAuthorizer
from central_api.composition import (
    AwsApiServices,
    LocalRuntime,
    RuntimeComponents,
    _notify_accepted,
    build_runtime,
    noop_execution_started,
)
from central_api.services.raw_ingestion import RawIngestionService
from central_api.services.run_planning import RunPlanningService
from central_api.serving import S3SignedServingAccess
from cnes_domain.orchestration.source_catalog import SourceCatalog
from cnes_domain.outbox_dispatcher import DispatchResult
from cnes_domain.profiles import parse_profile
from cnes_infra.audit.s3_object_lock_sink import S3ObjectLockAuditSink
from cnes_infra.auth import OidcVerifier
from cnes_infra.aws import AwsRuntimeConfigurationError
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.executor.step_functions import IncompatibleStateMachine, StepFunctionsExecutor
from cnes_infra.object_store import S3ObjectStore

STATE_MACHINE_ARN = "arn:aws:states:us-east-1:000000000000:stateMachine:cnesdata-test"
STATE_MACHINE_FIXTURE = (
    Path(__file__).parents[3]
    / "packages/cnes_infra/tests/fixtures/step_functions/standard_inline_ecs.json"
)
API_COMPOSITION = Path(composition.__file__)
LOCAL_FIELDS = (
    "control_plane", "object_store", "executor", "audit_sink", "raw_ingestion",
    "source_catalog", "run_planning",
)


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
        "AWS_SERVING_URL_TTL_SECONDS": "300",
        "AWS_AUDIT_RETENTION_DAYS": "365",
        "OIDC_ISSUER": "https://id.example.test",
        "OIDC_AUDIENCE": "cnesdata-dashboard",
    }


def _local_values() -> dict[str, str]:
    return {"PROFILE": "local", "TENANT_ID": "354130", "DATA_DIR": "/srv/cnesdata"}


def _step_functions_client(workflow_type: str = "STANDARD") -> Mock:
    client = Mock(name="stepfunctions")
    client.describe_state_machine.return_value = {
        "type": workflow_type,
        "definition": STATE_MACHINE_FIXTURE.read_text(encoding="utf-8"),
    }
    return client


def _s3_client() -> Mock:
    client = Mock(name="s3")
    client.get_object_lock_configuration.return_value = {
        "ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}
    }
    return client


def _session(step_functions: Mock | None = None) -> Mock:
    clients = {
        "dynamodb": Mock(name="dynamodb"),
        "s3": _s3_client(),
        "stepfunctions": step_functions or _step_functions_client(),
    }
    session = Mock(name="session")
    session.client.side_effect = lambda service, **_: clients[service]
    session.clients = clients
    return session


@pytest.fixture
def session() -> Mock:
    return _session()


def _local_api_runtime() -> LocalRuntime:
    return LocalRuntime(**{field: Mock(name=field) for field in LOCAL_FIELDS})


def _aws_components() -> RuntimeComponents:
    return RuntimeComponents(
        **{field: Mock(name=field) for field in LOCAL_FIELDS},
        services=AwsApiServices(
            membership_authorizer=Mock(spec=MembershipAuthorizer),
            serving_access=Mock(spec=S3SignedServingAccess),
        ),
    )


def _make_app() -> Any:
    with patch("central_api.app.init_telemetry"):
        from central_api.app import create_app

        return create_app()


def _set_env(monkeypatch: pytest.MonkeyPatch, values: dict[str, str]) -> None:
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_api_local_delega_ao_runtime_cnd_sem_cliente_aws(session: Mock) -> None:
    local = _local_api_runtime()
    with patch(
        "central_api.composition.build_local_runtime", return_value=local,
    ) as build_local:
        runtime = build_runtime("local", _local_values(), session)

    build_local.assert_called_once_with(parse_profile(_local_values()), composition._utc_now)
    for field in LOCAL_FIELDS:
        assert getattr(runtime, field) is getattr(local, field)
    assert runtime.services is None
    session.client.assert_not_called()


def test_api_aws_instala_runtime_completo_oidc_e_serving(session: Mock) -> None:
    runtime = build_runtime("aws", _aws_values(), session)

    assert isinstance(runtime.control_plane, DynamoDBControlPlane)
    assert isinstance(runtime.object_store, S3ObjectStore)
    assert isinstance(runtime.executor, StepFunctionsExecutor)
    assert isinstance(runtime.audit_sink, S3ObjectLockAuditSink)
    assert isinstance(runtime.raw_ingestion, RawIngestionService)
    assert isinstance(runtime.source_catalog, SourceCatalog)
    assert isinstance(runtime.run_planning, RunPlanningService)
    assert isinstance(runtime.services, AwsApiServices)
    assert isinstance(runtime.services.membership_authorizer, MembershipAuthorizer)
    assert isinstance(runtime.services.serving_access, S3SignedServingAccess)
    assert [call.args[0] for call in session.client.call_args_list] == [
        "dynamodb", "s3", "stepfunctions",
    ]
    session.clients["stepfunctions"].describe_state_machine.assert_called_once_with(
        stateMachineArn=STATE_MACHINE_ARN,
    )


def test_api_aws_planeja_com_executor_e_politica_canonicos(session: Mock) -> None:
    started = Mock(name="execution_started")

    runtime = build_runtime("aws", _aws_values(), session, started)

    planning = runtime.run_planning
    assert planning._dispatch_enabled is True
    assert planning._dependencies.executor is runtime.executor
    assert planning._dependencies.control_plane is runtime.control_plane
    assert planning._dependencies.source_catalog is runtime.source_catalog
    assert planning._execution.callbacks.started is started
    assert (planning._execution.deployment_limit, planning._execution.dispatch_lease_seconds) == (
        8, 300,
    )


def test_api_aws_usa_noop_quando_billing_nao_injeta_callback(session: Mock) -> None:
    runtime = build_runtime("aws", _aws_values(), session)

    assert runtime.run_planning._execution.callbacks.started is noop_execution_started


def test_api_aws_manifesto_aceito_notifica_o_planejamento(session: Mock) -> None:
    runtime = build_runtime("aws", _aws_values(), session)

    with patch.object(RunPlanningService, "on_raw_manifest_accepted") as accepted:
        runtime.raw_ingestion._accepted_manifest(sentinel.record)

    accepted.assert_called_once_with(sentinel.record)


def test_api_aws_falha_fechado_sem_oidc(session: Mock) -> None:
    with pytest.raises(AwsRuntimeConfigurationError, match="auth_mode_must_be_oidc"):
        build_runtime("aws", _aws_values() | {"AUTH_MODE": "local"}, session)

    session.client.assert_not_called()


def test_api_aws_falha_no_startup_com_state_machine_incompativel() -> None:
    session = _session(_step_functions_client("EXPRESS"))

    with pytest.raises(IncompatibleStateMachine, match="workflow_must_be_standard"):
        build_runtime("aws", _aws_values(), session)


@pytest.mark.parametrize("profile", ["", "gcp", "AWS"])
def test_api_perfil_desconhecido_falha_no_startup(session: Mock, profile: str) -> None:
    with pytest.raises(ValueError, match="profile=unknown"):
        build_runtime(profile, _aws_values(), session)

    session.client.assert_not_called()


def test_callback_de_manifesto_aceito_descarta_retorno() -> None:
    run_planning = Mock()
    run_planning.on_raw_manifest_accepted.return_value = sentinel.launch_result

    assert _notify_accepted(run_planning, sentinel.record) is None
    run_planning.on_raw_manifest_accepted.assert_called_once_with(sentinel.record)


def test_builders_e_helpers_tem_corpo_menor_que_cinquenta_linhas() -> None:
    names = {
        "build_runtime", "_build_aws_api_runtime", "_validate_runtime",
        "_execution_config", "_notify_accepted",
    }
    tree = ast.parse(API_COMPOSITION.read_text(encoding="utf-8"))
    functions = {node.name: node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}

    for name in names:
        node = functions[name]
        body_lines = node.end_lineno - node.body[0].lineno + 1
        assert body_lines < 50, (name, body_lines)


@pytest.mark.parametrize("profile", ["local", "aws"])
def test_deps_instala_o_mesmo_runtime_consumido_por_billing(
    profile: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = _aws_values() if profile == "aws" else _local_values()
    _set_env(monkeypatch, values | {"DATA_DIR": str(tmp_path)})
    expected = _aws_components() if profile == "aws" else Mock(spec=RuntimeComponents)
    app = _make_app()

    with patch("central_api.deps.build_runtime", return_value=expected) as build:
        with TestClient(app):
            assert app.state.runtime is expected

    build.assert_called_once_with(profile, os.environ, ANY)


def test_deps_aws_instala_verificador_oidc_do_issuer_configurado(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch, _aws_values())
    app = _make_app()

    with patch("central_api.deps.build_runtime", return_value=_aws_components()):
        with TestClient(app):
            verifier = app.state.oidc_verifier

    assert isinstance(verifier, OidcVerifier)
    assert (verifier._issuer, verifier._audience) == (
        "https://id.example.test", "cnesdata-dashboard",
    )


def test_deps_aws_health_nao_exige_identidade_nem_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch, _aws_values())
    app = _make_app()

    with (
        patch("central_api.deps.build_runtime", return_value=_aws_components()),
        patch("central_api.deps.get_engine") as get_engine,
        patch("central_api.deps.create_engine") as create_engine,
        TestClient(app) as client,
    ):
        response = client.get(
            "/api/v1/system/health", headers={"Authorization": "Bearer lixo"},
        )

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    get_engine.assert_not_called()
    create_engine.assert_not_called()


@pytest.mark.parametrize(
    "state",
    [{}, {"principal": Mock(subject="user-1")}, {"authorized_tenant": Mock(tenant_id="354130")}],
    ids=["sem-contexto", "sem-tenant-autorizado", "sem-principal"],
)
def test_deps_aws_serving_sem_contexto_autorizado_retorna_401(state: dict[str, Mock]) -> None:
    from fastapi import HTTPException

    from central_api.deps import _serving_principal_from_state

    request = SimpleNamespace(state=SimpleNamespace(**state))

    with pytest.raises(HTTPException) as caught:
        _serving_principal_from_state(request)

    assert (caught.value.status_code, caught.value.detail) == (401, "auth_required")


@pytest.mark.parametrize(
    "conflict",
    [{"RAW_BACKEND": "aws"}, {"RAW_AWS_ACCESS_KEY_ID": "AKIAEXAMPLE"}],
    ids=["raw-backend", "chave-estatica-raw"],
)
def test_deps_aws_falha_fechado_com_backend_raw_legado(
    conflict: dict[str, str], monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch, _aws_values() | conflict)
    app = _make_app()

    with patch("central_api.deps.build_runtime") as build:
        with pytest.raises(AwsRuntimeConfigurationError, match="raw_backend=forbidden"):
            with TestClient(app):
                pass

    build.assert_not_called()


def test_raw_backend_aws_preservado_fora_do_perfil_aws(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PROFILE", raising=False)
    monkeypatch.setenv("RAW_BACKEND", "aws")
    raw = (Mock(name="control"), Mock(name="upload"), Mock(name="ingestion"))
    app = _make_app()

    with (
        patch("central_api.raw_aws_runtime.RawAWSConfig.from_env"),
        patch("central_api.raw_aws_runtime.build_raw_aws_runtime", return_value=raw),
        patch("central_api.deps.build_runtime") as build,
        TestClient(app),
    ):
        assert app.state.raw_ingestion is raw[2]
        assert app.state.control_plane is raw[0]
        assert not hasattr(app.state, "oidc_verifier")

    build.assert_not_called()


async def test_loop_da_outbox_entrega_eventos_e_sobrevive_a_falha(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from central_api.deps import _outbox_dispatch_loop

    runtime = _aws_components()
    outcomes = iter([DispatchResult(delivered=2, failed=1), RuntimeError("dynamodb=unavailable")])

    def _dispatch(*_: object) -> DispatchResult:
        outcome = next(outcomes, DispatchResult(delivered=0, failed=0))
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    caplog.set_level("INFO", logger="central_api.deps")
    with (
        patch("central_api.deps._OUTBOX_INTERVAL", 0),
        patch("central_api.deps.dispatch_once", side_effect=_dispatch) as dispatch,
    ):
        task = asyncio.create_task(_outbox_dispatch_loop(runtime))
        while dispatch.call_count < 3:
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    dispatch.assert_called_with(runtime.control_plane, runtime.audit_sink, ANY)
    messages = [record.getMessage() for record in caplog.records]
    assert messages == ["outbox_dispatched delivered=2 failed=1", "outbox_dispatch_error"]
