"""Matriz de billing da composição da central_api: perfil x modo."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import Mock

import pytest

from central_api.composition import build_runtime, noop_execution_started
from central_api.services.run_authorization import RunAuthorizationService
from cnes_domain.billing import (
    BillingConcurrencyPolicy,
    BillingExecutionStarted,
    RunExecutionPermit,
)
from cnes_domain.profiles import BillingMode
from cnes_infra.aws import AwsRuntimeSettings
from cnes_infra.billing import BillingConfigurationError
from cnes_infra.billing.wiring import ChainedExecutionStarted
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane

if TYPE_CHECKING:
    from cnes_domain.control_plane.entities import Run, RunDispatch

STATE_MACHINE_ARN = "arn:aws:states:us-east-1:000000000000:stateMachine:cnesdata-test"
STATE_MACHINE_FIXTURE = (
    Path(__file__).parents[3]
    / "packages/cnes_infra/tests/fixtures/step_functions/standard_inline_ecs.json"
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


@pytest.fixture
def session() -> Mock:
    step_functions = Mock(name="stepfunctions")
    step_functions.describe_state_machine.return_value = {
        "type": "STANDARD",
        "definition": STATE_MACHINE_FIXTURE.read_text(encoding="utf-8"),
    }
    s3 = Mock(name="s3")
    s3.get_object_lock_configuration.return_value = {
        "ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}
    }
    clients = {"dynamodb": Mock(name="dynamodb"), "s3": s3, "stepfunctions": step_functions}
    fake = Mock(name="session")
    fake.client.side_effect = lambda service, **_: clients[service]
    return fake


def _local_values(tmp_path: Path) -> dict[str, str]:
    return {"PROFILE": "local", "TENANT_ID": "354130", "DATA_DIR": str(tmp_path)}


def _run_and_dispatch() -> tuple[SimpleNamespace, SimpleNamespace]:
    run = SimpleNamespace(tenant_id="354130", run_id="run-01")
    dispatch = SimpleNamespace(
        tenant_id="354130", run_id="run-01", wave_id="1" * 16, dispatch_id="2" * 16,
        generation=1,
    )
    return run, dispatch


def test_local_disabled_usa_callbacks_de_billing_encadeados_ao_noop(tmp_path: Path) -> None:
    runtime = build_runtime("local", _local_values(tmp_path), Mock())

    callbacks = runtime.run_planning._execution.callbacks
    assert isinstance(callbacks.policy, BillingConcurrencyPolicy)
    assert isinstance(callbacks.started, ChainedExecutionStarted)
    assert isinstance(callbacks.started.billing, BillingExecutionStarted)
    assert callbacks.started.downstream is noop_execution_started


def test_local_disabled_emite_permit_com_contexto_de_binding(tmp_path: Path) -> None:
    runtime = build_runtime("local", _local_values(tmp_path), Mock())
    run, dispatch = _run_and_dispatch()

    permit = runtime.run_planning._execution.callbacks.policy(
        cast("Run", run), cast("RunDispatch", dispatch), 3
    )

    assert permit.max_concurrency == 3
    assert isinstance(permit.binding_context, RunExecutionPermit)


def test_local_disabled_compoe_autorizacao_de_run(tmp_path: Path) -> None:
    runtime = build_runtime("local", _local_values(tmp_path), Mock())

    assert isinstance(runtime.run_authorization, RunAuthorizationService)


def test_local_stripe_e_rejeitado_no_startup(tmp_path: Path) -> None:
    values = _local_values(tmp_path) | {"BILLING_MODE": "stripe"}

    with pytest.raises(BillingConfigurationError) as error:
        build_runtime("local", values, Mock())

    assert error.value.code == "local_billing_disabled"


@pytest.mark.parametrize(
    ("mode", "enforcement", "expected"),
    [
        ("disabled", "enforce", BillingMode.DISABLED),
        ("stripe", "off", BillingMode.DISABLED),
        ("stripe", "shadow", BillingMode.DISABLED),
        ("stripe", "enforce", BillingMode.STRIPE),
    ],
)
def test_aws_propaga_modo_efetivo_ao_control_plane_e_a_politica(
    session: Mock, mode: str, enforcement: str, expected: BillingMode,
) -> None:
    started = Mock(name="execution_started")
    values = _aws_values() | {"BILLING_MODE": mode, "BILLING_ENFORCEMENT_MODE": enforcement}

    runtime = build_runtime("aws", values, session, started)

    callbacks = runtime.run_planning._execution.callbacks
    assert isinstance(runtime.control_plane, DynamoDBControlPlane)
    assert runtime.control_plane._billing.mode is BillingMode(mode)
    assert runtime.control_plane._billing.enforcement_mode.value == enforcement
    assert isinstance(callbacks.policy, BillingConcurrencyPolicy)
    assert callbacks.policy._dependencies.mode is expected
    assert cast("Any", callbacks.started).billing._dependencies.mode is expected
    assert cast("Any", callbacks.started).downstream is started
    assert isinstance(runtime.run_authorization, RunAuthorizationService)


def test_aws_usa_noop_a_jusante_quando_nao_ha_callback_injetado(session: Mock) -> None:
    runtime = build_runtime("aws", _aws_values(), session)

    started = runtime.run_planning._execution.callbacks.started
    assert cast("Any", started).downstream is noop_execution_started


def test_aws_rejeita_enforcement_invalido(session: Mock) -> None:
    values = _aws_values() | {"BILLING_ENFORCEMENT_MODE": "bogus"}

    with pytest.raises(BillingConfigurationError):
        build_runtime("aws", values, session)


def test_settings_aws_continuam_ignorando_billing_mode() -> None:
    settings = AwsRuntimeSettings.from_mapping(_aws_values() | {"BILLING_MODE": "stripe"})

    assert settings.control_plane_table == "cnesdata-test-control-plane"
