"""Composição dos gates de billing da API: local, aws e serving."""
from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

from central_api import composition, deps
from central_api.composition import (
    api_billing_gates,
    build_local_runtime,
    entitled_serving_access,
)
from central_api.services.serving_access import LocalServingAccess
from central_api.services.serving_entitlement import EntitledServingAccess
from central_api.serving import S3SignedServingAccess
from cnes_domain.billing.models import BillingEnforcementMode
from cnes_domain.profiles import BillingMode, parse_profile
from cnes_infra.aws import AwsRuntimeSettings
from cnes_infra.billing import BillingGateResources, BillingSettings
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations

TENANT = "354130"


def _clock() -> datetime:
    return datetime.now(UTC)


def _settings(tmp_path):
    return parse_profile({"TENANT_ID": TENANT, "DATA_DIR": str(tmp_path)})


def _billing(mode: BillingMode, enforcement: BillingEnforcementMode) -> BillingSettings:
    return BillingSettings(mode, enforcement, 60)


def _resources() -> BillingGateResources:
    return BillingGateResources(_clock, 4, Mock(name="dynamodb"), "tabela")


def _aws_settings() -> AwsRuntimeSettings:
    return AwsRuntimeSettings.from_mapping({
        "PROFILE": "aws", "AUTH_MODE": "oidc", "AWS_REGION": "us-east-1",
        "AWS_CONTROL_PLANE_TABLE": "tabela", "AWS_DATA_BUCKET": "dados",
        "AWS_AUDIT_BUCKET": "auditoria",
        "AWS_STATE_MACHINE_ARN": "arn:aws:states:us-east-1:000000000000:stateMachine:x",
        "AWS_PROCESSOR_CONTAINER_NAME": "processor", "AWS_AUDIT_RETENTION_DAYS": "365",
        "OIDC_ISSUER": "https://id.example.test", "OIDC_AUDIENCE": "cnesdata-dashboard",
    })


def test_runtime_local_compartilha_gate_entre_runs_e_gates_da_api(tmp_path) -> None:
    runtime = build_local_runtime(_settings(tmp_path), _clock)

    gates = runtime.billing_gates
    assert gates.mode is BillingMode.DISABLED
    assert runtime.run_authorization._gate is gates.gate
    assert gates.accounts.resolve(TENANT) == f"local-{TENANT}"


def test_api_billing_gates_enforce_usa_catalogo_e_capacidade_dynamodb() -> None:
    billing = _billing(BillingMode.STRIPE, BillingEnforcementMode.ENFORCE)

    gates = api_billing_gates(billing, _resources())

    assert gates.mode is BillingMode.STRIPE
    assert isinstance(gates.accounts.catalog, DynamoBillingCatalog)
    assert isinstance(gates.capacity, DynamoQuotaReservations)
    assert gates.capacity is gates.gate._quotas


def test_api_billing_gates_shadow_nao_mede() -> None:
    billing = _billing(BillingMode.STRIPE, BillingEnforcementMode.SHADOW)

    gates = api_billing_gates(billing, _resources())

    assert gates.mode is BillingMode.DISABLED
    assert gates.accounts.catalog is None


def test_api_billing_gates_disabled_nao_mede() -> None:
    billing = _billing(BillingMode.DISABLED, BillingEnforcementMode.OFF)

    gates = api_billing_gates(billing, BillingGateResources(_clock, 4))

    assert gates.mode is BillingMode.DISABLED
    assert gates.accounts.catalog is None
    assert gates.enforced is False


def test_entitled_serving_access_envolve_o_acesso_local() -> None:
    gates = api_billing_gates(
        _billing(BillingMode.DISABLED, BillingEnforcementMode.OFF),
        BillingGateResources(_clock, 4),
    )
    control_plane = Mock(name="control_plane")

    access = entitled_serving_access(control_plane, Mock(name="object_store"), gates)

    assert isinstance(access, EntitledServingAccess)
    assert isinstance(access._inner, LocalServingAccess)
    assert access._gates is gates
    assert access._versions is control_plane


def test_runtime_aws_envolve_serving_assinado_com_gate() -> None:
    settings = _aws_settings()
    clients = SimpleNamespace(dynamodb=Mock(name="dynamodb"), s3=Mock(name="s3"))
    core = SimpleNamespace(control_plane=Mock(), object_store=Mock())
    gates = api_billing_gates(
        _billing(BillingMode.DISABLED, BillingEnforcementMode.OFF),
        BillingGateResources(_clock, 4),
    )

    services = composition._aws_api_services(settings, clients, core, gates)

    assert isinstance(services.serving_access, S3SignedServingAccess)
    assert isinstance(services.serving_access._access_policy, EntitledServingAccess)
    assert services.serving_access._access_policy._gates is gates
    assert services.billing_storage.table_name == "tabela"


def test_serving_local_e_envolvido_quando_ha_gates(tmp_path) -> None:
    from central_api.routes import serving

    settings = _settings(tmp_path)
    runtime = composition.RuntimeComponents.from_local(build_local_runtime(settings, _clock))
    app = SimpleNamespace(state=SimpleNamespace(), dependency_overrides={})

    deps._install_local_auth_and_serving(app, runtime, settings)

    access = app.dependency_overrides[serving.get_serving_access]()
    assert isinstance(access, EntitledServingAccess)
    assert access._gates is runtime.billing_gates
    assert app.state.serving_access is access


def test_serving_local_sem_gates_usa_acesso_local_direto(tmp_path) -> None:
    from central_api.routes import serving

    settings = _settings(tmp_path)
    local = build_local_runtime(settings, _clock)
    runtime = composition.RuntimeComponents.from_local(local)
    runtime = SimpleNamespace(
        control_plane=runtime.control_plane, object_store=runtime.object_store,
        billing_gates=None,
    )
    app = SimpleNamespace(state=SimpleNamespace(), dependency_overrides={})

    deps._install_local_auth_and_serving(app, runtime, settings)

    access = app.dependency_overrides[serving.get_serving_access]()
    assert isinstance(access, LocalServingAccess)


def test_runtime_aws_api_usa_o_mesmo_gate_em_runs_serving_e_runtime() -> None:
    settings = _aws_settings()
    clients = SimpleNamespace(
        dynamodb=Mock(name="dynamodb"), s3=Mock(name="s3"), step_functions=Mock(),
    )
    core = SimpleNamespace(
        control_plane=Mock(), object_store=Mock(), audit_sink=Mock(),
    )
    billing = composition._AwsBilling(
        _billing(BillingMode.DISABLED, BillingEnforcementMode.OFF),
        composition.noop_execution_started,
    )
    with (
        patch.object(composition, "_validate_runtime"),
        patch.object(composition, "StepFunctionsExecutor"),
    ):
        runtime = composition._build_aws_api_runtime(settings, clients, core, billing)

    gates = runtime.billing_gates
    assert runtime.run_authorization._gate is gates.gate
    assert runtime.services.serving_access._access_policy._gates is gates
