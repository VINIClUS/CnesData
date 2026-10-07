"""Testes da composição do gate de entitlement e dos callbacks de execução."""

import logging
import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timedelta
from unittest.mock import Mock
from uuid import UUID

import pytest

from cnes_domain.billing.errors import BillingDependencyError, EntitlementDenied
from cnes_domain.billing.execution_policy import BillingConcurrencyPolicy, BillingExecutionStarted
from cnes_domain.billing.models import BillingEnforcementMode, SubscriptionStatus
from cnes_domain.control_plane.entities import Run, RunDependency, RunDispatch
from cnes_domain.control_plane.enums import DispatchState, RunState
from cnes_domain.profiles import BillingMode
from cnes_infra.billing import (
    LOCAL_BILLING_SETTINGS,
    BillingConfigurationError,
    BillingGateResources,
    BillingSettings,
    build_entitlement_gate,
    build_execution_callbacks,
)
from cnes_infra.billing.cache import LocalEntitlementCache
from cnes_infra.billing.wiring import (
    RESERVATION_TTL,
    ChainedExecutionStarted,
    ShadowEntitlementGate,
)
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME, make_snapshot
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    make_run_request,
    quota_env,
    seed_snapshot,
)

OFF = BillingEnforcementMode.OFF
SHADOW = BillingEnforcementMode.SHADOW
ENFORCE = BillingEnforcementMode.ENFORCE
LOGGER_NAME = "cnes_infra.billing.wiring"


def _clock() -> datetime:
    return NOW


def _settings(mode: BillingMode, enforcement: BillingEnforcementMode, ttl: int = 60):
    return BillingSettings(mode, enforcement, ttl)


def _resources(client=None, table=None) -> BillingGateResources:
    return BillingGateResources(_clock, 8, client, table)


class FakeProjection:
    def __init__(self, snapshot=None, error: Exception | None = None) -> None:
        self.snapshot = snapshot
        self.error = error
        self.calls = 0

    def get_snapshot(self, billing_account_id, consistency):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.snapshot


def _shadow_gate(projection: FakeProjection) -> ShadowEntitlementGate:
    gate = build_entitlement_gate(
        _settings(BillingMode.STRIPE, SHADOW), _resources(Mock(), "tabela"),
    )
    assert isinstance(gate, ShadowEntitlementGate)
    gate._observed = projection
    return gate


def _shadow_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == LOGGER_NAME]


@pytest.mark.parametrize("enforcement", [OFF, SHADOW, ENFORCE])
def test_modo_disabled_monta_gate_sem_medicao_e_sem_dynamodb(enforcement):
    client = Mock()
    gate = build_entitlement_gate(_settings(BillingMode.DISABLED, enforcement), _resources(client))

    authorization = gate.authorize_create_run(make_run_request())

    assert authorization.plan_version_id == "local-unmetered-v1"
    assert authorization.budget_reservation_id is None
    assert authorization.max_concurrency == 8
    assert client.mock_calls == []
    assert type(gate).__name__ == "EntitlementGate"


def test_stripe_com_enforcement_off_monta_gate_sem_medicao():
    client = Mock()
    settings = _settings(BillingMode.STRIPE, OFF)

    gate = build_entitlement_gate(settings, _resources(client, TABLE_NAME))
    authorization = gate.authorize_create_run(make_run_request())

    assert authorization.budget_reservation_id is None
    assert authorization.plan_version_id == "local-unmetered-v1"
    assert client.mock_calls == []


def test_local_settings_montam_gate_sem_cliente():
    gate = build_entitlement_gate(LOCAL_BILLING_SETTINGS, _resources())

    assert gate.authorize_create_run(make_run_request()).budget_reservation_id is None


def test_fabrica_de_reserva_gera_uuid_e_ttl_de_quinze_minutos():
    gate = build_entitlement_gate(LOCAL_BILLING_SETTINGS, _resources())
    run_settings = gate._settings

    first = run_settings.reservation_id_factory()
    second = run_settings.reservation_id_factory()

    assert UUID(first).version == 4
    assert first != second
    assert run_settings.reservation_ttl == RESERVATION_TTL == timedelta(minutes=15)
    assert run_settings.deployment_max_concurrency == 8


@pytest.mark.parametrize("enforcement", [SHADOW, ENFORCE])
@pytest.mark.parametrize(
    "resources",
    [_resources(), _resources(Mock()), _resources(None, TABLE_NAME)],
)
def test_stripe_sem_cliente_ou_tabela_exige_dynamodb(enforcement, resources):
    with pytest.raises(BillingConfigurationError) as error:
        build_entitlement_gate(_settings(BillingMode.STRIPE, enforcement), resources)

    assert error.value.code == "billing_dynamodb_required"


def test_enforce_reserva_cota_sobre_dynamodb():
    with quota_env() as env:
        gate = build_entitlement_gate(
            _settings(BillingMode.STRIPE, ENFORCE), _resources(env.client, TABLE_NAME),
        )

        authorization = gate.authorize_create_run(make_run_request())

    assert authorization.budget_reservation_id is not None
    assert authorization.max_concurrency <= 8


def test_enforce_nega_quando_snapshot_ausente():
    with quota_env() as env:
        gate = build_entitlement_gate(
            _settings(BillingMode.STRIPE, ENFORCE), _resources(env.client, TABLE_NAME),
        )

        with pytest.raises(EntitlementDenied):
            gate.authorize_create_run(make_run_request(billing_account_id="ba_ausente"))


def test_enforce_usa_cache_local_quando_ttl_positivo():
    gate = build_entitlement_gate(
        _settings(BillingMode.STRIPE, ENFORCE, 30), _resources(Mock(), TABLE_NAME),
    )

    assert isinstance(gate._cache, LocalEntitlementCache)


def test_enforce_sem_cache_quando_ttl_zero():
    gate = build_entitlement_gate(
        _settings(BillingMode.STRIPE, ENFORCE, 0), _resources(Mock(), TABLE_NAME),
    )

    assert gate._cache is None


def test_shadow_sem_snapshot_libera_e_registra_snapshot_missing(caplog):
    gate = _shadow_gate(FakeProjection(None))

    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        authorization = gate.authorize_create_run(make_run_request())

    records = _shadow_records(caplog)
    assert authorization.budget_reservation_id is None
    assert len(records) == 1
    assert records[0].getMessage() == (
        "billing_shadow_denied action=create_run "
        f"reason=snapshot_missing billing_account_id={ACCOUNT} tenant_id=354130"
    )


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"subscription_status": SubscriptionStatus.ADMIN_REVOKED}, "admin_revoked"),
        (
            {"valid_until": NOW - timedelta(seconds=1), "updated_at": NOW - timedelta(hours=1)},
            "snapshot_expired",
        ),
    ],
)
def test_shadow_com_snapshot_negado_libera_e_registra_motivo(caplog, changes, reason):
    gate = _shadow_gate(FakeProjection(make_snapshot(ACCOUNT, **changes)))

    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        authorization = gate.authorize_create_run(make_run_request())

    records = _shadow_records(caplog)
    assert authorization.budget_reservation_id is None
    assert [r.getMessage().split()[2] for r in records] == [f"reason={reason}"]


def test_shadow_com_snapshot_permitido_nao_registra(caplog):
    snapshot = make_snapshot(ACCOUNT)
    projection = FakeProjection(replace(snapshot, quotas=replace(snapshot.quotas)))
    gate = _shadow_gate(projection)

    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        authorization = gate.authorize_create_run(make_run_request())

    assert authorization.budget_reservation_id is None
    assert _shadow_records(caplog) == []
    assert projection.calls == 1


def test_shadow_com_projecao_indisponivel_libera_e_registra(caplog):
    gate = _shadow_gate(FakeProjection(error=BillingDependencyError("dynamodb_unavailable")))

    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        authorization = gate.authorize_create_run(make_run_request())

    records = _shadow_records(caplog)
    assert authorization.budget_reservation_id is None
    assert [r.getMessage().split()[2] for r in records] == ["reason=projection_unavailable"]


def test_shadow_com_conta_divergente_registra_snapshot_account_mismatch(caplog):
    gate = _shadow_gate(FakeProjection(make_snapshot("ba_outra")))

    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        gate.authorize_create_run(make_run_request())

    records = _shadow_records(caplog)
    assert [r.getMessage().split()[2] for r in records] == ["reason=snapshot_account_mismatch"]


def test_shadow_nao_grava_reserva_no_dynamodb():
    with quota_env() as env:
        gate = build_entitlement_gate(
            _settings(BillingMode.STRIPE, SHADOW), _resources(env.client, TABLE_NAME),
        )
        before = env.client.scan(TableName=TABLE_NAME)["Items"]

        authorization = gate.authorize_create_run(make_run_request())
        after = env.client.scan(TableName=TABLE_NAME)["Items"]

    assert authorization.budget_reservation_id is None
    assert after == before


def test_shadow_le_snapshot_real_do_dynamodb_sem_registrar_quando_permitido(caplog):
    with quota_env() as env:
        seed_snapshot(env.client, make_snapshot(ACCOUNT))
        gate = build_entitlement_gate(
            _settings(BillingMode.STRIPE, SHADOW), _resources(env.client, TABLE_NAME),
        )

        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            gate.authorize_create_run(make_run_request())

    assert isinstance(gate, ShadowEntitlementGate)
    assert _shadow_records(caplog) == []


class FakeControlPlane:
    def get_active_run_dispatch(self, tenant_id, run_id):
        return None

    def get_run_billing_state(self, tenant_id, run_id):
        return None

    def bind_run_execution(self, command):
        raise AssertionError(command)


def _run() -> Run:
    return Run(
        tenant_id="tenant-1",
        run_id="run-1",
        competencia="2026-09",
        dataset_name="cnes",
        state=RunState.PROCESSING,
        dependencies=(RunDependency(source_type="cnes", file_subtype="pf", required=True),),
        missing_sources=(),
        created_at=NOW,
    )


def _dispatch() -> RunDispatch:
    return RunDispatch(
        tenant_id="tenant-1",
        run_id="run-1",
        wave_id="0123456789abcdef",
        dispatch_id="fedcba9876543210",
        generation=1,
        unit_ids=("u1",),
        state=DispatchState.RESERVED,
        lease_until=NOW,
    )


def test_callbacks_compoem_politica_e_started_encadeado():
    downstream = Mock()

    callbacks = build_execution_callbacks(
        _settings(BillingMode.STRIPE, ENFORCE), FakeControlPlane(), _resources(), downstream,
    )

    assert isinstance(callbacks.policy, BillingConcurrencyPolicy)
    assert isinstance(callbacks.started, ChainedExecutionStarted)
    assert isinstance(callbacks.started.billing, BillingExecutionStarted)
    assert callbacks.started.downstream is downstream


def test_callbacks_em_stripe_negam_run_sem_companion():
    callbacks = build_execution_callbacks(
        _settings(BillingMode.STRIPE, ENFORCE), FakeControlPlane(), _resources(), Mock(),
    )

    with pytest.raises(EntitlementDenied):
        callbacks.policy(_run(), _dispatch(), 4)


@pytest.mark.parametrize("enforcement", [OFF, SHADOW])
def test_callbacks_em_stripe_sem_enforce_aceitam_run_legado_sem_companion(enforcement):
    callbacks = build_execution_callbacks(
        _settings(BillingMode.STRIPE, enforcement), FakeControlPlane(), _resources(), Mock(),
    )

    permit = callbacks.policy(_run(), _dispatch(), 4)

    assert permit.max_concurrency == 4
    assert permit.binding_context.billing_account_id == "local-tenant-1"


def test_callbacks_em_disabled_devolvem_permit_sem_medicao():
    callbacks = build_execution_callbacks(
        LOCAL_BILLING_SETTINGS, FakeControlPlane(), _resources(), Mock(),
    )

    permit = callbacks.policy(_run(), _dispatch(), 4)

    assert permit.max_concurrency == 4
    assert permit.binding_context.billing_account_id == "local-tenant-1"


def test_started_encadeado_chama_billing_e_depois_downstream_com_mesmo_permit():
    order: list[str] = []
    billing = Mock(side_effect=lambda *args: order.append("billing"))
    downstream = Mock(side_effect=lambda *args: order.append("downstream"))
    permit, request = object(), object()
    run = _run()

    ChainedExecutionStarted(billing, downstream)(run, request, "exec-1", permit)

    assert order == ["billing", "downstream"]
    assert billing.call_args.args == (run, request, "exec-1", permit)
    assert downstream.call_args.args[3] is permit
    assert downstream.call_args.args == billing.call_args.args


def test_started_encadeado_nao_chama_downstream_quando_billing_falha():
    billing = Mock(side_effect=RuntimeError("bind_failed"))
    downstream = Mock()

    with pytest.raises(RuntimeError, match="bind_failed"):
        ChainedExecutionStarted(billing, downstream)(_run(), object(), "exec-1", object())

    downstream.assert_not_called()


def test_importar_billing_nao_carrega_sdk_remoto():
    code = (
        "import sys, cnes_infra.billing; "
        "print([m for m in ('stripe','boto3','botocore') if m in sys.modules])"
    )

    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", code], capture_output=True, text=True, check=True,
    )

    assert result.stdout.strip() == "[]"

