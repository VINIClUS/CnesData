"""Testes da composição do worker de billing por modo de enforcement."""

from typing import Any, cast
from unittest.mock import Mock, patch

import pytest

from apps.billing_worker.tests.support import REGION, STATE_MACHINE, STRIPE_ENV, TABLE, env, without
from billing_worker.composition import build_worker
from cnes_domain.billing.revocation import ImmediateRevocationService
from cnes_infra.billing.enforcement import ShadowAccessLossEnforcer
from cnes_infra.billing.metrics import CloudWatchBillingMetrics, DiscardBillingMetrics
from cnes_infra.billing.revocation_sweep import RevocationSweep
from cnes_infra.billing.settings import BillingConfigurationError

ENFORCED = env(BILLING_ENFORCEMENT_MODE="enforce", AWS_STATE_MACHINE_ARN=STATE_MACHINE)


def _build(values: dict[str, str]) -> tuple[Any, Mock, Mock]:
    session = Mock()
    with patch("billing_worker.composition.build_stripe_billing") as stripe:
        worker = build_worker(values, Mock(return_value=session))
    return worker, session, stripe


def _projector_enforcer(worker: Any) -> object:
    projector = worker._jobs.recovery._deps.projector
    return projector._deps.enforcer


@pytest.mark.parametrize(
    "values",
    [
        {"PROFILE": "aws", "BILLING_MODE": "disabled"},
        {"PROFILE": "local", "TENANT_ID": "354130", "BILLING_MODE": "disabled"},
    ],
)
def test_modo_desabilitado_nao_compoe_nem_cria_sessao(values) -> None:
    factory = Mock()
    assert build_worker(values, factory) is None
    factory.assert_not_called()


def test_stripe_compoe_provider_e_storage_da_mesma_sessao() -> None:
    worker, session, stripe = _build(STRIPE_ENV)
    assert [c.args[0] for c in session.client.call_args_list] == ["secretsmanager", "dynamodb"]
    session.client.assert_any_call("dynamodb", region_name=REGION, endpoint_url=None)
    settings, provider, storage, clock = stripe.call_args.args
    assert provider.__class__.__name__ == "SecretsManagerSecretProvider"
    assert (storage.client, storage.table_name) == (session.client.return_value, TABLE)
    assert worker._jobs.request == settings.recovery
    assert clock().utcoffset().total_seconds() == 0


def test_repassa_endpoint_dynamodb_local() -> None:
    _worker, session, _stripe = _build(env(DYNAMODB_ENDPOINT_URL=" http://localhost:8000 "))
    session.client.assert_any_call(
        "dynamodb", region_name=REGION, endpoint_url="http://localhost:8000",
    )


def test_modo_off_nao_tem_enforcer_nem_varredura() -> None:
    worker, session, _stripe = _build(STRIPE_ENV)
    assert worker._jobs.revocations is None
    assert worker._jobs.reconciler._deps.enforcer is None
    assert _projector_enforcer(worker) is None
    assert "stepfunctions" not in [c.args[0] for c in session.client.call_args_list]


def test_modo_shadow_usa_enforcer_que_so_audita() -> None:
    worker, _session, stripe = _build(env(BILLING_ENFORCEMENT_MODE="shadow"))
    enforcer = worker._jobs.reconciler._deps.enforcer
    assert isinstance(enforcer, ShadowAccessLossEnforcer)
    assert _projector_enforcer(worker) is enforcer
    assert isinstance(worker._jobs.revocations, RevocationSweep)
    assert enforcer._audit is stripe.return_value.audit


def test_modo_enforce_compoe_revogacao_imediata_com_step_functions() -> None:
    worker, session, stripe = _build(ENFORCED)
    enforcer = worker._jobs.reconciler._deps.enforcer
    assert isinstance(enforcer, ImmediateRevocationService)
    assert _projector_enforcer(worker) is enforcer
    assert worker._jobs.revocations._deps.enforcer is enforcer
    session.client.assert_any_call("stepfunctions", region_name=REGION)
    deps = enforcer._deps
    assert deps.projection is stripe.return_value.projection
    assert deps.audit is stripe.return_value.audit
    assert cast("Any", deps.executor)._state_machine_arn == STATE_MACHINE


def test_enforce_exige_state_machine_antes_da_sessao() -> None:
    factory = Mock()
    values = env(BILLING_ENFORCEMENT_MODE="enforce")
    with pytest.raises(BillingConfigurationError) as error:
        build_worker(values, factory)
    assert error.value.code == "state_machine_arn_required"
    factory.assert_not_called()


def test_metricas_usam_o_ambiente_configurado() -> None:
    worker, _session, _stripe = _build(env(BILLING_METRICS_ENVIRONMENT="prod"))
    assert isinstance(worker._jobs.metrics, CloudWatchBillingMetrics)
    assert worker._jobs.reconciler._deps.metrics is worker._jobs.metrics


def test_projetor_recebe_as_metricas_do_worker() -> None:
    worker, _session, _stripe = _build(ENFORCED)
    projector = worker._jobs.recovery._deps.projector
    assert projector._deps.metrics is worker._jobs.metrics


def test_metricas_sem_ambiente_sao_descartadas() -> None:
    worker, _session, _stripe = _build(STRIPE_ENV)
    assert isinstance(worker._jobs.metrics, DiscardBillingMetrics)


def test_cursores_de_reconciliacao_e_varredura_sao_distintos() -> None:
    worker, _session, _stripe = _build(ENFORCED)
    reconcile_key = worker._jobs.reconciler._deps.cursor._key
    sweep_key = worker._jobs.revocations._deps.cursor._key
    assert reconcile_key != sweep_key


@pytest.mark.parametrize(
    ("values", "code"),
    [
        (without("AWS_REGION"), "aws_region_required"),
        (env(AWS_REGION="  "), "aws_region_required"),
        (without("AWS_CONTROL_PLANE_TABLE"), "billing_table_required"),
        (without("STRIPE_SECRET_KEY_SECRET_ARN"), "stripe_secret_key_arn_required"),
        (env(BILLING_SUCCESS_URL="https://evil.example/x"), "billing_return_urls_invalid"),
    ],
)
def test_rejeita_config_invalida_antes_de_criar_sessao(values, code) -> None:
    factory = Mock()
    with pytest.raises(BillingConfigurationError) as error:
        build_worker(values, factory)
    assert error.value.code == code
    factory.assert_not_called()


def test_exige_provider_em_modo_stripe() -> None:
    with (
        patch("billing_worker.composition.build_secret_provider", return_value=None),
        pytest.raises(BillingConfigurationError) as error,
    ):
        build_worker(STRIPE_ENV, Mock())
    assert error.value.code == "secret_provider_required"
