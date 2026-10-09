"""Testes da composição do observador de shadow por modo de billing."""

import json
from dataclasses import replace
from unittest.mock import Mock

import pytest

from cnes_domain.billing.models import (
    BillingEnforcementMode,
    CapacityKind,
    EntitlementAction,
)
from cnes_domain.billing.shadow import (
    NULL_SHADOW_OBSERVER,
    ShadowEntitlementObserver,
    ShadowObservation,
    shadow_bucket_id,
)
from cnes_domain.profiles import BillingMode
from cnes_infra.billing import (
    LOCAL_BILLING_SETTINGS,
    BillingSettings,
    build_billing_enforcement,
)
from cnes_infra.billing.dynamodb_capacity_counters import DynamoCapacityCounters
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_items import deterministic_id
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    make_create_command,
    put_tenant,
)
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT, TENANT, quota_env
from packages.cnes_infra.tests.billing.shadow_support import seed_capacity, shadow_reasons
from packages.cnes_infra.tests.billing.test_wiring import _resources, _settings

OFF = BillingEnforcementMode.OFF
SHADOW = BillingEnforcementMode.SHADOW
ENFORCE = BillingEnforcementMode.ENFORCE
AGENT = ShadowObservation(EntitlementAction.REGISTER_AGENT, TENANT)


@pytest.mark.parametrize(
    "settings",
    [
        BillingSettings(BillingMode.DISABLED, OFF, 0),
        BillingSettings(BillingMode.DISABLED, SHADOW, 0),
        BillingSettings(BillingMode.DISABLED, ENFORCE, 0),
        BillingSettings(BillingMode.STRIPE, OFF, 0),
        BillingSettings(BillingMode.STRIPE, ENFORCE, 0),
        LOCAL_BILLING_SETTINGS,
    ],
)
def test_fora_de_stripe_shadow_observador_e_nulo_sem_chamar_cliente(settings):
    client = Mock()

    enforcement = build_billing_enforcement(settings, _resources(client, TABLE_NAME))
    enforcement.observer.observe(AGENT)

    assert enforcement.observer is NULL_SHADOW_OBSERVER
    assert client.mock_calls == []


def test_stripe_shadow_compoe_observador_real_sem_chamar_cliente():
    client = Mock()

    enforcement = build_billing_enforcement(
        _settings(BillingMode.STRIPE, SHADOW), _resources(client, TABLE_NAME),
    )

    assert isinstance(enforcement.observer, ShadowEntitlementObserver)
    assert client.mock_calls == []


def _shadow_observer(client):
    return build_billing_enforcement(
        _settings(BillingMode.STRIPE, SHADOW), _resources(client, TABLE_NAME),
    ).observer


def test_tenant_sem_link_grava_um_evento_por_hora_no_outbox():
    with quota_env() as env:
        observer = _shadow_observer(env.client)

        observer.observe(AGENT)
        observer.observe(AGENT)
        reasons = shadow_reasons(env.client)

    assert reasons == ["billing_account_missing"]


def _link_account(env) -> None:
    put_tenant(env.client, TENANT)
    DynamoBillingCatalog(env.client, TABLE_NAME, env.clock.now).create_account(
        make_create_command(ACCOUNT, TENANT),
    )


def test_link_real_e_capacidade_semeada_no_limite_registram_excedido():
    with quota_env() as env:
        _link_account(env)
        seed_capacity(env.client, ACCOUNT, agent_count=5, tenant_count=1)
        observer = _shadow_observer(env.client)

        observer.observe(AGENT)
        reasons = shadow_reasons(env.client)

    assert reasons == ["max_agents_exceeded"]


def test_link_real_sem_contador_registra_capacity_not_seeded():
    with quota_env() as env:
        _link_account(env)
        observer = _shadow_observer(env.client)

        observer.observe(AGENT)
        counters = DynamoCapacityCounters(env.client, TABLE_NAME)
        seeded = counters.get_capacity_count(ACCOUNT, CapacityKind.AGENT)
        reasons = shadow_reasons(env.client)

    assert seeded is None
    assert reasons == ["capacity_not_seeded"]


def test_bucket_do_dominio_e_o_mesmo_deterministic_id_do_infra():
    reason = "billing_account_missing"
    expected = deterministic_id(TENANT, "register_agent", reason, "2026093012")

    assert shadow_bucket_id(TENANT, EntitlementAction.REGISTER_AGENT, reason, NOW) == expected


def test_metricas_de_shadow_sao_aceitas_pelo_catalogo_emf(capsys):
    settings = replace(_settings(BillingMode.STRIPE, SHADOW), metrics_environment="prod")
    with quota_env() as env:
        observer = build_billing_enforcement(
            settings, _resources(env.client, TABLE_NAME),
        ).observer

        observer.observe(AGENT)

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    [document] = [line for line in lines if line.get("event") == "billing_metric"]
    assert document["ShadowEntitlementDenials"] == 1.0
    assert document["Reason"] == "billing_account_missing"


def test_metrica_de_falha_do_observador_e_aceita_pelo_catalogo_emf(capsys):
    client = Mock()
    client.get_item.side_effect = TimeoutError("lento")
    settings = replace(_settings(BillingMode.STRIPE, SHADOW), metrics_environment="prod")
    observer = build_billing_enforcement(settings, _resources(client, TABLE_NAME)).observer

    observer.observe(AGENT)

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    [document] = [line for line in lines if line.get("event") == "billing_metric"]
    assert document["ShadowObserverFailures"] == 1.0
    client.transact_write_items.assert_not_called()
