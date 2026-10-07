"""Testes da composição única de gate e capacidade por modo de billing."""

from datetime import datetime
from unittest.mock import Mock

import pytest

from cnes_domain.billing.gate import EntitlementGate
from cnes_domain.billing.models import BillingEnforcementMode
from cnes_domain.profiles import BillingMode
from cnes_infra.billing import (
    LOCAL_BILLING_SETTINGS,
    BillingConfigurationError,
    BillingEnforcement,
    BillingGateResources,
    BillingSettings,
    build_billing_enforcement,
    build_entitlement_gate,
    wiring,
)
from cnes_infra.billing.disabled import DisabledQuotaReservations
from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations
from cnes_infra.billing.wiring import ShadowEntitlementGate
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import quota_env

STRIPE = BillingMode.STRIPE


def _clock() -> datetime:
    return NOW


def _settings(mode: BillingMode, enforcement: BillingEnforcementMode) -> BillingSettings:
    return BillingSettings(mode, enforcement, 60)


def _resources(client=None, table=None) -> BillingGateResources:
    return BillingGateResources(_clock, 8, client, table)


def test_disabled_expoe_capacidade_sem_medicao():
    client = Mock()

    result = build_billing_enforcement(
        _settings(BillingMode.DISABLED, BillingEnforcementMode.ENFORCE), _resources(client),
    )

    assert isinstance(result, BillingEnforcement)
    assert isinstance(result.capacity, DisabledQuotaReservations)
    assert type(result.gate) is EntitlementGate
    assert result.capacity is result.gate._quotas
    assert client.mock_calls == []


def test_stripe_off_expoe_capacidade_sem_medicao():
    client = Mock()

    result = build_billing_enforcement(
        _settings(STRIPE, BillingEnforcementMode.OFF), _resources(client, TABLE_NAME),
    )

    assert isinstance(result.capacity, DisabledQuotaReservations)
    assert type(result.gate) is EntitlementGate
    assert result.capacity is result.gate._quotas
    assert client.mock_calls == []


def test_shadow_compartilha_a_capacidade_unmetered_do_gate():
    result = build_billing_enforcement(
        _settings(STRIPE, BillingEnforcementMode.SHADOW), _resources(Mock(), TABLE_NAME),
    )

    assert isinstance(result.gate, ShadowEntitlementGate)
    assert isinstance(result.capacity, DisabledQuotaReservations)
    assert result.capacity is result.gate._quotas


def test_enforce_compartilha_a_capacidade_dynamodb_do_gate():
    with quota_env() as env:
        result = build_billing_enforcement(
            _settings(STRIPE, BillingEnforcementMode.ENFORCE),
            _resources(env.client, TABLE_NAME),
        )

    assert isinstance(result.capacity, DynamoQuotaReservations)
    assert result.capacity is result.gate._quotas


@pytest.mark.parametrize(("client", "table"), [(None, TABLE_NAME), (Mock(), None), (Mock(), "")])
def test_enforce_sem_dynamodb_falha_fechado(client, table):
    settings = _settings(STRIPE, BillingEnforcementMode.ENFORCE)

    with pytest.raises(BillingConfigurationError, match="billing_dynamodb_required"):
        build_billing_enforcement(settings, _resources(client, table))


def test_build_entitlement_gate_delega_para_a_mesma_composicao(monkeypatch):
    sentinel = BillingEnforcement(Mock(), Mock())
    monkeypatch.setattr(wiring, "build_billing_enforcement", lambda settings, resources: sentinel)

    gate = build_entitlement_gate(LOCAL_BILLING_SETTINGS, _resources())

    assert gate is sentinel.gate
