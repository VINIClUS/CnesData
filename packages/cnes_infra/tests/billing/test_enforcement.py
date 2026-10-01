"""Testes do enforcer shadow e da seleção do enforcer por modo de billing."""

import logging
from unittest.mock import Mock

import pytest

from cnes_domain.billing.models import BillingEnforcementMode, SubscriptionStatus
from cnes_domain.billing.revocation import RevocationResult
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.dynamodb_items import deterministic_id
from cnes_infra.billing.enforcement import (
    SHADOW_ACCESS_LOSS_EVENT,
    AccessLossEnforcerPort,
    ShadowAccessLossEnforcer,
    select_access_loss_enforcer,
)
from cnes_infra.billing.settings import BillingSettings
from packages.cnes_infra.tests.billing.billing_factories import NOW, make_snapshot


class _Audit:
    def __init__(self) -> None:
        self.events: list = []

    def append(self, event) -> None:
        self.events.append(event)


def _settings(mode: BillingMode, enforcement: BillingEnforcementMode) -> BillingSettings:
    return BillingSettings(mode, enforcement, 60)


def test_shadow_registra_auditoria_duravel_deterministica_sem_fence(caplog):
    audit = _Audit()
    enforcer = ShadowAccessLossEnforcer(audit, lambda: NOW)
    snapshot = make_snapshot(version=3, subscription_status=SubscriptionStatus.CANCELED)
    with caplog.at_level(logging.INFO):
        result = enforcer.enforce_access_loss(snapshot, "stripe_webhook")
    event = audit.events[0]
    assert result == RevocationResult(3, (), ())
    assert event.event_id == deterministic_id(SHADOW_ACCESS_LOSS_EVENT, "ba_01", "3")
    assert event.event_type == "entitlement.shadow_access_loss"
    assert event.aggregate_id == "ba_01"
    assert event.actor_id == "stripe_webhook"
    assert event.reason_code == "shadow_access_loss"
    assert event.occurred_at == NOW
    assert event.attributes == {"entitlement_version": 3, "subscription_status": "canceled"}
    expected = "billing_shadow_access_loss billing_account_id=ba_01 entitlement_version=3"
    assert expected in caplog.text
    assert "billing_audit event_type=" not in caplog.text


def test_shadow_nunca_retoma_pendencias():
    enforcer = ShadowAccessLossEnforcer(_Audit(), lambda: NOW)
    assert enforcer.resume_pending("ba_01", "system:reconciler") is None
    assert isinstance(enforcer, AccessLossEnforcerPort)


@pytest.mark.parametrize(
    ("mode", "enforcement"),
    [
        (BillingMode.DISABLED, BillingEnforcementMode.ENFORCE),
        (BillingMode.DISABLED, BillingEnforcementMode.SHADOW),
        (BillingMode.STRIPE, BillingEnforcementMode.OFF),
    ],
)
def test_selecao_desligada_nao_chama_fabricas(mode, enforcement):
    enforced, shadow = Mock(), Mock()
    assert select_access_loss_enforcer(_settings(mode, enforcement), enforced, shadow) is None
    enforced.assert_not_called()
    shadow.assert_not_called()


def test_selecao_stripe_enforce_usa_somente_o_enforcer_real():
    enforced, shadow = Mock(), Mock()
    settings = _settings(BillingMode.STRIPE, BillingEnforcementMode.ENFORCE)
    assert select_access_loss_enforcer(settings, enforced, shadow) is enforced.return_value
    shadow.assert_not_called()


def test_selecao_stripe_shadow_usa_somente_o_enforcer_shadow():
    enforced, shadow = Mock(), Mock()
    settings = _settings(BillingMode.STRIPE, BillingEnforcementMode.SHADOW)
    assert select_access_loss_enforcer(settings, enforced, shadow) is shadow.return_value
    enforced.assert_not_called()
