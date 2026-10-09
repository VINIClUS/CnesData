"""Testes do audit durável e das métricas compostos pelo wiring de billing."""

import json
from dataclasses import replace
from typing import Any, cast
from unittest.mock import Mock

import pytest

from cnes_domain.billing.errors import EntitlementDenied
from cnes_domain.profiles import BillingMode
from cnes_infra.billing import (
    LOCAL_BILLING_SETTINGS,
    build_billing_enforcement,
    build_entitlement_gate,
    build_execution_callbacks,
)
from cnes_infra.billing.audit_outbox import BestEffortBillingAudit
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME, make_snapshot
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    make_run_request,
    quota_env,
)
from packages.cnes_infra.tests.billing.test_wiring import (
    ENFORCE,
    SHADOW,
    FakeControlPlane,
    FakeProjection,
    _resources,
    _settings,
    _shadow_gate,
)


class _SpyAudit:
    def __init__(self) -> None:
        self.events: list = []

    def append(self, event) -> None:
        self.events.append(event)


def test_shadow_negado_grava_audit_duravel_deterministico():
    audit = _SpyAudit()
    gate = _shadow_gate(FakeProjection(None))
    gate._audit = audit
    request = make_run_request()

    gate.authorize_create_run(request)
    gate.authorize_create_run(request)

    first, second = audit.events
    assert first.event_id == second.event_id
    assert first.event_type == "entitlement.shadow_denied"
    assert first.aggregate_id == ACCOUNT
    assert first.actor_id == "system:shadow_gate"
    assert first.reason_code == "snapshot_missing"
    assert first.occurred_at == NOW
    assert dict(first.attributes) == {
        "action": "create_run",
        "tenant_id": request.tenant_id,
        "run_id": request.run_id,
    }


def test_shadow_permitido_nao_grava_audit():
    audit = _SpyAudit()
    gate = _shadow_gate(FakeProjection(make_snapshot(ACCOUNT)))
    gate._audit = audit

    gate.authorize_create_run(make_run_request())

    assert audit.events == []


def test_shadow_sem_audit_configurado_apenas_libera():
    gate = _shadow_gate(FakeProjection(None))
    gate._audit = None

    assert gate.authorize_create_run(make_run_request()).budget_reservation_id is None


def test_shadow_e_enforce_expoem_audit_best_effort_e_unmetered_nao():
    shadow = build_billing_enforcement(
        _settings(BillingMode.STRIPE, SHADOW), _resources(Mock(), TABLE_NAME),
    )
    enforced = build_billing_enforcement(
        _settings(BillingMode.STRIPE, ENFORCE), _resources(Mock(), TABLE_NAME),
    )
    unmetered = build_billing_enforcement(LOCAL_BILLING_SETTINGS, _resources())

    assert isinstance(shadow.audit, BestEffortBillingAudit)
    assert isinstance(enforced.audit, BestEffortBillingAudit)
    assert unmetered.audit is None


def test_enforce_com_ambiente_de_metricas_emite_negacao_em_cloudwatch(capsys):
    settings = replace(_settings(BillingMode.STRIPE, ENFORCE), metrics_environment="prod")
    with quota_env() as env:
        gate = build_entitlement_gate(settings, _resources(env.client, TABLE_NAME))

        with pytest.raises(EntitlementDenied):
            gate.authorize_create_run(make_run_request(billing_account_id="ba_ausente"))

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    [document] = [line for line in lines if line.get("event") == "billing_metric"]
    assert document["EntitlementChecksDenied"] == 1.0
    assert document["Reason"] == "snapshot_missing"


def _binding_callbacks(settings, resources):
    return build_execution_callbacks(settings, FakeControlPlane(), resources, Mock())


def test_callbacks_stripe_com_dynamodb_montam_audit_best_effort():
    callbacks = _binding_callbacks(
        _settings(BillingMode.STRIPE, ENFORCE), _resources(Mock(), TABLE_NAME),
    )

    audit = cast("Any", callbacks.started).billing._dependencies.audit

    assert isinstance(audit, BestEffortBillingAudit)


@pytest.mark.parametrize(
    "resources",
    [_resources(), _resources(Mock()), _resources(Mock(), "")],
)
def test_callbacks_stripe_sem_dynamodb_ficam_sem_audit(resources):
    callbacks = _binding_callbacks(_settings(BillingMode.STRIPE, ENFORCE), resources)

    assert cast("Any", callbacks.started).billing._dependencies.audit is None


def test_callbacks_disabled_ficam_sem_audit():
    callbacks = _binding_callbacks(LOCAL_BILLING_SETTINGS, _resources(Mock(), TABLE_NAME))

    assert cast("Any", callbacks.started).billing._dependencies.audit is None
