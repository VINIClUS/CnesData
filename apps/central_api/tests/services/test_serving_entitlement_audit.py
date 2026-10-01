"""Auditoria durável das negações do gate de entitlement do serving."""

from dataclasses import replace
from datetime import timedelta
from unittest.mock import Mock

import pytest

from apps.central_api.tests.services.test_serving_entitlement import (
    ACCOUNT,
    NOW,
    RETENTION_DAYS,
    TENANT,
    USER,
    FakeProjection,
    FakeVersions,
    _access,
    _gates,
    _request,
    _snapshot,
    _version,
)
from central_api.services.billing_gates import BillingAccountMissing
from central_api.services.serving_access import ServingUnavailable
from cnes_domain.billing.errors import PermanentBillingError
from cnes_domain.billing.models import SubscriptionStatus


def _audited(projection: FakeProjection, audit: Mock | None):
    return replace(_gates(projection), audit=audit)


def test_negacao_de_entitlement_grava_um_evento_duravel() -> None:
    audit = Mock()
    gates = _audited(FakeProjection(_snapshot(SubscriptionStatus.ADMIN_REVOKED)), audit)

    with pytest.raises(ServingUnavailable) as captured:
        _access(gates).authorize(_request())

    assert captured.value.code == "entitlement_denied"
    audit.append.assert_called_once()
    event = audit.append.call_args.args[0]
    assert event.event_type == "serving.denied"
    assert event.event_id.startswith("serving.denied:")
    assert event.aggregate_id == ACCOUNT
    assert event.actor_id == USER
    assert event.occurred_at == NOW
    assert event.attributes == {
        "tenant_id": TENANT, "dataset_name": "cnes", "access_level": "blocked",
    }


def test_conta_ausente_usa_tenant_como_agregado() -> None:
    audit = Mock()
    resolver = Mock()
    resolver.resolve.side_effect = BillingAccountMissing()
    gates = replace(_gates(FakeProjection(None), resolver=resolver), audit=audit)

    with pytest.raises(ServingUnavailable) as captured:
        _access(gates).authorize(_request())

    assert captured.value.code == "billing_account_missing"
    event = audit.append.call_args.args[0]
    assert event.aggregate_id == TENANT
    assert event.reason_code == "billing_account_missing"


def test_retencao_expirada_audita_com_conta_resolvida() -> None:
    audit = Mock()
    gates = _audited(FakeProjection(_snapshot(SubscriptionStatus.CANCELED)), audit)
    versions = FakeVersions(_version(NOW - timedelta(days=RETENTION_DAYS + 1)))

    with pytest.raises(ServingUnavailable) as captured:
        _access(gates, versions).authorize(_request())

    assert captured.value.code == "retention_expired"
    event = audit.append.call_args.args[0]
    assert event.aggregate_id == ACCOUNT
    assert event.attributes["access_level"] == "read_only"


def test_sem_audit_configurada_nega_sem_gravar() -> None:
    gates = _audited(FakeProjection(_snapshot(SubscriptionStatus.ADMIN_REVOKED)), None)

    with pytest.raises(ServingUnavailable) as captured:
        _access(gates).authorize(_request())

    assert captured.value.code == "entitlement_denied"


def test_indisponibilidade_de_dependencia_nao_audita() -> None:
    audit = Mock()
    gates = _audited(FakeProjection(error=PermanentBillingError("x")), audit)

    with pytest.raises(ServingUnavailable):
        _access(gates).authorize(_request())

    audit.append.assert_not_called()


def _denied_event_id(access_clock) -> str:
    audit = Mock()
    gates = _audited(FakeProjection(_snapshot(SubscriptionStatus.ADMIN_REVOKED)), audit)
    access = _access(gates)
    access._clock = access_clock
    with pytest.raises(ServingUnavailable):
        access.authorize(_request())
    return audit.append.call_args.args[0].event_id


def test_negacoes_repetidas_na_mesma_hora_compartilham_o_id_do_audit() -> None:
    first = _denied_event_id(lambda: NOW)
    second = _denied_event_id(lambda: NOW + timedelta(minutes=30))
    assert first == second
    assert first.startswith("serving.denied:")


def test_negacao_em_outra_hora_gera_novo_audit() -> None:
    first = _denied_event_id(lambda: NOW)
    later = _denied_event_id(lambda: NOW + timedelta(hours=1))
    assert first != later
