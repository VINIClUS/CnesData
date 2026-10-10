"""Serving em shadow: o observador recebe a âncora de retenção sem mudar o grant."""

from datetime import timedelta
from typing import Any, cast

import pytest

from apps.central_api.tests.services.test_serving_entitlement import (
    NOW,
    RUN_ID,
    TENANT,
    FakeProjection,
    FakeVersions,
    _access,
    _gates,
    _inner,
    _request,
    _snapshot,
    _version,
)
from central_api.services.billing_gates import ApiBillingGates
from central_api.services.serving_access import ServingUnavailable
from cnes_domain.billing.errors import BillingDependencyError
from cnes_domain.billing.models import EntitlementAction, SubscriptionStatus
from cnes_domain.billing.shadow import (
    ShadowEntitlementObserver,
    ShadowObservation,
    ShadowObserverDependencies,
)
from cnes_domain.profiles import BillingMode


class SpyObserver:
    def __init__(self) -> None:
        self.observations: list[ShadowObservation] = []

    def observe(self, observation: ShadowObservation) -> None:
        self.observations.append(observation)


def _shadow_gates(observer: Any, mode: BillingMode = BillingMode.DISABLED) -> ApiBillingGates:
    gates = _gates(FakeProjection(_snapshot()), mode=mode)
    return ApiBillingGates(
        gates.mode, gates.gate, gates.capacity, gates.accounts, observer=observer,
    )


def test_shadow_observa_serving_permitido_com_ancora_preguicosa() -> None:
    observer = SpyObserver()
    versions = FakeVersions(_version(NOW - timedelta(days=3)))
    inner = _inner()

    grant = _access(_shadow_gates(observer), versions, inner).authorize(_request())

    assert grant is inner.authorize.return_value
    [observation] = observer.observations
    assert (observation.action, observation.tenant_id, observation.billing_account_id) == (
        EntitlementAction.SERVING_ACCESS, TENANT, None,
    )
    assert versions.calls == []
    anchor = observation.retention_anchor
    assert anchor is not None
    assert anchor() == NOW - timedelta(days=3)
    assert versions.calls == [(TENANT, "cnes", RUN_ID)]


def test_ancora_sem_versao_devolve_none() -> None:
    observer = SpyObserver()

    _access(_shadow_gates(observer), FakeVersions(None)).authorize(_request())

    anchor = observer.observations[0].retention_anchor
    assert anchor is not None
    assert anchor() is None


def test_membership_negado_nao_chama_observador() -> None:
    observer = SpyObserver()
    access = _access(_shadow_gates(observer), inner=_inner(ServingUnavailable("membership")))

    with pytest.raises(ServingUnavailable):
        access.authorize(_request())

    assert observer.observations == []


def test_decisao_real_negada_nao_chama_observador() -> None:
    observer = SpyObserver()
    gates = _gates(FakeProjection(_snapshot(SubscriptionStatus.ADMIN_REVOKED)))
    gates = ApiBillingGates(
        gates.mode, gates.gate, gates.capacity, gates.accounts, observer=observer,
    )

    with pytest.raises(ServingUnavailable):
        _access(gates).authorize(_request())

    assert observer.observations == []


class _BrokenCatalog:
    def get_tenant_account(self, tenant_id: str, consistency: Any) -> None:
        raise BillingDependencyError("dynamodb_unavailable")


class _NoAudit:
    def append(self, event: Any) -> None:
        raise AssertionError(event)


def test_falha_do_observador_real_mantem_o_grant() -> None:
    observer = ShadowEntitlementObserver(ShadowObserverDependencies(
        cast("Any", _BrokenCatalog()), cast("Any", None), cast("Any", None), _NoAudit(),
        lambda: NOW,
    ))
    inner = _inner()

    grant = _access(_shadow_gates(observer), inner=inner).authorize(_request())

    assert grant is inner.authorize.return_value
