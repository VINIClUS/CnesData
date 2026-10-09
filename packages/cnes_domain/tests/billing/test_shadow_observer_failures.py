"""Falhas do observador de shadow: nunca levantam, viram log e métrica."""

import logging
from datetime import datetime

import pytest

from cnes_domain.billing.errors import BillingDependencyError, PermanentBillingError
from cnes_domain.billing.models import (
    EntitlementAction,
    SubscriptionStatus,
)
from cnes_domain.billing.shadow import (
    BILLING_ACCOUNT_MISSING,
    CapacityCounterReader,
    ShadowCatalogReader,
    ShadowObserver,
    TenantAccountReader,
)
from packages.cnes_domain.tests.billing.shadow_fakes import (
    LOGGER,
    FakeCatalog,
    FakeProjection,
    Harness,
    SpyAudit,
    SpyMetrics,
    agent_observation,
    observe,
)

_A = EntitlementAction
_S = SubscriptionStatus


@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        ({"catalog": FakeCatalog(error=BillingDependencyError("dynamodb_unavailable"))},
         "dynamodb_unavailable"),
        ({"projection": FakeProjection(error=TimeoutError("lento"))}, "unexpected"),
        ({"projection": FakeProjection(error=PermanentBillingError("snapshot_corrupt"))},
         "snapshot_corrupt"),
        ({"catalog": FakeCatalog(None), "audit": SpyAudit(RuntimeError("cpf=1"))},
         "unexpected"),
    ],
)
def test_falha_de_dependencia_nunca_levanta_e_emite_metrica(caplog, overrides, code):
    harness = Harness(**overrides)

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        observe(harness, agent_observation())

    assert harness.reasons() == []
    assert harness.metric_names() == ["ShadowObserverFailures"]
    assert harness.metrics.metrics[0].dimensions == {}
    messages = [r.getMessage() for r in caplog.records if r.name == LOGGER]
    assert messages[-1] == f"billing_shadow_observer_failed action=register_agent code={code}"


def test_falha_do_relogio_nunca_levanta(caplog):
    def broken() -> datetime:
        raise RuntimeError("relogio")

    harness = Harness(clock=broken)

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        observe(harness, agent_observation())

    assert harness.reasons() == []
    assert harness.metric_names() == []
    messages = [r.getMessage() for r in caplog.records if r.name == LOGGER]
    assert messages[0] == "billing_shadow_observer_failed action=register_agent code=unexpected"


def test_falha_da_metrica_de_falha_nunca_levanta(caplog):
    metrics = SpyMetrics(RuntimeError("emf"))
    harness = Harness(projection=FakeProjection(error=TimeoutError()), metrics=metrics)

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        observe(harness, agent_observation())

    messages = [r.getMessage() for r in caplog.records if r.name == LOGGER]
    assert messages[-1] == "billing_shadow_metric_failed action=register_agent"


def test_falha_sem_metricas_configuradas_apenas_loga(caplog):
    harness = Harness(projection=FakeProjection(error=TimeoutError()), metrics=None)

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        observe(harness, agent_observation())

    assert harness.reasons() == []


def test_falha_da_metrica_de_negacao_mantem_audit_sem_contar_falha(caplog):
    harness = Harness(catalog=FakeCatalog(None), metrics=SpyMetrics(RuntimeError("emf")))

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        observe(harness, agent_observation())

    messages = [r.getMessage() for r in caplog.records if r.name == LOGGER]
    assert harness.reasons() == [BILLING_ACCOUNT_MISSING]
    assert messages == [
        "billing_shadow_denied action=register_agent reason=billing_account_missing",
        "billing_shadow_metric_failed action=register_agent",
    ]


@pytest.mark.parametrize(
    ("protocol", "method", "arity"),
    [
        (TenantAccountReader, "get_tenant_account", 2),
        (ShadowCatalogReader, "get_tenant_link", 3),
        (CapacityCounterReader, "get_capacity_count", 2),
        (ShadowObserver, "observe", 1),
    ],
)
def test_stubs_dos_protocolos_devolvem_none(protocol: type, method: str, arity: int):
    declared = getattr(protocol, method)

    assert declared(object(), *(None for _ in range(arity))) is None
