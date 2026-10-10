"""Testes do observador de shadow que audita negações hipotéticas dos gates de API."""

import logging
from dataclasses import replace
from datetime import datetime, timedelta
from typing import cast

import pytest

from cnes_domain.billing.models import (
    CapacityKind,
    EntitlementAction,
    EntitlementSnapshot,
    ReadConsistency,
    SubscriptionStatus,
)
from cnes_domain.billing.shadow import (
    BILLING_ACCOUNT_MISSING,
    NULL_SHADOW_OBSERVER,
    SHADOW_DENIED_EVENT,
    SHADOW_OBSERVER_ACTOR,
    ShadowObservation,
    linked_billing_account,
    shadow_bucket_id,
)
from packages.cnes_domain.tests.billing.shadow_fakes import (
    ACCOUNT,
    LOGGER,
    NOW,
    QUOTAS,
    SNAPSHOT,
    TENANT,
    FakeCapacity,
    FakeCatalog,
    FakeProjection,
    Harness,
    agent_observation,
    bucket,
    make_link,
    observe,
)

_A = EntitlementAction
_S = SubscriptionStatus


def test_tenant_sem_link_registra_billing_account_missing():
    harness = Harness(catalog=FakeCatalog(None))

    observe(harness, agent_observation())

    assert harness.reasons() == [BILLING_ACCOUNT_MISSING]
    assert harness.projection.calls == []
    assert harness.catalog.calls == [("account", TENANT, "strong")]


def test_link_de_outro_tenant_conta_como_conta_ausente():
    harness = Harness(catalog=FakeCatalog(make_link("outro")))

    observe(harness, agent_observation())

    assert harness.reasons() == [BILLING_ACCOUNT_MISSING]


def test_conta_explicita_nao_consulta_link_reverso():
    harness = Harness(capacity=FakeCapacity({CapacityKind.TENANT: 0}))

    observe(harness, ShadowObservation(_A.TENANT_CREATION, "novo", ACCOUNT))

    assert harness.reasons() == []
    assert ("account", "novo", "strong") not in harness.catalog.calls
    assert harness.projection.calls == [(ACCOUNT, ReadConsistency.STRONG)]


@pytest.mark.parametrize(
    ("snapshot", "reason"),
    [
        (None, "snapshot_missing"),
        (replace(SNAPSHOT, billing_account_id="ba-outra"), "snapshot_account_mismatch"),
        (replace(SNAPSHOT, subscription_status=_S.ADMIN_REVOKED), "admin_revoked"),
        (replace(SNAPSHOT, subscription_status=_S.INCOMPLETE), "status_incomplete"),
        (replace(SNAPSHOT, quotas=replace(QUOTAS, max_agents=None)), "quota_missing"),
        (replace(SNAPSHOT, quotas=replace(QUOTAS, max_agents=0)), "quota_not_granted"),
        (
            replace(SNAPSHOT, valid_until=NOW - timedelta(seconds=1)),
            "snapshot_expired",
        ),
    ],
)
def test_snapshot_e_politica_stripe_definem_o_motivo(snapshot, reason):
    harness = Harness(projection=FakeProjection(snapshot))

    observe(harness, agent_observation())

    assert harness.reasons() == [reason]
    assert harness.capacity.calls == []


def test_serving_sem_feature_registra_feature_missing():
    snapshot = replace(SNAPSHOT, features=frozenset())
    harness = Harness(projection=FakeProjection(snapshot))

    observe(harness, ShadowObservation(_A.SERVING_ACCESS, TENANT))

    assert harness.reasons() == ["feature_missing"]


@pytest.mark.parametrize(
    ("action", "kind", "reason"),
    [
        (_A.REGISTER_AGENT, CapacityKind.AGENT, "max_agents_exceeded"),
        (_A.TENANT_CREATION, CapacityKind.TENANT, "max_tenants_exceeded"),
    ],
)
def test_capacidade_no_limite_registra_excedido_com_limite_e_uso(action, kind, reason):
    limit = cast("int", QUOTAS.max_agents if kind is CapacityKind.AGENT else QUOTAS.max_tenants)
    harness = Harness(capacity=FakeCapacity({kind: limit}))

    observe(harness, ShadowObservation(action, "novo", ACCOUNT))

    [event] = harness.audit.events
    assert event.reason_code == reason
    assert event.attributes["limit"] == limit
    assert event.attributes["used"] == limit
    assert harness.capacity.calls == [(ACCOUNT, kind)]


def test_capacidade_abaixo_do_limite_nao_registra():
    harness = Harness(capacity=FakeCapacity({CapacityKind.AGENT: 1}))

    observe(harness, agent_observation())

    assert harness.reasons() == []
    assert harness.metric_names() == []


def test_contador_ausente_registra_capacity_not_seeded():
    harness = Harness()

    observe(harness, agent_observation())

    [event] = harness.audit.events
    assert event.reason_code == "capacity_not_seeded"
    assert "used" not in event.attributes
    assert event.attributes["limit"] == QUOTAS.max_agents


def test_replay_de_tenant_ja_ligado_pula_capacidade():
    catalog = FakeCatalog(tenant_link=make_link("novo"))
    harness = Harness(catalog=catalog, capacity=FakeCapacity({CapacityKind.TENANT: 99}))

    observe(harness, ShadowObservation(_A.TENANT_CREATION, "novo", ACCOUNT))

    assert harness.reasons() == []
    assert harness.capacity.calls == []
    assert catalog.calls == [("link", ACCOUNT, "novo", "strong")]


def test_agente_nao_consulta_link_de_replay():
    harness = Harness(capacity=FakeCapacity({CapacityKind.AGENT: 0}))

    observe(harness, agent_observation())

    assert [call[0] for call in harness.catalog.calls] == ["account"]


def _read_only(retention_days: int = 30) -> EntitlementSnapshot:
    quotas = replace(QUOTAS, retention_days=retention_days)
    return replace(SNAPSHOT, subscription_status=_S.CANCELED, quotas=quotas)


def _serving(created_at: datetime | None) -> ShadowObservation:
    return ShadowObservation(_A.SERVING_ACCESS, TENANT, retention_anchor=lambda: created_at)


def test_serving_read_only_com_versao_expirada_registra_retention_expired():
    harness = Harness(projection=FakeProjection(_read_only()))

    observe(harness, _serving(NOW - timedelta(days=31)))

    [event] = harness.audit.events
    assert event.reason_code == "retention_expired"
    assert event.attributes["limit"] == 30


def test_serving_read_only_dentro_da_retencao_nao_registra():
    harness = Harness(projection=FakeProjection(_read_only()))

    observe(harness, _serving(NOW - timedelta(days=29)))

    assert harness.reasons() == []


def test_serving_read_only_exatamente_no_limite_da_retencao_nao_registra():
    harness = Harness(projection=FakeProjection(_read_only()))

    observe(harness, _serving(NOW - timedelta(days=30)))

    assert harness.reasons() == []


def test_serving_full_nao_le_ancora_de_retencao():
    reads: list[int] = []
    observation = ShadowObservation(
        _A.SERVING_ACCESS, TENANT, retention_anchor=lambda: reads.append(1) or NOW,
    )
    harness = Harness()

    observe(harness, observation)

    assert harness.reasons() == []
    assert reads == []


def test_serving_read_only_sem_ancora_nao_registra():
    harness = Harness(projection=FakeProjection(_read_only()))

    observe(harness, ShadowObservation(_A.SERVING_ACCESS, TENANT))

    assert harness.reasons() == []


def test_serving_read_only_com_versao_ausente_conta_como_falha():
    harness = Harness(projection=FakeProjection(_read_only()))

    observe(harness, _serving(None))

    assert harness.reasons() == []
    assert harness.metric_names() == ["ShadowObserverFailures"]


def test_create_run_permitido_nao_consulta_capacidade():
    harness = Harness()

    observe(harness, ShadowObservation(_A.CREATE_RUN, TENANT, ACCOUNT))

    assert harness.reasons() == []
    assert harness.capacity.calls == []


def test_evento_tem_id_ator_e_atributos_exatos_sem_pii():
    harness = Harness(capacity=FakeCapacity({CapacityKind.AGENT: 2}))

    observe(harness, agent_observation())

    [event] = harness.audit.events
    stable = bucket(TENANT, "register_agent", "max_agents_exceeded", "2026091512")
    assert event.event_id == f"entitlement.shadow_denied:{stable}"
    expected = shadow_bucket_id(TENANT, _A.REGISTER_AGENT, "max_agents_exceeded", NOW)
    assert expected == stable
    assert event.event_type == SHADOW_DENIED_EVENT
    assert event.actor_id == SHADOW_OBSERVER_ACTOR == "system:shadow_observer"
    assert event.aggregate_id == ACCOUNT
    assert event.occurred_at == NOW
    assert dict(event.attributes) == {
        "action": "register_agent",
        "reason": "max_agents_exceeded",
        "tenant_id": TENANT,
        "billing_account_id": ACCOUNT,
        "limit": 2,
        "used": 2,
    }


def test_evento_sem_conta_usa_tenant_como_agregado_e_omite_conta():
    harness = Harness(catalog=FakeCatalog(None))

    observe(harness, agent_observation())

    [event] = harness.audit.events
    assert event.aggregate_id == TENANT
    assert dict(event.attributes) == {
        "action": "register_agent",
        "reason": BILLING_ACCOUNT_MISSING,
        "tenant_id": TENANT,
    }


def test_id_estavel_na_mesma_hora_e_muda_com_hora_acao_ou_motivo():
    later = NOW + timedelta(minutes=29)
    base = shadow_bucket_id(TENANT, _A.REGISTER_AGENT, "admin_revoked", NOW)

    assert shadow_bucket_id(TENANT, _A.REGISTER_AGENT, "admin_revoked", later) == base
    assert shadow_bucket_id(TENANT, _A.REGISTER_AGENT, "admin_revoked",
                            NOW + timedelta(hours=1)) != base
    assert shadow_bucket_id(TENANT, _A.SERVING_ACCESS, "admin_revoked", NOW) != base
    assert shadow_bucket_id(TENANT, _A.REGISTER_AGENT, "status_unpaid", NOW) != base
    assert shadow_bucket_id("outro", _A.REGISTER_AGENT, "admin_revoked", NOW) != base


def test_negacao_emite_metrica_e_log_sem_identificadores(caplog):
    harness = Harness(catalog=FakeCatalog(None))

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        observe(harness, agent_observation())

    [metric] = harness.metrics.metrics
    assert metric.name == "ShadowEntitlementDenials"
    assert metric.unit == "Count"
    assert dict(metric.dimensions) == {"Reason": BILLING_ACCOUNT_MISSING}
    assert [r.getMessage() for r in caplog.records if r.name == LOGGER] == [
        "billing_shadow_denied action=register_agent reason=billing_account_missing",
    ]


def test_sem_metricas_configuradas_apenas_audita():
    harness = Harness(catalog=FakeCatalog(None), metrics=None)

    observe(harness, agent_observation())

    assert harness.reasons() == [BILLING_ACCOUNT_MISSING]


def test_observador_nulo_nao_faz_nada():
    assert NULL_SHADOW_OBSERVER.observe(agent_observation()) is None


def test_link_reverso_resolve_conta_com_leitura_forte():
    catalog = FakeCatalog(make_link())

    assert linked_billing_account(catalog, TENANT) == ACCOUNT
    assert catalog.calls == [("account", TENANT, "strong")]


@pytest.mark.parametrize("link", [None, make_link("outro")])
def test_link_reverso_ausente_ou_divergente_devolve_none(link):
    assert linked_billing_account(FakeCatalog(link), TENANT) is None


def test_politica_usada_e_stripe_mesmo_com_coringa():
    snapshot = replace(SNAPSHOT, features=frozenset({"*"}))
    harness = Harness(projection=FakeProjection(snapshot))

    observe(harness, ShadowObservation(_A.SERVING_ACCESS, TENANT))

    assert harness.reasons() == ["feature_missing"]
