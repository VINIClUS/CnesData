"""Testes dos modelos imutáveis de billing."""

from collections.abc import Callable
from dataclasses import FrozenInstanceError, astuple, replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from cnes_domain.billing.models import (
    LOCAL_UNMETERED_PLAN_KEY,
    AccessLevel,
    AnalyticsAuthorization,
    BillingAccount,
    BillingAccountPage,
    BillingAccountStatus,
    BillingAccountTenantLink,
    BillingAuditEvent,
    BillingEnforcementMode,
    BillingMetric,
    CapacityKind,
    CapacityReservation,
    EntitlementAction,
    EntitlementDecision,
    EntitlementSnapshot,
    PlanVersion,
    QuotaLimits,
    QuotaReservation,
    ReadConsistency,
    ReservationKind,
    ReservationStatus,
    RunAuthorization,
    SubscriptionStatus,
)

_NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)
_NAIVE = _NOW.replace(tzinfo=None)
_LATER = _NOW + timedelta(days=30)
_QUOTAS = QuotaLimits(5, 10, 100, 4, 365, 1_000_000)


def _account(**overrides: Any) -> BillingAccount:
    fields = {
        "billing_account_id": "ba-1", "stripe_customer_id": "cus_1", "owner_user_id": "user-1",
        "status": BillingAccountStatus.ACTIVE, "created_at": _NOW, "updated_at": _NOW,
    }
    return BillingAccount(**{**fields, **overrides})


def _link(tenant_id: str = "tenant-a", **overrides: Any) -> BillingAccountTenantLink:
    fields = {
        "billing_account_id": "ba-1", "tenant_id": tenant_id, "linked_by_user_id": "user-1",
        "reason_code": "onboarding", "linked_at": _NOW,
    }
    return BillingAccountTenantLink(**{**fields, **overrides})


def _plan(**overrides: Any) -> PlanVersion:
    fields = {
        "plan_version_id": "plan-v1", "plan_key": "pro", "stripe_product_id": "prod_1",
        "stripe_price_ids": ("price_1", "price_2"),
        "features": frozenset({"serving", "analytics"}), "quotas": _QUOTAS,
        "grace_period_days": 7, "effective_from": _NOW,
    }
    return PlanVersion(**{**fields, **overrides})


def _snapshot(**overrides: Any) -> EntitlementSnapshot:
    fields = {
        "billing_account_id": "ba-1", "stripe_subscription_id": "sub_1",
        "subscription_status": SubscriptionStatus.ACTIVE, "cancel_at_period_end": False,
        "plan_version_id": "plan-v1", "features": frozenset({"serving"}), "quotas": _QUOTAS,
        "period_start": _NOW, "period_end": _LATER, "grace_until": _LATER, "valid_until": _LATER,
        "entitlement_version": 3, "updated_at": _NOW, "source_event_id": "evt_1",
    }
    return EntitlementSnapshot(**{**fields, **overrides})


_QUOTA_RES: dict[str, Any] = {
    "reservation_id": "res-1", "billing_account_id": "ba-1", "resource_id": "run-1",
    "kind": ReservationKind.RUN, "period_start": _NOW, "reserved_runs": 1,
    "reserved_scan_bytes": 10, "consumed_runs": 0, "consumed_scan_bytes": 0,
    "status": ReservationStatus.RESERVED, "created_at": _NOW, "expires_at": _LATER,
}


def _quota_reservation(**overrides: Any) -> QuotaReservation:
    return QuotaReservation(**{**_QUOTA_RES, **overrides})


def _capacity(**overrides: Any) -> CapacityReservation:
    fields = {
        "reservation_id": "cap-1", "billing_account_id": "ba-1", "resource_id": "agent-1",
        "kind": CapacityKind.AGENT, "status": ReservationStatus.RESERVED, "created_at": _NOW,
        "expires_at": _LATER,
    }
    return CapacityReservation(**{**fields, **overrides})


def _audit(**overrides: Any) -> BillingAuditEvent:
    fields = {
        "event_id": "aud-1", "event_type": "snapshot_written", "aggregate_id": "ba-1",
        "actor_id": "system", "reason_code": "webhook", "occurred_at": _NOW,
        "attributes": {"version": 3, "ok": True, "note": None, "src": "stripe"},
    }
    return BillingAuditEvent(**{**fields, **overrides})


def _metric(**overrides: Any) -> BillingMetric:
    fields = {
        "name": "runs_reserved", "value": 1.5, "unit": "count", "dimensions": {"plan": "pro"},
        "occurred_at": _NOW,
    }
    return BillingMetric(**{**fields, **overrides})


def test_plan_version_e_snapshot_sao_immutaveis() -> None:
    plan = _plan()
    snapshot = _snapshot(plan_version_id=plan.plan_version_id)
    with pytest.raises(FrozenInstanceError):
        plan.plan_key = "outro"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        snapshot.entitlement_version = 2  # type: ignore[misc]


def test_snapshot_rejeita_periodo_invertido() -> None:
    with pytest.raises(ValueError, match="period_end_before_start"):
        _snapshot(period_start=_NOW, period_end=_NOW - timedelta(seconds=1))


def test_conta_multi_tenant_usa_links_sem_tenant_singular() -> None:
    account = _account()
    first = _link("tenant-a", billing_account_id=account.billing_account_id)
    second = _link("tenant-b", billing_account_id=account.billing_account_id)
    assert first.tenant_id != second.tenant_id
    assert not hasattr(account, "tenant_id")


def test_enums_expoem_valores_de_string_do_plano() -> None:
    assert [s.value for s in SubscriptionStatus] == [
        "trialing", "active", "past_due", "incomplete", "incomplete_expired",
        "unpaid", "paused", "canceled", "admin_revoked",
    ]
    assert [a.value for a in EntitlementAction] == [
        "create_run", "register_agent", "analytics_query",
        "serving_access", "tenant_creation", "publish_run",
    ]
    assert [s.value for s in BillingAccountStatus] == ["active", "transfer_pending", "closed"]
    assert [m.value for m in BillingEnforcementMode] == ["off", "shadow", "enforce"]
    assert [a.value for a in AccessLevel] == ["full", "read_only", "blocked"]
    assert [s.value for s in ReservationStatus] == ["reserved", "consumed", "released"]
    assert [k.value for k in ReservationKind] == ["run", "analytics"]
    assert [k.value for k in CapacityKind] == ["tenant", "agent"]
    assert (ReadConsistency.EVENTUAL, ReadConsistency.STRONG) == ("eventual", "strong")
    assert LOCAL_UNMETERED_PLAN_KEY == "local-unmetered"


def test_instancia_conta_com_todos_os_campos() -> None:
    account = BillingAccount(
        billing_account_id="ba-9",
        stripe_customer_id=None,
        owner_user_id="owner",
        status=BillingAccountStatus.TRANSFER_PENDING,
        created_at=_NOW,
        updated_at=_LATER,
    )
    assert (account.stripe_customer_id, account.updated_at) == (None, _LATER)
    assert account.status is BillingAccountStatus.TRANSFER_PENDING


@pytest.mark.parametrize(
    "overrides",
    [{"billing_account_id": ""}, {"owner_user_id": " "}, {"stripe_customer_id": ""},
     {"created_at": _NAIVE}, {"updated_at": _NAIVE},],
)
def test_rejeita_conta_invalida(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="reason="):
        _account(**overrides)


def test_instancia_link_com_todos_os_campos() -> None:
    link = BillingAccountTenantLink("ba-2", "tenant-z", "admin", "migracao", _LATER)
    assert (link.billing_account_id, link.tenant_id, link.linked_at) == ("ba-2", "tenant-z", _LATER)
    assert (link.linked_by_user_id, link.reason_code) == ("admin", "migracao")


@pytest.mark.parametrize(
    "overrides",
    [{"billing_account_id": ""}, {"tenant_id": ""}, {"linked_by_user_id": ""},
     {"reason_code": ""}, {"linked_at": _NAIVE},],
)
def test_rejeita_link_invalido(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="reason="):
        _link(**overrides)


def test_instancia_pagina_de_contas_com_todos_os_campos() -> None:
    page = BillingAccountPage(accounts=(_account(),), next_cursor="cur-1")
    assert (page.accounts, page.next_cursor) == ((_account(),), "cur-1")
    assert BillingAccountPage(accounts=(), next_cursor=None).next_cursor is None


def test_instancia_limites_de_quota_com_todos_os_campos() -> None:
    quotas = QuotaLimits(1, 2, 3, 4, 5, 6)
    assert astuple(quotas) == (1, 2, 3, 4, 5, 6)
    assert QuotaLimits(None, None, None, None, None, None).max_tenants is None


@pytest.mark.parametrize("field", list(QuotaLimits.__dataclass_fields__))
def test_rejeita_limite_de_quota_negativo(field: str) -> None:
    with pytest.raises(ValueError, match=f"negative_value field={field}"):
        replace(_QUOTAS, **{field: -1})


def test_instancia_plano_com_todos_os_campos() -> None:
    plan = PlanVersion(
        plan_version_id="plan-v2",
        plan_key="enterprise",
        stripe_product_id=None,
        stripe_price_ids=(),
        features=frozenset(),
        quotas=_QUOTAS,
        grace_period_days=0,
        effective_from=_LATER,
    )
    assert (plan.plan_key, plan.stripe_product_id) == ("enterprise", None)
    assert (plan.stripe_price_ids, plan.features) == ((), frozenset())
    assert (plan.quotas, plan.grace_period_days, plan.effective_from) == (_QUOTAS, 0, _LATER)


@pytest.mark.parametrize(
    "overrides",
    [{"plan_version_id": ""}, {"plan_key": ""}, {"stripe_product_id": ""},
     {"stripe_price_ids": ("p", "p")}, {"stripe_price_ids": ("",)},
     {"features": frozenset({" "})}, {"grace_period_days": -1}, {"effective_from": _NAIVE},],
)
def test_rejeita_plano_invalido(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="reason="):
        _plan(**overrides)


def test_plano_local_sem_medicao_aceita_quotas_nulas() -> None:
    empty = QuotaLimits(None, None, None, None, None, None)
    plan = _plan(plan_key=LOCAL_UNMETERED_PLAN_KEY, quotas=empty)
    assert plan.quotas.max_agents is None


@pytest.mark.parametrize("field", list(QuotaLimits.__dataclass_fields__))
def test_rejeita_plano_stripe_com_quota_nula(field: str) -> None:
    with pytest.raises(ValueError, match="stripe_plan_requires_quotas"):
        _plan(quotas=replace(_QUOTAS, **{field: None}))


def test_instancia_snapshot_com_todos_os_campos() -> None:
    snapshot = EntitlementSnapshot(
        billing_account_id="ba-1",
        stripe_subscription_id=None,
        subscription_status=SubscriptionStatus.PAST_DUE,
        cancel_at_period_end=True,
        plan_version_id="plan-v1",
        features=frozenset({"a"}),
        quotas=_QUOTAS,
        period_start=_NOW,
        period_end=_LATER,
        grace_until=None,
        valid_until=_LATER,
        entitlement_version=1,
        updated_at=_NOW,
        source_event_id="evt_9",
    )
    assert (snapshot.stripe_subscription_id, snapshot.grace_until) == (None, None)
    assert snapshot.subscription_status is SubscriptionStatus.PAST_DUE
    assert (snapshot.cancel_at_period_end, snapshot.features) == (True, {"a"})
    assert snapshot.source_event_id == "evt_9"


@pytest.mark.parametrize(
    "overrides",
    [{"billing_account_id": ""}, {"plan_version_id": ""}, {"source_event_id": ""},
     {"stripe_subscription_id": ""}, {"features": frozenset({""})}, {"period_start": _NAIVE},
     {"period_end": _NAIVE}, {"valid_until": _NAIVE}, {"updated_at": _NAIVE},
     {"grace_until": _NAIVE}, {"entitlement_version": 0},],
)
def test_rejeita_snapshot_invalido(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="reason="):
        _snapshot(**overrides)


def test_instancia_decisao_com_todos_os_campos() -> None:
    decision = EntitlementDecision(
        action=EntitlementAction.PUBLISH_RUN,
        allowed=False,
        access_level=AccessLevel.READ_ONLY,
        reason="quota_exceeded",
        entitlement_version=4,
        quota_limit=None,
    )
    assert decision.action is EntitlementAction.PUBLISH_RUN
    assert decision.access_level is AccessLevel.READ_ONLY
    assert (decision.allowed, decision.reason) == (False, "quota_exceeded")
    assert (decision.entitlement_version, decision.quota_limit) == (4, None)


@pytest.mark.parametrize(
    "overrides",
    [{"reason": ""}, {"entitlement_version": 0}, {"quota_limit": -1}],
)
def test_rejeita_decisao_invalida(overrides: dict[str, Any]) -> None:
    fields = {
        "action": EntitlementAction.CREATE_RUN, "allowed": True, "access_level": AccessLevel.FULL,
        "reason": "ok", "entitlement_version": 1, "quota_limit": 5,
    }
    with pytest.raises(ValueError, match="reason="):
        EntitlementDecision(**{**fields, **overrides})


def _run_auth(**overrides: Any) -> RunAuthorization:
    fields = {
        "billing_account_id": "ba-1", "plan_version_id": "plan-v1", "entitlement_version": 2,
        "max_concurrency": 3, "budget_reservation_id": "res-1", "authorized_at": _NOW,
    }
    return RunAuthorization(**{**fields, **overrides})


def test_instancia_autorizacao_de_run_com_todos_os_campos() -> None:
    auth = RunAuthorization("ba-1", "plan-v1", 2, 3, None, _NOW)
    assert (auth.billing_account_id, auth.plan_version_id) == ("ba-1", "plan-v1")
    assert (auth.entitlement_version, auth.max_concurrency) == (2, 3)
    assert auth.budget_reservation_id is None
    assert auth.authorized_at == _NOW


@pytest.mark.parametrize(
    "overrides",
    [{"billing_account_id": ""}, {"plan_version_id": ""}, {"entitlement_version": 0},
     {"max_concurrency": 0}, {"budget_reservation_id": ""}, {"authorized_at": _NAIVE},],
)
def test_rejeita_autorizacao_de_run_invalida(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="reason="):
        _run_auth(**overrides)


def test_instancia_autorizacao_analytics_com_todos_os_campos() -> None:
    auth = AnalyticsAuthorization("ba-1", 2, "res-2", 0, _NOW)
    assert (auth.billing_account_id, auth.entitlement_version, auth.authorized_at) == (
        "ba-1", 2, _NOW,
    )
    assert (auth.budget_reservation_id, auth.max_scan_bytes) == ("res-2", 0)


@pytest.mark.parametrize(
    "overrides",
    [{"billing_account_id": ""}, {"entitlement_version": 0}, {"budget_reservation_id": ""},
     {"max_scan_bytes": -1}, {"authorized_at": _NAIVE},],
)
def test_rejeita_autorizacao_analytics_invalida(overrides: dict[str, Any]) -> None:
    fields = {
        "billing_account_id": "ba-1", "entitlement_version": 1, "budget_reservation_id": None,
        "max_scan_bytes": 5, "authorized_at": _NOW,
    }
    with pytest.raises(ValueError, match="reason="):
        AnalyticsAuthorization(**{**fields, **overrides})


def test_instancia_reserva_de_capacidade_com_todos_os_campos() -> None:
    cap = CapacityReservation(
        "cap-2", "ba-1", "tenant-1", CapacityKind.TENANT, ReservationStatus.CONSUMED, _NOW, _NOW
    )
    assert (cap.reservation_id, cap.resource_id) == ("cap-2", "tenant-1")
    assert (cap.kind, cap.status) == (CapacityKind.TENANT, ReservationStatus.CONSUMED)
    assert cap.expires_at == cap.created_at


@pytest.mark.parametrize(
    "overrides",
    [{"reservation_id": ""}, {"billing_account_id": ""}, {"resource_id": ""},
     {"created_at": _NAIVE}, {"expires_at": _NAIVE},],
)
def test_rejeita_reserva_de_capacidade_invalida(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="reason="):
        _capacity(**overrides)


def test_instancia_reserva_de_quota_com_todos_os_campos() -> None:
    res = QuotaReservation(
        reservation_id="res-7",
        billing_account_id="ba-1",
        resource_id="q-1",
        kind=ReservationKind.ANALYTICS,
        period_start=_NOW,
        reserved_runs=0,
        reserved_scan_bytes=50,
        consumed_runs=0,
        consumed_scan_bytes=20,
        status=ReservationStatus.CONSUMED,
        created_at=_NOW,
        expires_at=_LATER,
    )
    assert (res.kind, res.status) == (ReservationKind.ANALYTICS, ReservationStatus.CONSUMED)
    assert (res.reserved_runs, res.reserved_scan_bytes) == (0, 50)
    assert (res.consumed_runs, res.consumed_scan_bytes) == (0, 20)


@pytest.mark.parametrize(
    "overrides",
    [{"reservation_id": ""}, {"billing_account_id": ""}, {"resource_id": ""},
     {"period_start": _NAIVE}, {"created_at": _NAIVE}, {"expires_at": _NAIVE},
     {"reserved_runs": -1}, {"reserved_scan_bytes": -1}, {"consumed_runs": -1},
     {"consumed_scan_bytes": -1},],
)
def test_rejeita_reserva_de_quota_invalida(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="reason="):
        _quota_reservation(**overrides)


_BEFORE = _NOW - timedelta(seconds=1)


@pytest.mark.parametrize(
    ("reason", "build"),
    [
        ("updated_before_created", lambda: _account(updated_at=_BEFORE)),
        ("blank_value", lambda: BillingAccountPage(accounts=(), next_cursor="")),
        ("valid_until_before_updated_at", lambda: _snapshot(valid_until=_BEFORE)),
        ("grace_until_before_period_start", lambda: _snapshot(grace_until=_BEFORE)),
        ("expires_before_created", lambda: _capacity(expires_at=_BEFORE)),
        ("expires_before_created", lambda: _quota_reservation(expires_at=_BEFORE)),
    ],
)
def test_rejeita_invariantes_entre_campos(reason: str, build: Callable[[], object]) -> None:
    with pytest.raises(ValueError, match=reason):
        build()


def test_instancia_evento_de_audit_com_todos_os_campos() -> None:
    event = BillingAuditEvent(
        event_id="aud-2",
        event_type="owner_transferred",
        aggregate_id="ba-1",
        actor_id="admin",
        reason_code="support",
        occurred_at=_NOW,
        attributes={"n": 1},
    )
    assert (event.event_id, event.event_type) == ("aud-2", "owner_transferred")
    assert (event.aggregate_id, event.actor_id) == ("ba-1", "admin")
    assert (event.reason_code, event.occurred_at) == ("support", _NOW)
    assert dict(event.attributes) == {"n": 1}


@pytest.mark.parametrize(
    "overrides",
    [{"event_id": ""}, {"event_type": ""}, {"aggregate_id": ""}, {"actor_id": ""},
     {"reason_code": ""}, {"occurred_at": _NAIVE}, {"attributes": {"k": 1.5}},],
)
def test_rejeita_evento_de_audit_invalido(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="reason="):
        _audit(**overrides)


def test_atributos_de_audit_sao_imutaveis() -> None:
    source = {"version": 1}
    event = _audit(attributes=source)
    source["version"] = 2
    source["extra"] = "x"
    assert dict(event.attributes) == {"version": 1}
    with pytest.raises(TypeError):
        event.attributes["version"] = 9  # type: ignore[index]


def test_instancia_metrica_com_todos_os_campos() -> None:
    metric = BillingMetric(
        name="scan_bytes",
        value=42,
        unit="bytes",
        dimensions={"tenant": "t1"},
        occurred_at=_NOW,
    )
    assert (metric.name, metric.value, metric.unit) == ("scan_bytes", 42, "bytes")
    assert (dict(metric.dimensions), metric.occurred_at) == ({"tenant": "t1"}, _NOW)


@pytest.mark.parametrize(
    "overrides",
    [{"name": ""}, {"unit": ""}, {"value": float("nan")}, {"value": float("inf")},
     {"occurred_at": _NAIVE}, {"dimensions": {"k": ""}},],
)
def test_rejeita_metrica_invalida(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="reason="):
        _metric(**overrides)


def test_dimensoes_de_metrica_sao_imutaveis() -> None:
    source = {"plan": "pro"}
    metric = _metric(dimensions=source)
    source["plan"] = "free"
    source["extra"] = "x"
    assert dict(metric.dimensions) == {"plan": "pro"}
    with pytest.raises(TypeError):
        metric.dimensions["plan"] = "free"  # type: ignore[index]
