"""Testes dos comandos e requisições de billing."""

from dataclasses import fields
from datetime import UTC, datetime
from typing import Any

import pytest

from cnes_domain.billing import commands as cmd
from cnes_domain.billing.models import (
    BillingAccount,
    BillingAccountStatus,
    BillingAccountTenantLink,
    BillingAuditEvent,
    CapacityKind,
    EntitlementSnapshot,
    PlanVersion,
    QuotaLimits,
    RunAuthorization,
    SubscriptionStatus,
)
from cnes_domain.control_plane.entities import RunDependency, Tenant

_NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)
_LATER = datetime(2026, 10, 1, 12, tzinfo=UTC)
_NAIVE = _NOW.replace(tzinfo=None)
_SHA = "a" * 64
_QUOTAS = QuotaLimits(
    max_tenants=1,
    max_agents=2,
    max_runs_per_period=3,
    max_concurrency=4,
    retention_days=5,
    athena_scan_budget_bytes=6,
)


def _account(**overrides: Any) -> BillingAccount:
    values = {
        "billing_account_id": "acc_1",
        "stripe_customer_id": "cus_1",
        "owner_user_id": "user_1",
        "status": BillingAccountStatus.ACTIVE,
        "created_at": _NOW,
        "updated_at": _NOW,
    }
    return BillingAccount(**{**values, **overrides})


def _link(**overrides: Any) -> BillingAccountTenantLink:
    values = {
        "billing_account_id": "acc_1",
        "tenant_id": "354130",
        "linked_by_user_id": "user_1",
        "reason_code": "onboarding",
        "linked_at": _NOW,
    }
    return BillingAccountTenantLink(**{**values, **overrides})


def _tenant(tenant_id: str = "354130") -> Tenant:
    return Tenant(tenant_id=tenant_id, municipality_name="Presidente Epitácio", created_at=_NOW)


def _dep(source_type: str = "cnes", file_subtype: str = "profissionais") -> RunDependency:
    return RunDependency(source_type=source_type, file_subtype=file_subtype, required=True)


def _snapshot(**overrides: Any) -> EntitlementSnapshot:
    values = {
        "billing_account_id": "acc_1",
        "stripe_subscription_id": "sub_1",
        "subscription_status": SubscriptionStatus.ACTIVE,
        "cancel_at_period_end": False,
        "plan_version_id": "plan_v1",
        "features": frozenset({"runs"}),
        "quotas": _QUOTAS,
        "period_start": _NOW,
        "period_end": _LATER,
        "grace_until": None,
        "valid_until": _LATER,
        "entitlement_version": 4,
        "updated_at": _NOW,
        "source_event_id": "evt_1",
    }
    return EntitlementSnapshot(**{**values, **overrides})


def _audit() -> BillingAuditEvent:
    return BillingAuditEvent(
        event_id="aud_1",
        event_type="snapshot.written",
        aggregate_id="acc_1",
        actor_id="system",
        reason_code="webhook",
        occurred_at=_NOW,
        attributes={"k": "v"},
    )


def _authorization(**overrides: Any) -> RunAuthorization:
    values = {
        "billing_account_id": "acc_1",
        "plan_version_id": "plan_v1",
        "entitlement_version": 4,
        "max_concurrency": 2,
        "budget_reservation_id": None,
        "authorized_at": _NOW,
    }
    return RunAuthorization(**{**values, **overrides})


def _plan() -> PlanVersion:
    return PlanVersion(
        plan_version_id="plan_v1",
        plan_key="pro",
        stripe_product_id="prod_1",
        stripe_price_ids=("price_1",),
        features=frozenset({"runs"}),
        quotas=_QUOTAS,
        grace_period_days=7,
        effective_from=_NOW,
    )


def _run_request(**overrides: Any) -> cmd.CreateRunRequest:
    values = {
        "billing_account_id": "acc_1",
        "tenant_id": "354130",
        "run_id": "run_1",
        "competencia": "2026-08",
        "dataset_name": "cnes",
        "dependencies": (_dep(),),
        "idempotency_key": "idem_1",
        "request_hash": _SHA,
        "requested_concurrency": 1,
        "estimated_scan_bytes": 0,
    }
    return cmd.CreateRunRequest(**{**values, **overrides})


def _analytics_request(**overrides: Any) -> cmd.AnalyticsRequest:
    values = {
        "billing_account_id": "acc_1",
        "tenant_id": "354130",
        "query_id": "q_1",
        "idempotency_key": "idem_1",
        "request_hash": _SHA,
        "estimated_scan_bytes": 10,
    }
    return cmd.AnalyticsRequest(**{**values, **overrides})


def _build(cls: type, values: dict[str, Any], overrides: dict[str, Any]) -> Any:
    return cls(**{**values, **overrides})


def _transfer(**o: Any) -> cmd.TransferOwnerCommand:
    values = {
        "billing_account_id": "acc_1",
        "expected_owner_user_id": "user_1",
        "new_owner_user_id": "user_2",
        "actor_id": "admin_1",
        "reason_code": "handover",
        "transferred_at": _NOW,
    }
    return _build(cmd.TransferOwnerCommand, values, o)


def _create_account(**o: Any) -> cmd.CreateBillingAccountCommand:
    values = {"account": _account(), "initial_tenant_link": _link(), "idempotency_key": "i1"}
    return _build(cmd.CreateBillingAccountCommand, values, o)


def _billed_tenant(**o: Any) -> cmd.CreateBilledTenantCommand:
    values = {
        "tenant": _tenant(),
        "link": _link(),
        "reservation_id": "res_1",
        "idempotency_key": "i1",
    }
    return _build(cmd.CreateBilledTenantCommand, values, o)


def _publish(**o: Any) -> cmd.PublishGateRequest:
    values = {
        "billing_account_id": "acc_1",
        "tenant_id": "354130",
        "run_id": "run_1",
        "expected_entitlement_version": 4,
        "expected_fencing_token": 0,
    }
    return _build(cmd.PublishGateRequest, values, o)


def _write(**o: Any) -> cmd.SnapshotWrite:
    values = {"expected_version": 3, "snapshot": _snapshot(), "audit_events": (_audit(),)}
    return _build(cmd.SnapshotWrite, values, o)


def _reserve_run(**o: Any) -> cmd.ReserveRunCommand:
    values = {
        "request": _run_request(),
        "snapshot": _snapshot(),
        "deployment_max_concurrency": 2,
        "reservation_id": "res_1",
        "expires_at": _LATER,
    }
    return _build(cmd.ReserveRunCommand, values, o)


def _reserve_analytics(**o: Any) -> cmd.ReserveAnalyticsCommand:
    values = {
        "request": _analytics_request(),
        "snapshot": _snapshot(),
        "reservation_id": "res_1",
        "expires_at": _LATER,
    }
    return _build(cmd.ReserveAnalyticsCommand, values, o)


def _authorized(**o: Any) -> cmd.AuthorizedRunCommand:
    values = {"request": _run_request(), "authorization": _authorization()}
    return _build(cmd.AuthorizedRunCommand, values, o)


def _capacity(**o: Any) -> cmd.CapacityReservationCommand:
    values = {
        "billing_account_id": "acc_1",
        "tenant_id": "354130",
        "resource_id": "agent_1",
        "kind": CapacityKind.AGENT,
        "idempotency_key": "i1",
        "request_hash": _SHA,
        "entitlement_version": 4,
        "limit": 5,
    }
    return _build(cmd.CapacityReservationCommand, values, o)


def _checkout(**o: Any) -> cmd.CheckoutCommand:
    values = {
        "billing_account_id": "acc_1",
        "stripe_customer_id": "cus_1",
        "plan_version": _plan(),
        "idempotency_key": "i1",
    }
    return _build(cmd.CheckoutCommand, values, o)


def _state(**o: Any) -> cmd.StripeBillingState:
    values = {
        "stripe_customer_id": "cus_1",
        "stripe_subscription_id": "sub_1",
        "subscription_status": SubscriptionStatus.ACTIVE,
        "cancel_at_period_end": False,
        "stripe_price_id": "price_1",
        "active_features": frozenset({"runs"}),
        "period_start": _NOW,
        "period_end": _LATER,
        "latest_invoice_id": "in_1",
    }
    return _build(cmd.StripeBillingState, values, o)


def _factory(cls: type, **defaults: Any) -> Any:
    def make(**overrides: Any) -> Any:
        return cls(**{**defaults, **overrides})

    make.__name__ = f"_{cls.__name__}"
    return make


_gate = _factory(cmd.GateRequest, billing_account_id="acc_1", tenant_id="354130")
_link_cmd = _factory(
    cmd.LinkBillingTenantCommand, link=_link(), expected_account_updated_at=_NOW,
    idempotency_key="i1",
)  # fmt: skip
_attach = _factory(
    cmd.AttachStripeCustomerCommand, billing_account_id="acc_1", stripe_customer_id="cus_1",
    expected_updated_at=_NOW,
)  # fmt: skip
_release_capacity = _factory(
    cmd.ReleaseCapacityCommand, billing_account_id="acc_1", reservation_id="res_1",
    released_at=_NOW, reason_code="failed",
)  # fmt: skip
_release_reservation = _factory(
    cmd.ReleaseReservationCommand, billing_account_id="acc_1", reservation_id="res_1",
    released_at=_NOW, reason_code="failed",
)  # fmt: skip
_consume_capacity = _factory(
    cmd.ConsumeCapacityCommand, billing_account_id="acc_1", reservation_id="res_1",
    consumed_at=_NOW,
)  # fmt: skip
_consume_reservation = _factory(
    cmd.ConsumeReservationCommand, billing_account_id="acc_1", reservation_id="res_1",
    actual_scan_bytes=7, consumed_at=_NOW,
)  # fmt: skip
_create_customer = _factory(
    cmd.CreateStripeCustomerCommand, billing_account_id="acc_1", idempotency_key="i1",
)  # fmt: skip
_customer = _factory(cmd.StripeCustomer, stripe_customer_id="cus_1")
_portal = _factory(
    cmd.PortalCommand, billing_account_id="acc_1", stripe_customer_id="cus_1",
    idempotency_key="i1",
)  # fmt: skip
_hosted = _factory(cmd.HostedSession, session_id="cs_1", url="https://stripe.example/s/1")
_state_request = _factory(
    cmd.StripeStateRequest, stripe_customer_id="cus_1", stripe_subscription_id="sub_1",
)  # fmt: skip


_FACTORIES = [
    _transfer, _create_account, _link_cmd, _attach, _billed_tenant, _gate, _run_request,
    _analytics_request, _publish, _write, _reserve_run, _reserve_analytics, _authorized,
    _capacity, _release_capacity, _consume_capacity, _consume_reservation,
    _release_reservation, _checkout, _create_customer, _customer, _portal, _hosted,
    _state_request, _state,
]  # fmt: skip


@pytest.mark.parametrize("factory", _FACTORIES, ids=lambda f: f.__name__)
def test_instancia_todos_os_campos_explicitamente(factory: Any) -> None:
    instance = factory()
    assert len(fields(instance)) >= 1
    for field in fields(instance):
        assert getattr(instance, field.name) is not None or field.name in {
            "stripe_subscription_id",
            "latest_invoice_id",
            "limit",
        }


def test_transfer_owner_command_preserva_todos_os_campos() -> None:
    command = cmd.TransferOwnerCommand(
        billing_account_id="acc_1",
        expected_owner_user_id="user_1",
        new_owner_user_id="user_2",
        actor_id="admin_1",
        reason_code="handover",
        transferred_at=_NOW,
    )
    assert command.new_owner_user_id == "user_2"
    assert command.transferred_at == _NOW


def test_transfer_owner_command_exige_novo_owner_diferente() -> None:
    with pytest.raises(ValueError, match="reason=owner_unchanged"):
        _transfer(new_owner_user_id="user_1")


def test_criacao_de_conta_exige_link_da_mesma_conta() -> None:
    with pytest.raises(ValueError, match="reason=account_link_mismatch"):
        _create_account(initial_tenant_link=_link(billing_account_id="acc_2"))


def test_criacao_de_conta_aceita_conta_sem_tenant_inicial() -> None:
    command = _create_account(initial_tenant_link=None)
    assert command.initial_tenant_link is None


def test_tenant_faturado_exige_link_do_mesmo_tenant() -> None:
    with pytest.raises(ValueError, match="reason=tenant_link_mismatch"):
        _billed_tenant(link=_link(tenant_id="999999"))


def test_create_run_request_rejeita_dependencias_duplicadas() -> None:
    with pytest.raises(ValueError, match="reason=duplicate_dependency"):
        _run_request(dependencies=(_dep(), _dep()))


def test_create_run_request_aceita_mesmo_source_com_subtipos_distintos() -> None:
    request = _run_request(dependencies=(_dep(), _dep(file_subtype="estabelecimentos")))
    assert len(request.dependencies) == 2


def test_create_run_request_exige_dependencias() -> None:
    with pytest.raises(ValueError, match="reason=dependencies_required"):
        _run_request(dependencies=())


def test_snapshot_write_exige_versao_sucessora() -> None:
    with pytest.raises(ValueError, match="reason=snapshot_version_not_successor"):
        _write(expected_version=4)


def test_snapshot_write_aceita_criacao_a_partir_de_versao_zero() -> None:
    write = _write(expected_version=0, snapshot=_snapshot(entitlement_version=1))
    assert write.snapshot.entitlement_version == 1


def test_snapshot_write_exige_audit_events_como_tupla() -> None:
    with pytest.raises(ValueError, match="reason=audit_events_not_tuple"):
        _write(audit_events=[_audit()])


def test_hosted_session_exige_https() -> None:
    with pytest.raises(ValueError, match="reason=hosted_session_url_not_https"):
        _hosted(url="http://stripe.example/s/1")


def test_reserva_de_run_exige_snapshot_da_mesma_conta() -> None:
    with pytest.raises(ValueError, match="reason=snapshot_account_mismatch"):
        _reserve_run(snapshot=_snapshot(billing_account_id="acc_2"))


def test_reserva_de_analytics_exige_snapshot_da_mesma_conta() -> None:
    with pytest.raises(ValueError, match="reason=snapshot_account_mismatch"):
        _reserve_analytics(snapshot=_snapshot(billing_account_id="acc_2"))


def test_run_autorizado_exige_autorizacao_da_mesma_conta() -> None:
    with pytest.raises(ValueError, match="reason=authorization_account_mismatch"):
        _authorized(authorization=_authorization(billing_account_id="acc_2"))


def test_stripe_billing_state_rejeita_periodo_invertido() -> None:
    with pytest.raises(ValueError, match="reason=period_end_before_start"):
        _state(period_start=_LATER, period_end=_NOW)


@pytest.mark.parametrize("value", [1, "yes", None])
def test_stripe_billing_state_exige_cancel_at_period_end_bool(value: object) -> None:
    with pytest.raises(ValueError, match="reason=cancel_at_period_end_not_bool"):
        _state(cancel_at_period_end=value)


def test_stripe_billing_state_rejeita_feature_em_branco() -> None:
    with pytest.raises(ValueError, match="field=active_features"):
        _state(active_features=frozenset({" "}))


def test_stripe_billing_state_aceita_fatura_ausente() -> None:
    assert _state(latest_invoice_id=None).latest_invoice_id is None


_INVALID_FIELDS: list[tuple[Any, dict[str, Any], str]] = [
    (_link_cmd, {"expected_account_updated_at": _NAIVE}, "datetime_not_utc"),
    (_link_cmd, {"idempotency_key": ""}, "blank_value"),
    (_create_account, {"idempotency_key": " "}, "blank_value"),
    (_transfer, {"actor_id": ""}, "blank_value"),
    (_transfer, {"transferred_at": _NAIVE}, "datetime_not_utc"),
    (_attach, {"stripe_customer_id": ""}, "blank_value"),
    (_attach, {"expected_updated_at": _NAIVE}, "datetime_not_utc"),
    (_billed_tenant, {"reservation_id": ""}, "blank_value"),
    (_billed_tenant, {"idempotency_key": ""}, "blank_value"),
    (_gate, {"tenant_id": ""}, "blank_value"),
    (_run_request, {"run_id": ""}, "blank_value"),
    (_run_request, {"dataset_name": " "}, "blank_value"),
    (_run_request, {"competencia": "2026-13"}, "invalid_competencia"),
    (_run_request, {"request_hash": "abc"}, "invalid_sha256"),
    (_run_request, {"requested_concurrency": 0}, "positive_value_required"),
    (_run_request, {"estimated_scan_bytes": -1}, "negative_value"),
    (_analytics_request, {"query_id": ""}, "blank_value"),
    (_analytics_request, {"request_hash": "abc"}, "invalid_sha256"),
    (_analytics_request, {"estimated_scan_bytes": -1}, "negative_value"),
    (_publish, {"run_id": ""}, "blank_value"),
    (_publish, {"expected_entitlement_version": 0}, "positive_value_required"),
    (_publish, {"expected_fencing_token": -1}, "negative_value"),
    (_write, {"expected_version": -1}, "negative_value"),
    (_reserve_run, {"deployment_max_concurrency": 0}, "positive_value_required"),
    (_reserve_run, {"reservation_id": ""}, "blank_value"),
    (_reserve_run, {"expires_at": _NAIVE}, "datetime_not_utc"),
    (_reserve_analytics, {"reservation_id": ""}, "blank_value"),
    (_reserve_analytics, {"expires_at": _NAIVE}, "datetime_not_utc"),
    (_capacity, {"resource_id": ""}, "blank_value"),
    (_capacity, {"request_hash": "abc"}, "invalid_sha256"),
    (_capacity, {"entitlement_version": 0}, "positive_value_required"),
    (_capacity, {"limit": -1}, "negative_value"),
    (_release_capacity, {"reason_code": ""}, "blank_value"),
    (_release_capacity, {"released_at": _NAIVE}, "datetime_not_utc"),
    (_consume_capacity, {"reservation_id": ""}, "blank_value"),
    (_consume_capacity, {"consumed_at": _NAIVE}, "datetime_not_utc"),
    (_consume_reservation, {"reservation_id": ""}, "blank_value"),
    (_consume_reservation, {"actual_scan_bytes": -1}, "negative_value"),
    (_consume_reservation, {"consumed_at": _NAIVE}, "datetime_not_utc"),
    (_release_reservation, {"reason_code": ""}, "blank_value"),
    (_release_reservation, {"released_at": _NAIVE}, "datetime_not_utc"),
    (_checkout, {"stripe_customer_id": ""}, "blank_value"),
    (_create_customer, {"idempotency_key": ""}, "blank_value"),
    (_customer, {"stripe_customer_id": ""}, "blank_value"),
    (_portal, {"idempotency_key": ""}, "blank_value"),
    (_hosted, {"session_id": ""}, "blank_value"),
    (_hosted, {"url": ""}, "blank_value"),
    (_state_request, {"stripe_customer_id": ""}, "blank_value"),
    (_state_request, {"stripe_subscription_id": ""}, "blank_value"),
    (_state, {"stripe_price_id": ""}, "blank_value"),
    (_state, {"latest_invoice_id": ""}, "blank_value"),
    (_state, {"period_start": _NAIVE}, "datetime_not_utc"),
]


@pytest.mark.parametrize(
    ("factory", "overrides", "reason"),
    _INVALID_FIELDS,
    ids=[f"{f.__name__}-{next(iter(o))}-{r}" for f, o, r in _INVALID_FIELDS],
)
def test_rejeita_campo_invalido(factory: Any, overrides: dict[str, Any], reason: str) -> None:
    with pytest.raises(ValueError, match=f"reason={reason}"):
        factory(**overrides)


def test_aceita_limite_e_subscription_ausentes() -> None:
    assert _capacity(limit=None).limit is None
    assert _state_request(stripe_subscription_id=None).stripe_subscription_id is None
