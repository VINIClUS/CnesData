"""Testes da forma de runtime dos ports de billing."""

import inspect
from collections.abc import Callable
from datetime import UTC, datetime
from typing import get_type_hints

import pytest

from cnes_domain.billing import ports
from cnes_domain.billing.commands import (
    AttachStripeCustomerCommand,
    CapacityReservationCommand,
    CheckoutCommand,
    ConsumeCapacityCommand,
    ConsumeReservationCommand,
    CreateBillingAccountCommand,
    CreateStripeCustomerCommand,
    LinkBillingTenantCommand,
    PortalCommand,
    ReleaseCapacityCommand,
    ReleasePendingCheckoutCommand,
    ReleaseReservationCommand,
    ReserveAnalyticsCommand,
    ReservePendingCheckoutCommand,
    ReserveRunCommand,
    SnapshotWrite,
    StripeStateRequest,
    TransferOwnerCommand,
)
from cnes_domain.billing.inbox import (
    InboxClaim,
    StripeEvent,
    StripeEventListRequest,
    StripeRecoveryCursor,
)
from cnes_domain.billing.models import ReadConsistency

_EXPECTED: dict[type, dict[str, tuple[str, ...]]] = {
    ports.EntitlementProjectionPort: {
        "get_snapshot": ("billing_account_id", "consistency"),
        "compare_and_set_snapshot": ("command",),
        "commit_claimed_snapshot": ("claim", "command"),
    },
    ports.BillingCatalogPort: {
        "create_account": ("command",),
        "get_account": ("billing_account_id",),
        "get_account_by_customer": ("stripe_customer_id",),
        "list_stripe_accounts": ("limit", "cursor"),
        "get_tenant_link": ("billing_account_id", "tenant_id", "consistency"),
        "get_tenant_account": ("tenant_id", "consistency"),
        "link_tenant": ("command",),
        "attach_customer": ("command",),
        "transfer_owner": ("command",),
        "publish_plan": ("plan",),
        "get_plan": ("plan_version_id",),
        "get_plan_by_price": ("stripe_price_id",),
        "reserve_pending_checkout": ("command",),
        "release_pending_checkout": ("command",),
    },
    ports.QuotaReservationPort: {
        "reserve_and_create_run": ("command",),
        "reserve_analytics": ("command",),
        "reserve_capacity": ("command",),
        "consume_capacity": ("command",),
        "release_capacity": ("command",),
        "consume": ("command",),
        "release": ("command",),
    },
    ports.StripeGatewayPort: {
        "create_customer": ("command",),
        "create_checkout": ("command",),
        "create_portal": ("command",),
        "get_current_state": ("request",),
        "list_events": ("request",),
    },
    ports.RecoveryCursorPort: {
        "load": ("consistency",),
        "start": ("cursor",),
        "advance": ("expected", "replacement"),
        "complete": ("expected", "completed_at"),
    },
    ports.WebhookInboxPort: {
        "accept": ("event",),
        "claim": ("event_id", "now"),
        "mark_processed": ("claim", "entitlement_version"),
        "mark_failed": ("claim", "error_code", "retryable"),
        "get_state": ("event_id", "consistency"),
        "get_recovery_record": ("event_id", "consistency"),
        "list_recoverable": ("now", "limit"),
    },
    ports.SecretProviderPort: {"get_secret": ("secret_arn",)},
    ports.BillingAuditPort: {"append": ("event",)},
    ports.BillingMetricsPort: {"emit": ("metric",)},
}

_ARGUMENT_TYPES: dict[tuple[type, str], tuple[object, ...]] = {
    (ports.EntitlementProjectionPort, "get_snapshot"): (str, ReadConsistency),
    (ports.EntitlementProjectionPort, "compare_and_set_snapshot"): (SnapshotWrite,),
    (ports.EntitlementProjectionPort, "commit_claimed_snapshot"): (InboxClaim, SnapshotWrite),
    (ports.BillingCatalogPort, "create_account"): (CreateBillingAccountCommand,),
    (ports.BillingCatalogPort, "link_tenant"): (LinkBillingTenantCommand,),
    (ports.BillingCatalogPort, "attach_customer"): (AttachStripeCustomerCommand,),
    (ports.BillingCatalogPort, "transfer_owner"): (TransferOwnerCommand,),
    (ports.BillingCatalogPort, "reserve_pending_checkout"): (ReservePendingCheckoutCommand,),
    (ports.BillingCatalogPort, "release_pending_checkout"): (ReleasePendingCheckoutCommand,),
    (ports.QuotaReservationPort, "reserve_and_create_run"): (ReserveRunCommand,),
    (ports.QuotaReservationPort, "reserve_analytics"): (ReserveAnalyticsCommand,),
    (ports.QuotaReservationPort, "reserve_capacity"): (CapacityReservationCommand,),
    (ports.QuotaReservationPort, "consume_capacity"): (ConsumeCapacityCommand,),
    (ports.QuotaReservationPort, "release_capacity"): (ReleaseCapacityCommand,),
    (ports.QuotaReservationPort, "consume"): (ConsumeReservationCommand,),
    (ports.QuotaReservationPort, "release"): (ReleaseReservationCommand,),
    (ports.StripeGatewayPort, "create_customer"): (CreateStripeCustomerCommand,),
    (ports.StripeGatewayPort, "create_checkout"): (CheckoutCommand,),
    (ports.StripeGatewayPort, "create_portal"): (PortalCommand,),
    (ports.StripeGatewayPort, "get_current_state"): (StripeStateRequest,),
    (ports.StripeGatewayPort, "list_events"): (StripeEventListRequest,),
    (ports.RecoveryCursorPort, "advance"): (StripeRecoveryCursor, StripeRecoveryCursor),
    (ports.WebhookInboxPort, "accept"): (StripeEvent,),
    (ports.WebhookInboxPort, "mark_failed"): (InboxClaim, str, bool),
}


def _fake(protocol: type) -> object:
    methods = {name: lambda self, *args: None for name in _EXPECTED[protocol]}
    return type(f"Fake{protocol.__name__}", (), methods)()


@pytest.mark.parametrize("protocol", list(_EXPECTED))
def test_port_expoe_exatamente_os_metodos_canonicos(protocol: type) -> None:
    public = {
        name for name, value in vars(protocol).items()
        if callable(value) and not name.startswith("_")
    }
    assert public == set(_EXPECTED[protocol])


@pytest.mark.parametrize(
    ("protocol", "method"),
    [(protocol, method) for protocol, methods in _EXPECTED.items() for method in methods],
)
def test_metodo_do_port_usa_parametros_canonicos(protocol: type, method: str) -> None:
    declared = getattr(protocol, method)
    parameters = tuple(inspect.signature(declared).parameters)
    assert parameters == ("self", *_EXPECTED[protocol][method])
    assert declared(object(), *(None for _ in _EXPECTED[protocol][method])) is None


@pytest.mark.parametrize(("key", "types"), list(_ARGUMENT_TYPES.items()))
def test_metodo_do_port_tipa_argumentos_com_o_catalogo(
    key: tuple[type, str], types: tuple[object, ...],
) -> None:
    protocol, method = key
    hints = get_type_hints(getattr(protocol, method))
    assert tuple(hints[name] for name in _EXPECTED[protocol][method]) == types


@pytest.mark.parametrize("protocol", list(_EXPECTED))
def test_port_e_verificavel_em_runtime(protocol: type) -> None:
    assert isinstance(_fake(protocol), protocol)
    assert not isinstance(object(), protocol)


def test_clock_port_e_callable_sem_argumentos() -> None:
    assert ports.ClockPort == Callable[[], datetime]
    clock: ports.ClockPort = lambda: datetime(2026, 9, 1, tzinfo=UTC)  # noqa: E731
    assert clock().tzinfo is UTC
