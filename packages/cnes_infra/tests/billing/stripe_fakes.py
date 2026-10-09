"""Fakes compartilhados dos testes do StripeGateway."""

from collections.abc import Sequence
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from cnes_domain.billing.models import PlanVersion, QuotaLimits
from cnes_infra.billing.stripe_gateway import StripeGateway, StripeGatewayConfig

ORIGIN = "https://app.example.test"
SUCCESS_URL = f"{ORIGIN}/billing/success?session_id={{CHECKOUT_SESSION_ID}}"
CANCEL_URL = f"{ORIGIN}/billing/cancel"
PORTAL_URL = f"{ORIGIN}/billing"
PERIOD_START = 1_788_000_000
PERIOD_END = 1_790_600_000


def make_plan(price_ids: tuple[str, ...] = ("price_01",)) -> PlanVersion:
    return PlanVersion(
        "plan_v1", "basic", "prod_01", price_ids, frozenset({"serving"}),
        QuotaLimits(1, 1, 1, 1, 1, 1), 7, datetime(2026, 9, 1, tzinfo=UTC),
    )


def make_config() -> StripeGatewayConfig:
    return StripeGatewayConfig(SUCCESS_URL, CANCEL_URL, PORTAL_URL, frozenset({ORIGIN}))


def make_gateway(
    plan: PlanVersion | None = None,
) -> tuple[StripeGateway, MagicMock, MagicMock]:
    client = MagicMock()
    client.v1.customers.search.return_value = page([])
    client.v1.subscriptions.list.return_value = page([])
    client.v1.checkout.sessions.list.return_value = page([])
    plans = MagicMock()
    plans.get_plan_by_price.return_value = plan if plan is not None else make_plan()
    return StripeGateway(client, make_config(), plans), client, plans


def page(data: Sequence[object], has_more: bool = False) -> SimpleNamespace:
    return SimpleNamespace(data=data, has_more=has_more)


def make_item(price: object = "price_01") -> SimpleNamespace:
    return SimpleNamespace(
        price=price, current_period_start=PERIOD_START, current_period_end=PERIOD_END,
    )


def make_subscription(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "id": "sub_01",
        "customer": "cus_01",
        "status": "active",
        "cancel_at_period_end": False,
        "items": SimpleNamespace(data=[make_item()]),
        "latest_invoice": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def make_invoice(subscription: object = "sub_01", parent: object = ...) -> SimpleNamespace:
    if parent is ...:
        details = SimpleNamespace(subscription=subscription)
        parent = SimpleNamespace(subscription_details=details)
    return SimpleNamespace(id="in_01", parent=parent)


def make_entitlements(keys: list[str], has_more: bool = False) -> SimpleNamespace:
    data = [SimpleNamespace(id=f"ent_{key}", lookup_key=key) for key in keys]
    return SimpleNamespace(data=data, has_more=has_more)


def make_event(
    event_id: str, obj: SimpleNamespace, event_type: str = "customer.updated",
) -> SimpleNamespace:
    raw = {"id": event_id, "type": event_type, "data": {"object": {"id": obj.id}}}
    return SimpleNamespace(
        id=event_id, type=event_type, created=PERIOD_START,
        data=SimpleNamespace(object=obj), to_dict=lambda: raw,
    )


def make_customer(
    customer_id: str, account: str | None = "ba_01", created: int = PERIOD_START,
    **extra: object,
) -> SimpleNamespace:
    metadata = SimpleNamespace() if account is None else SimpleNamespace(billing_account_id=account)
    return SimpleNamespace(id=customer_id, created=created, metadata=metadata, **extra)
