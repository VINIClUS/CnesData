"""Adaptador Stripe do StripeGatewayPort sobre um cliente injetado estreito."""

import hashlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from re import compile as re_compile
from typing import Any, Protocol
from urllib.parse import urlsplit

from cnes_domain.billing.commands import (
    CheckoutCommand,
    CreateStripeCustomerCommand,
    HostedSession,
    PortalCommand,
    StripeBillingState,
    StripeCustomer,
    StripeStateRequest,
)
from cnes_domain.billing.errors import PermanentBillingError, RetryableBillingError
from cnes_domain.billing.inbox import StripeEvent, StripeEventListRequest, StripeEventPage
from cnes_domain.billing.models import PlanVersion, SubscriptionStatus

logger = logging.getLogger(__name__)

_DETAIL_VALUE = re_compile(r"^[A-Za-z0-9_.:-]+$")
_TRANSIENT_ERRORS = frozenset({"APIConnectionError", "RateLimitError", "APIError"})
_ENTITLEMENT_PAGE = 100
_GUARD_PAGE = 100
_ENDED_STATUSES = frozenset({"canceled", "incomplete_expired"})


class StripeMappingError(RetryableBillingError):
    pass


class PlanPriceLookup(Protocol):
    def get_plan_by_price(self, stripe_price_id: str) -> PlanVersion | None: ...  # pragma: no cover


class _Creatable(Protocol):
    def create(
        self, params: dict[str, Any], options: dict[str, Any],
    ) -> Any: ...  # pragma: no cover


class _Listable(Protocol):
    def list(
        self, params: dict[str, Any], options: dict[str, Any] | None = None,
    ) -> Any: ...  # pragma: no cover


class _Retrievable(Protocol):
    def retrieve(
        self, resource_id: str, /, params: dict[str, Any] | None = None,
        options: dict[str, Any] | None = None,
    ) -> Any: ...  # pragma: no cover


class _Expirable(Protocol):
    def expire(self, session: str, /) -> Any: ...  # pragma: no cover


class _Sessions(Protocol):
    sessions: _Creatable


class _CheckoutSessions(_Creatable, _Listable, _Expirable, Protocol):
    pass


class _Checkout(Protocol):
    sessions: _CheckoutSessions


class _Subscriptions(_Retrievable, _Listable, Protocol):
    pass


class _Entitlements(Protocol):
    active_entitlements: _Listable


class _V1(Protocol):
    customers: _Creatable
    checkout: _Checkout
    billing_portal: _Sessions
    subscriptions: _Subscriptions
    invoices: _Retrievable
    entitlements: _Entitlements
    events: _Listable


class StripeClientProtocol(Protocol):
    v1: _V1


def _url_allowed(url: str, origins: frozenset[str]) -> bool:
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname or "@" in parts.netloc:
        return False
    return f"https://{parts.netloc.lower()}" in origins


def _origin_valid(origin: str) -> bool:
    parts = urlsplit(origin)
    return (
        parts.scheme == "https"
        and bool(parts.hostname)
        and "@" not in parts.netloc
        and not (parts.path or parts.query or parts.fragment)
    )


@dataclass(frozen=True, slots=True)
class StripeGatewayConfig:
    success_url: str
    cancel_url: str
    portal_return_url: str
    allowed_origins: frozenset[str]

    def __post_init__(self) -> None:
        if not self.allowed_origins:
            raise ValueError("reason=return_origins_empty")
        if not all(_origin_valid(origin) for origin in self.allowed_origins):
            raise ValueError("reason=return_origin_invalid")
        origins = frozenset(origin.lower() for origin in self.allowed_origins)
        for name in ("success_url", "cancel_url", "portal_return_url"):
            if not _url_allowed(getattr(self, name), origins):
                raise ValueError(f"reason=return_url_not_allowed field={name}")


def _detail(**pairs: object) -> str | None:
    kept = [
        f"{key}={value}"
        for key, value in pairs.items()
        if value is not None and _DETAIL_VALUE.fullmatch(str(value))
    ]
    return " ".join(kept) or None


def _object_id(value: object) -> str | None:
    if isinstance(value, str):
        return value
    identifier = getattr(value, "id", None)
    return identifier if isinstance(identifier, str) else None


def _invoice_subscription_id(invoice: object) -> str | None:
    parent = getattr(invoice, "parent", None)
    details = getattr(parent, "subscription_details", None)
    return _object_id(getattr(details, "subscription", None))


def _epoch(seconds: int) -> datetime:
    return datetime.fromtimestamp(seconds, tz=UTC)


def _translate(error: Exception, operation: str) -> Exception:
    names = {cls.__name__ for cls in type(error).__mro__}
    if "StripeError" not in names:
        return error
    status = getattr(error, "http_status", None)
    status = status if isinstance(status, int) else None
    logger.warning(
        "stripe_call_failed operation=%s code=%s http_status=%s",
        operation, type(error).__name__, status,
    )
    if names & _TRANSIENT_ERRORS or (status is not None and (status == 429 or status >= 500)):
        return RetryableBillingError("stripe_unavailable")
    return PermanentBillingError("stripe_request_rejected", detail=_detail(http_status=status))


def _call(operation: str, call: Callable[[], Any]) -> Any:
    try:
        return call()
    except Exception as error:
        translated = _translate(error, operation)
        if translated is error:
            raise
        raise translated from None


def _mapping(code: str, **pairs: object) -> StripeMappingError:
    return StripeMappingError(code, detail=_detail(**pairs))


def _event_customer(obj: Any) -> str | None:
    if obj.object == "customer":
        return obj.id
    return _object_id(getattr(obj, "customer", None))


def _event_subscription(obj: Any) -> str | None:
    if obj.object == "subscription":
        return obj.id
    if obj.object == "invoice":
        return _invoice_subscription_id(obj)
    return _object_id(getattr(obj, "subscription", None))


def _payload_hash(event: Any) -> str:
    raw = json.dumps(event.to_dict(), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def _to_stripe_event(event: Any) -> StripeEvent:
    obj = event.data.object
    return StripeEvent(
        event_id=event.id,
        event_type=event.type,
        created_at=_epoch(event.created),
        stripe_customer_id=_event_customer(obj),
        stripe_subscription_id=_event_subscription(obj),
        payload_sha256=_payload_hash(event),
    )


class StripeGateway:
    def __init__(
        self, client: StripeClientProtocol, config: StripeGatewayConfig, plans: PlanPriceLookup,
    ) -> None:
        self._client = client
        self._config = config
        self._plans = plans

    def create_customer(self, command: CreateStripeCustomerCommand) -> StripeCustomer:
        """Args: command: Conta de billing e chave de idempotencia.
        Returns: Cliente Stripe criado.
        Raises: RetryableBillingError, PermanentBillingError: Falha Stripe traduzida.
        """
        result = _call("customers.create", lambda: self._client.v1.customers.create(
            params={"metadata": {"billing_account_id": command.billing_account_id}},
            options={"idempotency_key": f"customer:{command.idempotency_key}"},
        ))
        return StripeCustomer(result.id)

    def create_checkout(self, command: CheckoutCommand) -> HostedSession:
        """Args: command: Cliente, plano e chave de idempotencia.
        Returns: Sessao hospedada de checkout.
        Raises: StripeMappingError: Preco do plano nao mapeado; erros Stripe traduzidos.
        """
        price_id = self._checkout_price(command.plan_version)
        self._guard_checkout(command.stripe_customer_id, command.idempotency_key)
        params = {
            "mode": "subscription",
            "customer": command.stripe_customer_id,
            "line_items": [{"price": price_id, "quantity": 1}],
            "metadata": {
                "billing_account_id": command.billing_account_id,
                "plan_version_id": command.plan_version.plan_version_id,
            },
            "success_url": self._config.success_url,
            "cancel_url": self._config.cancel_url,
            "client_reference_id": command.idempotency_key,
        }
        options = {"idempotency_key": f"checkout:{command.idempotency_key}"}
        session = _call(
            "checkout.sessions.create",
            lambda: self._client.v1.checkout.sessions.create(params=params, options=options),
        )
        return HostedSession(session.id, session.url)

    def create_portal(self, command: PortalCommand) -> HostedSession:
        """Args: command: Cliente e chave de idempotencia.
        Returns: Sessao hospedada do portal de cobranca.
        Raises: RetryableBillingError, PermanentBillingError: Falha Stripe traduzida.
        """
        params = {
            "customer": command.stripe_customer_id,
            "return_url": self._config.portal_return_url,
        }
        session = _call(
            "billing_portal.sessions.create",
            lambda: self._client.v1.billing_portal.sessions.create(
                params=params, options={"idempotency_key": f"portal:{command.idempotency_key}"},
            ),
        )
        return HostedSession(session.id, session.url)

    def get_current_state(self, request: StripeStateRequest) -> StripeBillingState:
        """Args: request: Cliente e, opcionalmente, assinatura.
        Returns: Estado atual da assinatura no Stripe.
        Raises: StripeMappingError: Dados Stripe ambiguos ou nao mapeados.
        """
        sub = self._subscription(request)
        if _object_id(sub.customer) != request.stripe_customer_id:
            raise _mapping("stripe_customer_mismatch", subscription_id=sub.id)
        item = self._single_item(sub)
        price_id = self._plan_price(item)
        status = _status(sub.status)
        invoice_id = self._latest_invoice(sub)
        features = self._features(request.stripe_customer_id)
        return StripeBillingState(
            request.stripe_customer_id, sub.id, status, sub.cancel_at_period_end is True,
            price_id, frozenset(features), _epoch(item.current_period_start),
            _epoch(item.current_period_end), invoice_id,
        )

    def list_events(self, request: StripeEventListRequest) -> StripeEventPage:
        """Args: request: Janela created_gte, cursor starting_after e limite.
        Returns: Pagina de eventos do mais novo ao mais antigo.
        Raises: RetryableBillingError, PermanentBillingError: Falha Stripe traduzida.
        """
        params: dict[str, Any] = {
            "created": {"gte": int(request.created_gte.timestamp())},
            "limit": request.limit,
        }
        if request.starting_after is not None:
            params["starting_after"] = request.starting_after
        page = _call("events.list", lambda: self._client.v1.events.list(params=params))
        events = tuple(_to_stripe_event(event) for event in page.data)
        return StripeEventPage(events, page.has_more is True)

    def _checkout_price(self, plan: PlanVersion) -> str:
        if len(plan.stripe_price_ids) != 1:
            raise _mapping("stripe_price_unmapped", plan_version_id=plan.plan_version_id)
        price_id = plan.stripe_price_ids[0]
        known = self._plans.get_plan_by_price(price_id)
        if (
            known is None
            or known.plan_version_id != plan.plan_version_id
            or price_id not in known.stripe_price_ids
        ):
            raise _mapping("stripe_price_unmapped", price_id=price_id)
        return price_id

    def _guard_checkout(self, customer_id: str, request_key: str) -> None:
        subscriptions = _call(
            "subscriptions.list",
            lambda: self._client.v1.subscriptions.list(
                params={"customer": customer_id, "status": "all", "limit": _GUARD_PAGE},
            ),
        )
        if subscriptions.has_more is True:
            raise StripeMappingError("stripe_subscriptions_unbounded")
        if any(sub.status not in _ENDED_STATUSES for sub in subscriptions.data):
            raise PermanentBillingError("stripe_subscription_exists")
        self._expire_open_sessions(customer_id, request_key)

    def _expire_open_sessions(self, customer_id: str, request_key: str) -> None:
        sessions = self._client.v1.checkout.sessions
        params = {"customer": customer_id, "status": "open", "limit": _GUARD_PAGE}
        found = _call("checkout.sessions.list", lambda: sessions.list(params=params))
        if found.has_more is True:
            raise StripeMappingError("stripe_checkout_sessions_unbounded")
        for session in found.data:
            if getattr(session, "client_reference_id", None) == request_key:
                continue
            _call("checkout.sessions.expire", partial(sessions.expire, session.id))

    def _subscription(self, request: StripeStateRequest) -> Any:
        subscriptions = self._client.v1.subscriptions
        if request.stripe_subscription_id is not None:
            return _call(
                "subscriptions.retrieve",
                lambda: subscriptions.retrieve(request.stripe_subscription_id),
            )
        params = {"customer": request.stripe_customer_id, "limit": 2}
        found = _call("subscriptions.list", lambda: subscriptions.list(params=params)).data
        if len(found) != 1:
            raise _mapping(
                "stripe_subscription_ambiguous",
                customer_id=request.stripe_customer_id, subscription_count=len(found),
            )
        return found[0]

    def _single_item(self, sub: Any) -> Any:
        items = sub.items.data
        if len(items) != 1:
            raise _mapping("stripe_subscription_items_unexpected", item_count=len(items))
        return items[0]

    def _plan_price(self, item: Any) -> str:
        price_id = _object_id(item.price)
        plan = None if price_id is None else self._plans.get_plan_by_price(price_id)
        if plan is None or price_id not in plan.stripe_price_ids:
            raise _mapping("stripe_price_unmapped", price_id=price_id)
        return price_id

    def _latest_invoice(self, sub: Any) -> str | None:
        invoice_id = _object_id(sub.latest_invoice)
        if invoice_id is None:
            return None
        invoice = _call("invoices.retrieve", lambda: self._client.v1.invoices.retrieve(invoice_id))
        if _invoice_subscription_id(invoice) != sub.id:
            raise _mapping("stripe_invoice_subscription_mismatch", invoice_id=invoice_id)
        return invoice_id

    def _features(self, customer_id: str) -> list[str]:
        listing = self._client.v1.entitlements.active_entitlements
        params: dict[str, Any] = {"customer": customer_id, "limit": _ENTITLEMENT_PAGE}
        features: list[str] = []
        while True:
            page = _call(
                "entitlements.active_entitlements.list",
                lambda: listing.list(params=dict(params)),
            )
            features.extend(entry.lookup_key for entry in page.data)
            if page.has_more is not True:
                return features
            if not page.data:
                raise StripeMappingError("stripe_entitlements_not_progressing")
            params["starting_after"] = page.data[-1].id


def _status(value: str) -> SubscriptionStatus:
    try:
        status = SubscriptionStatus(value)
    except ValueError:
        status = SubscriptionStatus.ADMIN_REVOKED
    if status is SubscriptionStatus.ADMIN_REVOKED:
        raise _mapping("stripe_status_unmapped", status=value)
    return status
