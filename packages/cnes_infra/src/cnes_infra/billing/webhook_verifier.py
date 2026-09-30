"""Verificador de assinatura de webhooks Stripe sobre o body raw."""

import hashlib
from datetime import UTC, datetime
from typing import Any

from cnes_domain.billing.errors import PermanentBillingError
from cnes_domain.billing.inbox import StripeEvent

_SIGNATURE_INVALID = "stripe_signature_invalid"
_SCHEMA_INVALID = "stripe_event_schema_invalid"
_SIGNATURE_ERROR = "SignatureVerificationError"
_ENTITLEMENT_SUMMARY = "entitlements.active_entitlement_summary."


def _object_id(value: object) -> str | None:
    if isinstance(value, str):
        return value
    identifier = getattr(value, "id", None)
    return identifier if isinstance(identifier, str) else None


def _invoice_subscription(obj: Any) -> str | None:
    parent = getattr(obj, "parent", None)
    details = getattr(parent, "subscription_details", None)
    return _object_id(getattr(details, "subscription", None))


def _subscription_id(event_type: str, obj: Any) -> str | None:
    if event_type.startswith("invoice."):
        return _invoice_subscription(obj)
    if event_type.startswith("customer.subscription."):
        return _object_id(obj)
    if event_type.startswith(_ENTITLEMENT_SUMMARY):
        return None
    return _object_id(getattr(obj, "subscription", None))


def _customer_id(obj: Any) -> str | None:
    if getattr(obj, "object", None) == "customer":
        return _object_id(obj)
    return _object_id(getattr(obj, "customer", None))


def _is_signature_error(error: Exception) -> bool:
    return any(cls.__name__ == _SIGNATURE_ERROR for cls in type(error).__mro__)


def _to_stripe_event(event: Any, payload: bytes) -> StripeEvent:
    obj = event.data.object
    return StripeEvent(
        event_id=event.id,
        event_type=event.type,
        created_at=datetime.fromtimestamp(event.created, tz=UTC),
        stripe_customer_id=_customer_id(obj),
        stripe_subscription_id=_subscription_id(event.type, obj),
        payload_sha256=hashlib.sha256(payload).hexdigest(),
    )


class StripeWebhookVerifier:
    """Valida a assinatura do webhook Stripe e mapeia o evento mínimo."""

    def __init__(self, webhook_secret: str) -> None:
        if not isinstance(webhook_secret, str) or not webhook_secret.strip():
            raise ValueError("reason=blank_webhook_secret")
        self._secret = webhook_secret

    def verify(self, payload: bytes, signature: str) -> StripeEvent:
        """Args: payload: Body raw exato; signature: Cabeçalho Stripe-Signature.
        Returns: Evento Stripe mínimo mapeado.
        Raises: PermanentBillingError: Assinatura inválida ou schema inválido.
        """
        import stripe

        try:
            event = stripe.Webhook.construct_event(payload, signature, self._secret)
        except Exception as error:
            if isinstance(error, ValueError) or _is_signature_error(error):
                raise PermanentBillingError(_SIGNATURE_INVALID) from None
            raise
        try:
            return _to_stripe_event(event, payload)
        except (AttributeError, TypeError, ValueError, OverflowError, OSError):
            raise PermanentBillingError(_SCHEMA_INVALID) from None
