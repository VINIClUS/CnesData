"""Rota de webhook Stripe: verifica a assinatura sobre o body raw e grava no inbox."""

import logging
from typing import Annotated, Protocol

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from starlette.concurrency import run_in_threadpool

from cnes_domain.billing.errors import PermanentBillingError, RetryableBillingError
from cnes_domain.billing.inbox import StripeEvent
from cnes_domain.billing.ports import WebhookInboxPort

logger = logging.getLogger(__name__)

_NOT_CONFIGURED = "billing_not_configured"
_SIGNATURE_INVALID = "stripe_signature_invalid"
_RETRY_AFTER_SECONDS = "5"

router = APIRouter(prefix="/api/v1/billing", tags=["billing"])


class WebhookVerifier(Protocol):
    def verify(self, payload: bytes, signature: str) -> StripeEvent: ...  # pragma: no cover


def get_stripe_webhook_verifier() -> WebhookVerifier:
    """Falha fechado até a composição fornecer o verificador de assinatura."""
    raise HTTPException(status_code=503, detail=_NOT_CONFIGURED)


def get_webhook_inbox() -> WebhookInboxPort:
    """Falha fechado até a composição fornecer o inbox de webhooks."""
    raise HTTPException(status_code=503, detail=_NOT_CONFIGURED)


async def _verified_event(
    verifier: WebhookVerifier, payload: bytes, signature: str | None,
) -> StripeEvent:
    if signature is None or not signature.strip():
        raise HTTPException(status_code=400, detail=_SIGNATURE_INVALID)
    try:
        return await run_in_threadpool(verifier.verify, payload, signature)
    except PermanentBillingError as error:
        raise HTTPException(status_code=400, detail=error.code) from None


@router.post("/webhooks/stripe", status_code=200)
async def receive_stripe_webhook(
    request: Request,
    verifier: Annotated[WebhookVerifier, Depends(get_stripe_webhook_verifier)],
    inbox: Annotated[WebhookInboxPort, Depends(get_webhook_inbox)],
    stripe_signature: Annotated[str | None, Header(alias="Stripe-Signature")] = None,
) -> dict[str, bool]:
    """Verifica a assinatura do body raw e aceita o evento no inbox idempotente."""
    payload = await request.body()
    event = await _verified_event(verifier, payload, stripe_signature)
    try:
        result = await run_in_threadpool(inbox.accept, event)
    except RetryableBillingError:
        raise HTTPException(
            status_code=503,
            detail="billing_dependency_unavailable",
            headers={"Retry-After": _RETRY_AFTER_SECONDS},
        ) from None
    logger.info(
        "stripe_webhook_received event_id=%s event_type=%s disposition=%s",
        event.event_id, event.event_type, result.disposition.value,
    )
    return {"received": True}
