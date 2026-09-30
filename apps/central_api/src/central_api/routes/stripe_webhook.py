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
_INVALID_CONTENT_LENGTH = "invalid_content_length"
_PAYLOAD_TOO_LARGE = "stripe_webhook_payload_too_large"
_RETRY_AFTER_SECONDS = "5"

STRIPE_WEBHOOK_MAX_BODY_BYTES = 1_048_576

router = APIRouter(prefix="/api/v1/billing", tags=["billing"])


class WebhookVerifier(Protocol):
    def verify(self, payload: bytes, signature: str) -> StripeEvent: ...  # pragma: no cover


def get_stripe_webhook_verifier() -> WebhookVerifier:
    """Falha fechado até a composição fornecer o verificador de assinatura."""
    raise HTTPException(status_code=503, detail=_NOT_CONFIGURED)


def get_webhook_inbox() -> WebhookInboxPort:
    """Falha fechado até a composição fornecer o inbox de webhooks."""
    raise HTTPException(status_code=503, detail=_NOT_CONFIGURED)


def _require_signature(signature: str | None) -> str:
    if signature is None or not signature.strip():
        raise HTTPException(status_code=400, detail=_SIGNATURE_INVALID)
    return signature


def _check_declared_length(header: str | None) -> None:
    if header is None:
        return
    try:
        declared = int(header)
    except ValueError:
        raise HTTPException(status_code=400, detail=_INVALID_CONTENT_LENGTH) from None
    if declared < 0:
        raise HTTPException(status_code=400, detail=_INVALID_CONTENT_LENGTH)
    if declared > STRIPE_WEBHOOK_MAX_BODY_BYTES:
        raise HTTPException(status_code=413, detail=_PAYLOAD_TOO_LARGE)


async def _bounded_body(request: Request) -> bytes:
    _check_declared_length(request.headers.get("content-length"))
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > STRIPE_WEBHOOK_MAX_BODY_BYTES:
            raise HTTPException(status_code=413, detail=_PAYLOAD_TOO_LARGE)
    return bytes(body)


async def _verified_event(
    verifier: WebhookVerifier, payload: bytes, signature: str,
) -> StripeEvent:
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
    signature = _require_signature(stripe_signature)
    payload = await _bounded_body(request)
    event = await _verified_event(verifier, payload, signature)
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
