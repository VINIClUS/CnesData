"""Mapeamento de erros de billing do domínio para respostas HTTP."""

import logging
from collections.abc import Generator
from contextlib import contextmanager

from fastapi import HTTPException

from cnes_domain.billing.errors import (
    BillingDependencyError,
    BillingError,
    BillingTenantConflict,
    IdempotencyConflict,
    PermanentBillingError,
    RetryableBillingError,
)

logger = logging.getLogger(__name__)

_RETRY_AFTER_SECONDS = "5"
_CONFLICT_CODES = {
    "stripe_subscription_exists": "subscription_exists",
    "checkout_in_progress": "checkout_in_progress",
}
_MAPPED = (RetryableBillingError, PermanentBillingError, IdempotencyConflict, BillingTenantConflict)


def _retryable_to_http(error: RetryableBillingError) -> HTTPException:
    if error.code == "stripe_price_unmapped":
        return HTTPException(409, "plan_price_unmapped")
    headers = {"Retry-After": _RETRY_AFTER_SECONDS}
    if error.code == "stripe_unavailable":
        return HTTPException(503, "stripe_unavailable", headers=headers)
    return HTTPException(503, "billing_dependency_unavailable", headers=headers)


def _to_http(error: BillingError) -> HTTPException | None:
    if isinstance(error, BillingDependencyError):
        return HTTPException(503, "billing_dependency_unavailable")
    if isinstance(error, RetryableBillingError):
        return _retryable_to_http(error)
    if isinstance(error, PermanentBillingError):
        if error.code in _CONFLICT_CODES:
            return HTTPException(409, _CONFLICT_CODES[error.code])
        stripe = error.code.startswith("stripe_")
        return HTTPException(502, "stripe_request_rejected") if stripe else None
    if isinstance(error, IdempotencyConflict):
        return HTTPException(409, "idempotency_conflict")
    return HTTPException(409, "billing_tenant_conflict")


@contextmanager
def mapped_errors() -> Generator[None]:
    """Traduz erros de billing do domínio em HTTPException."""
    try:
        yield
    except _MAPPED as error:
        logger.warning("billing_request_failed code=%s", error.code)
        mapped = _to_http(error)
        if mapped is None:
            raise
        raise mapped from error
