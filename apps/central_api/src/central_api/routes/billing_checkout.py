"""Reserva atômica de checkout pendente por conta de billing."""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timedelta

from cnes_domain.billing.commands import (
    PendingCheckout,
    ReleasePendingCheckoutCommand,
    ReservePendingCheckoutCommand,
)
from cnes_domain.billing.errors import BillingError
from cnes_domain.billing.ports import BillingCatalogPort

logger = logging.getLogger(__name__)

CHECKOUT_RESERVATION_TTL = timedelta(minutes=15)
MAX_CHECKOUT_RESERVATION_TTL = timedelta(hours=24)
RELEASABLE_CODES = frozenset({"stripe_price_unmapped", "stripe_subscription_exists"})


def get_checkout_reservation_ttl() -> timedelta:
    """Entrega a validade da reserva de checkout pendente."""
    return CHECKOUT_RESERVATION_TTL


def reservation_expiry(now: datetime, ttl: timedelta) -> datetime:
    """Calcula o fim da reserva; rejeita validade fora de (0, 24h]."""
    if not timedelta(0) < ttl <= MAX_CHECKOUT_RESERVATION_TTL:
        raise ValueError("reason=invalid_checkout_reservation_ttl")
    return now + ttl


def _release_quietly(catalog: BillingCatalogPort, reservation: PendingCheckout) -> None:
    command = ReleasePendingCheckoutCommand(
        reservation.billing_account_id, reservation.request_key, reservation.reserved_at,
    )
    try:
        catalog.release_pending_checkout(command)
    except BillingError as error:
        logger.warning("billing_checkout_release_failed code=%s", error.code)


@contextmanager
def pending_checkout(
    catalog: BillingCatalogPort, account_id: str, request_key: str, expires_at: datetime,
) -> Generator[PendingCheckout]:
    """Reserva o checkout da conta; libera só a reserva criada aqui, antes da sessão."""
    command = ReservePendingCheckoutCommand(account_id, request_key, expires_at)
    reservation = catalog.reserve_pending_checkout(command)
    try:
        yield reservation
    except BillingError as error:
        # Why: a replayed reservation may back a Stripe session from an earlier attempt.
        if not reservation.replayed and error.code in RELEASABLE_CODES:
            _release_quietly(catalog, reservation)
        raise
