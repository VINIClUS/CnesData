"""Carga de conta e anexo idempotente do Customer Stripe da conta de billing."""

import logging

from fastapi import HTTPException

from cnes_domain.billing.commands import AttachStripeCustomerCommand, CreateStripeCustomerCommand
from cnes_domain.billing.errors import PermanentBillingError, RetryableBillingError
from cnes_domain.billing.models import BillingAccount
from cnes_domain.billing.ports import BillingCatalogPort, StripeGatewayPort

logger = logging.getLogger(__name__)

_ATTACH_CONFLICTS = frozenset({"stripe_customer_already_attached", "billing_account_stale"})


def load_account(catalog: BillingCatalogPort, billing_account_id: str) -> BillingAccount:
    """Lê a conta de billing.
    Raises: HTTPException: 404 billing_account_not_found; erros de leitura propagam.
    """
    account = catalog.get_account(billing_account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="billing_account_not_found")
    return account


def _attached_by_race(
    catalog: BillingCatalogPort, account_id: str, customer_id: str,
) -> BillingAccount:
    account = load_account(catalog, account_id)
    if account.stripe_customer_id is None:
        raise RetryableBillingError("billing_account_stale")
    if account.stripe_customer_id != customer_id:
        logger.warning(
            "billing_customer_orphaned billing_account_id=%s stripe_customer_id=%s",
            account_id, customer_id,
        )
    return account


def ensure_customer(
    catalog: BillingCatalogPort, gateway: StripeGatewayPort, account: BillingAccount,
) -> BillingAccount:
    """Cria e anexa o Customer Stripe da conta quando ausente, de forma idempotente.
    Returns: A conta com Customer anexado.
    Raises: RetryableBillingError, PermanentBillingError, HTTPException 404.
    """
    if account.stripe_customer_id is not None:
        return account
    id_ = account.billing_account_id
    customer = gateway.create_customer(CreateStripeCustomerCommand(id_, id_)).stripe_customer_id
    try:
        return catalog.attach_customer(
            AttachStripeCustomerCommand(id_, customer, account.updated_at),
        )
    except PermanentBillingError as error:
        if error.code not in _ATTACH_CONFLICTS:
            raise
    return _attached_by_race(catalog, id_, customer)
