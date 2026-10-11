"""Gates de billing da API: resolução tenant → conta e bundle gate/capacidade."""

from dataclasses import dataclass

from cnes_domain.billing.execution_policy import local_billing_account_id
from cnes_domain.billing.gate import EntitlementGate
from cnes_domain.billing.ports import BillingAuditPort, QuotaReservationPort
from cnes_domain.billing.shadow import (
    NULL_SHADOW_OBSERVER,
    ShadowObserver,
    TenantAccountReader,
    linked_billing_account,
)
from cnes_domain.profiles import BillingMode


class BillingAccountMissing(Exception):
    code = "billing_account_missing"

    def __init__(self) -> None:
        super().__init__(f"code={self.code}")


@dataclass(frozen=True, slots=True)
class TenantAccountResolver:
    mode: BillingMode
    catalog: TenantAccountReader | None = None

    def resolve(self, tenant_id: str) -> str:
        """Args: tenant_id: Tenant autenticado.
        Returns: Conta de billing do tenant; local quando a execução não é medida.
        Raises: BillingAccountMissing: Link reverso ausente ou divergente em Stripe.
        """
        if self.mode is BillingMode.DISABLED:
            return local_billing_account_id(tenant_id)
        if self.catalog is None:
            raise BillingAccountMissing
        account = linked_billing_account(self.catalog, tenant_id)
        if account is None:
            raise BillingAccountMissing
        return account


@dataclass(frozen=True, slots=True)
class ApiBillingGates:
    mode: BillingMode
    gate: EntitlementGate
    capacity: QuotaReservationPort
    accounts: TenantAccountResolver
    audit: BillingAuditPort | None = None
    observer: ShadowObserver = NULL_SHADOW_OBSERVER

    @property
    def enforced(self) -> bool:
        return self.mode is BillingMode.STRIPE
