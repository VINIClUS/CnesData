"""Gates de billing da API: resolução tenant → conta e bundle gate/capacidade."""

from dataclasses import dataclass
from typing import Protocol

from cnes_domain.billing.execution_policy import local_billing_account_id
from cnes_domain.billing.gate import EntitlementGate
from cnes_domain.billing.models import BillingAccountTenantLink, ReadConsistency
from cnes_domain.billing.ports import QuotaReservationPort
from cnes_domain.profiles import BillingMode


class BillingAccountMissing(Exception):
    code = "billing_account_missing"

    def __init__(self) -> None:
        super().__init__(f"code={self.code}")


class TenantAccountReader(Protocol):
    def get_tenant_account(
        self, tenant_id: str, consistency: ReadConsistency,
    ) -> BillingAccountTenantLink | None: ...


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
        link = self.catalog.get_tenant_account(tenant_id, ReadConsistency.STRONG)
        if link is None or link.tenant_id != tenant_id:
            raise BillingAccountMissing
        return link.billing_account_id


@dataclass(frozen=True, slots=True)
class ApiBillingGates:
    mode: BillingMode
    gate: EntitlementGate
    capacity: QuotaReservationPort
    accounts: TenantAccountResolver

    @property
    def enforced(self) -> bool:
        return self.mode is BillingMode.STRIPE
