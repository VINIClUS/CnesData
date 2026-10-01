"""Leitura da conta de billing de um tenant pelo índice reverso."""

from collections.abc import Callable
from typing import Any

from cnes_domain.billing.models import BillingAccountTenantLink, ReadConsistency
from cnes_infra.billing.dynamodb_items import decode_tenant_account, get_item
from cnes_infra.billing.keys import tenant_account_key


class DynamoTenantAccountMixin:
    _client: Any
    _table: str

    get_tenant_link: Callable[[str, str, ReadConsistency], BillingAccountTenantLink | None]

    def get_tenant_account(
        self, tenant_id: str, consistency: ReadConsistency
    ) -> BillingAccountTenantLink | None:
        """Resolve o link do tenant pelo índice reverso, validado contra o link direto.

        Args: Tenant e consistência de leitura.
        Returns: O link, ou None se ausente, pendurado ou divergente.
        Raises: PermanentBillingError, BillingDependencyError.
        """
        key = tenant_account_key(tenant_id)
        item = get_item(self._client, self._table, key, consistency is ReadConsistency.STRONG)
        if item is None:
            return None
        account_id = decode_tenant_account(item, tenant_id)
        link = self.get_tenant_link(account_id, tenant_id, consistency)
        if link is None or (link.billing_account_id, link.tenant_id) != (account_id, tenant_id):
            return None
        return link
