"""Late replay recognition for billing catalog writes after idempotency expiry."""

from collections.abc import Callable
from typing import Any

from cnes_domain.billing.commands import CreateBillingAccountCommand
from cnes_domain.billing.models import (
    BillingAccount,
    BillingAccountTenantLink,
    ReadConsistency,
)
from cnes_infra.billing.dynamodb_items import (
    decode_tenant_account,
    get_item,
    idempotency_digest,
)
from cnes_infra.billing.keys import tenant_account_key


class DynamoLateReplayMixin:
    _client: Any
    _table: str

    get_account: Callable[[str], BillingAccount | None]
    get_tenant_link: Callable[[str, str, ReadConsistency], BillingAccountTenantLink | None]

    def _stored_pair(
        self, billing_account_id: str, tenant_id: str
    ) -> tuple[BillingAccount, BillingAccountTenantLink] | None:
        item = get_item(self._client, self._table, tenant_account_key(tenant_id), True)
        if item is None or decode_tenant_account(item, tenant_id) != billing_account_id:
            return None
        account = self.get_account(billing_account_id)
        link = self.get_tenant_link(billing_account_id, tenant_id, ReadConsistency.STRONG)
        if account is None or link is None:
            return None
        return account, link

    def _created_account(self, command: CreateBillingAccountCommand) -> BillingAccount | None:
        link = command.initial_tenant_link
        if link is None:
            account = self.get_account(command.account.billing_account_id)
            pair = None if account is None else (account, None)
        else:
            pair = self._stored_pair(link.billing_account_id, link.tenant_id)
        if pair is None:
            return None
        stored = CreateBillingAccountCommand(*pair, command.idempotency_key)
        return pair[0] if idempotency_digest(stored) == idempotency_digest(command) else None
