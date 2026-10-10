"""Leitura forte dos contadores de capacidade da conta para o observador de shadow."""

from typing import Any

from cnes_domain.billing.models import CapacityKind
from cnes_infra.billing.dynamodb_items import get_item
from cnes_infra.billing.dynamodb_quota_items import CAPACITY_COUNTERS, usage_counter
from cnes_infra.billing.keys import capacity_usage_key


class DynamoCapacityCounters:
    """Lê `BILLING#<conta>/CAPACITY`; contador ausente significa capacidade não semeada."""

    def __init__(self, client: Any, table_name: str) -> None:
        self._client = client
        self._table_name = table_name

    def get_capacity_count(self, billing_account_id: str, kind: CapacityKind) -> int | None:
        """Args: billing_account_id: Conta; kind: Agente ou tenant.
        Returns: Contador lido com consistência forte; None quando ausente.
        Raises: BillingDependencyError, PermanentBillingError.
        """
        key = capacity_usage_key(billing_account_id)
        item = get_item(self._client, self._table_name, key, True)
        attribute = CAPACITY_COUNTERS[kind]
        if item is None or attribute not in item:
            return None
        return usage_counter(item, attribute)
