"""Leitura forte dos contadores de capacidade da conta para o observador de shadow."""

from typing import Any

from cnes_control_plane.billing.dynamodb_items import get_item
from cnes_control_plane.billing.dynamodb_quota_items import CAPACITY_COUNTERS, usage_counter
from cnes_control_plane.billing.keys import capacity_usage_key
from cnes_control_plane.billing.models import CapacityKind


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
