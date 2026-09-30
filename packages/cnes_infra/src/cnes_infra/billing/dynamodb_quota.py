"""DynamoDB transactional quota and budget reservations."""

from typing import Any

from cnes_domain.billing.commands import ReserveAnalyticsCommand, ReserveRunCommand
from cnes_domain.billing.models import AnalyticsAuthorization, RunAuthorization
from cnes_domain.billing.ports import ClockPort
from cnes_infra.billing.dynamodb_quota_capacity import DynamoQuotaCapacityMixin
from cnes_infra.billing.dynamodb_quota_recovery import DynamoQuotaRecoveryMixin
from cnes_infra.billing.dynamodb_quota_settlement import DynamoQuotaSettlementMixin


class DynamoQuotaReservations(
    DynamoQuotaCapacityMixin, DynamoQuotaSettlementMixin, DynamoQuotaRecoveryMixin
):
    """Reservas de quota e budget em transações DynamoDB únicas."""

    def __init__(self, client: Any, table_name: str, clock: ClockPort) -> None:
        self._client = client
        self._table = table_name
        self._clock = clock

    def reserve_and_create_run(self, command: ReserveRunCommand) -> RunAuthorization:
        raise NotImplementedError

    def reserve_analytics(self, command: ReserveAnalyticsCommand) -> AnalyticsAuthorization:
        raise NotImplementedError
