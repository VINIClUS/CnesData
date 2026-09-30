"""Quota reservation settlement mixin."""

from typing import Any

from cnes_domain.billing.ports import ClockPort


class DynamoQuotaSettlementMixin:
    _client: Any
    _table: str
    _clock: ClockPort
