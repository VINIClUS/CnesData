"""Quota reservation capacity mixin."""

from typing import Any

from cnes_domain.billing.ports import ClockPort


class DynamoQuotaCapacityMixin:
    _client: Any
    _table: str
    _clock: ClockPort
