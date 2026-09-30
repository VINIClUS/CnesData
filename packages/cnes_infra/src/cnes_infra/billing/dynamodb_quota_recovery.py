"""Quota reservation recovery mixin."""

from typing import Any

from cnes_domain.billing.ports import ClockPort


class DynamoQuotaRecoveryMixin:
    _client: Any
    _table: str
    _clock: ClockPort
