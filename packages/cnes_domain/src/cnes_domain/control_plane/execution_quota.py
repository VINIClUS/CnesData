"""Quota mensal de tentativas de StartExecution do ambiente: comando, resultado e decisão pura."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Final

from cnes_domain.control_plane.environment_gate import require_environment

if TYPE_CHECKING:
    from collections.abc import Set

MONTHLY_ATTEMPT_LIMIT: Final = 200


class AttemptStatus(StrEnum):
    COUNTED = "counted"
    ALREADY_COUNTED = "already_counted"
    EXHAUSTED = "exhausted"


def _require_utc(value: object) -> None:
    if not isinstance(value, datetime) or value.utcoffset() != timedelta(0):
        raise ValueError("reason=datetime_not_utc field=now")


def _require_text(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"reason=blank_value field={name}")


def _require_positive(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"reason=out_of_range field={name}")


def execution_period(now: datetime) -> str:
    """Mês UTC do instante, no formato `YYYY-MM` da chave do contador.

    Raises: ValueError: Instante sem UTC.
    """
    _require_utc(now)
    return f"{now.year:04d}-{now.month:02d}"


@dataclass(frozen=True, slots=True)
class ConsumeExecutionAttempt:
    """Tentativa de start do dispatch no contador do mês UTC; o dispatch_id deduplica o replay."""

    environment: str
    period: str
    dispatch_id: str
    now: datetime
    limit: int = MONTHLY_ATTEMPT_LIMIT

    def __post_init__(self) -> None:
        require_environment(self.environment)
        if self.period != execution_period(self.now):
            raise ValueError("reason=period_mismatch field=period")
        _require_text(self.dispatch_id, "dispatch_id")
        _require_positive(self.limit, "limit")


@dataclass(frozen=True, slots=True)
class AttemptOutcome:
    """Decisão da tentativa e o contador do período depois dela."""

    status: AttemptStatus
    attempts: int
    limit: int


def decide_attempt(
    attempts: int, counted: Set[str], command: ConsumeExecutionAttempt,
) -> AttemptOutcome:
    """Decide a tentativa sobre o contador do período; o dispatch já contado vence o limite.

    Args: attempts: Tentativas do período; counted: Dispatches já contados; command: Tentativa.
    Returns: ALREADY_COUNTED ou EXHAUSTED com o contador intacto; senão COUNTED com ele somado.
    """
    if command.dispatch_id in counted:
        return AttemptOutcome(AttemptStatus.ALREADY_COUNTED, attempts, command.limit)
    if attempts >= command.limit:
        return AttemptOutcome(AttemptStatus.EXHAUSTED, attempts, command.limit)
    return AttemptOutcome(AttemptStatus.COUNTED, attempts + 1, command.limit)
