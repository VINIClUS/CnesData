"""Limites de código das janelas de estágio governadas e do prazo fixo (decisão E2)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final, cast

if TYPE_CHECKING:
    from collections.abc import Mapping

STAGES: Final[dict[str, tuple[int, int]]] = {
    "phase-b": (360, 720), "api": (180, 360), "static": (180, 360), "unit": (120, 360),
    "phase-c-recovery": (240, 660), "phase-c-audit": (240, 660), "final": (60, 180),
}
FINAL_RESERVE: Final = 420
STAGE_SUM_CEILING: Final = 1830
CLOSE_DRAIN_VERSION: Final = 2
_FIELDS: Final = frozenset({"stages", "fixed_deadline_seconds"})


def governed_windows(value: Any) -> dict[str, Any]:
    """Returns: Janelas dentro dos limites do código.

    Raises: ValueError: Forma, janela fora de [min, max], soma acima do teto ou prazo incoerente.
    """
    if not isinstance(value, dict) or set(cast("dict[str, Any]", value)) != _FIELDS:
        raise ValueError("windows=fields")
    windows = cast("dict[str, Any]", value)
    stages, fixed = windows["stages"], windows["fixed_deadline_seconds"]
    if not isinstance(stages, dict) or set(cast("dict[str, Any]", stages)) != set(STAGES):
        raise ValueError("windows=stages")
    seconds = cast("dict[str, Any]", stages)
    for name, (low, high) in STAGES.items():
        if type(seconds[name]) is not int or not low <= seconds[name] <= high:
            raise ValueError(
                f"windows=stage stage={name} value={seconds[name]} min={low} max={high}")
    total = sum(cast("dict[str, int]", seconds).values())
    if total > STAGE_SUM_CEILING:
        raise ValueError(f"windows=sum sum={total} ceiling={STAGE_SUM_CEILING}")
    if type(fixed) is not int or fixed != total + FINAL_RESERVE:
        raise ValueError(f"windows=fixed value={fixed} expected={total + FINAL_RESERVE}")
    return windows


def later_seconds(windows: Mapping[str, Any], stage: str) -> int:
    """Returns: Soma das janelas dos estágios depois de `stage`, na ordem do código."""
    order = list(STAGES)
    return sum(windows["stages"][name] for name in order[order.index(stage) + 1:])


def close_windows(close: Mapping[str, Any], prefix: str) -> dict[str, Any]:
    """Returns: Janelas do close-drain versão 2. Raises: ValueError: Versão ou janelas fora."""
    version = close.get("version")
    if type(version) is not int or version != CLOSE_DRAIN_VERSION:
        raise ValueError(f"{prefix}=close_drain_version")
    return governed_windows(close.get("windows"))


__all__ = [
    "CLOSE_DRAIN_VERSION",
    "FINAL_RESERVE",
    "STAGES",
    "STAGE_SUM_CEILING",
    "close_windows",
    "governed_windows",
    "later_seconds",
]
