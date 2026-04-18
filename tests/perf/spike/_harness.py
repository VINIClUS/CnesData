"""Spike: baseline → burst 10x → recovery; mede p99 por fase."""
from __future__ import annotations

import statistics
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(slots=True)
class PhaseMetrics:
    rps_obs: float
    p50_ms: float
    p99_ms: float
    error_rate: float


@dataclass(slots=True)
class SpikeReport:
    pre: PhaseMetrics
    burst: PhaseMetrics
    recovery: PhaseMetrics


def _fase(fn: Callable[[], None], rps: int, duracao_s: float) -> PhaseMetrics:
    intervalo = 1.0 / rps
    latencias: list[float] = []
    erros = 0
    total = 0
    inicio = time.monotonic()
    proximo = inicio
    while time.monotonic() - inicio < duracao_s:
        if time.monotonic() >= proximo:
            t0 = time.monotonic_ns()
            try:
                fn()
            except Exception:
                erros += 1
            latencias.append((time.monotonic_ns() - t0) / 1e6)
            total += 1
            proximo += intervalo
    decorrido = time.monotonic() - inicio
    p50 = statistics.median(latencias) if latencias else 0.0
    p99 = (
        statistics.quantiles(latencias, n=100)[98]
        if len(latencias) >= 100 else max(latencias or [0.0])
    )
    return PhaseMetrics(
        rps_obs=total / decorrido if decorrido > 0 else 0.0,
        p50_ms=p50,
        p99_ms=p99,
        error_rate=erros / total if total > 0 else 0.0,
    )


def run_spike(
    fn: Callable[[], None],
    *,
    baseline_rps: int,
    pre_s: float = 600.0,
    burst_s: float = 60.0,
    burst_rps_multiplicador: int = 10,
    recovery_s: float = 300.0,
) -> SpikeReport:
    pre = _fase(fn, baseline_rps, pre_s)
    burst = _fase(fn, baseline_rps * burst_rps_multiplicador, burst_s)
    recovery = _fase(fn, baseline_rps, recovery_s)
    return SpikeReport(pre=pre, burst=burst, recovery=recovery)
