"""Harness simples: rampa RPS síncrona; mede p99 latência e error_rate."""
from __future__ import annotations

import statistics
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(slots=True)
class StressResult:
    rps_alvo: int
    rps_observado: float
    p50_ms: float
    p99_ms: float
    error_rate: float
    total: int
    erros: int


def rampa_sync(
    fn: Callable[[], None],
    rps_values: list[int],
    duracao_s: float = 30.0,
) -> list[StressResult]:
    out: list[StressResult] = []
    for rps in rps_values:
        intervalo = 1.0 / rps
        latencias_ms: list[float] = []
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
                latencias_ms.append((time.monotonic_ns() - t0) / 1e6)
                total += 1
                proximo += intervalo
        decorrido = time.monotonic() - inicio
        p50 = statistics.median(latencias_ms) if latencias_ms else 0.0
        p99 = (
            statistics.quantiles(latencias_ms, n=100)[98]
            if len(latencias_ms) >= 100 else max(latencias_ms or [0.0])
        )
        out.append(StressResult(
            rps_alvo=rps,
            rps_observado=total / decorrido if decorrido > 0 else 0.0,
            p50_ms=p50,
            p99_ms=p99,
            error_rate=erros / total if total > 0 else 0.0,
            total=total,
            erros=erros,
        ))
    return out


def break_point(results: list[StressResult]) -> int:
    for r in results:
        if r.p99_ms > 1000.0 or r.error_rate > 0.01:
            return r.rps_alvo
    return results[-1].rps_alvo
