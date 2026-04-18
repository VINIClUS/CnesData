"""Soak: carga constante com sampling de RSS/FD; detecção de leak via regressão linear."""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import psutil

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(slots=True)
class LeakReport:
    rss_mb_inicio: float
    rss_mb_fim: float
    rss_slope_mb_por_min: float
    fd_delta: int
    amostras: list[tuple[float, float, int]] = field(default_factory=list)


def _slope_linear(xs: list[float], ys: list[float]) -> float:
    if len(xs) < 2:
        return 0.0
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True))
    den = sum((x - mx) ** 2 for x in xs)
    return num / den if den > 0 else 0.0


def _fd_count(proc: psutil.Process) -> int:
    return (
        proc.num_fds() if hasattr(proc, "num_fds")
        else len(proc.open_files())
    )


def run_soak(
    fn: Callable[[], None],
    duracao_s: float,
    rps_alvo: int,
    intervalo_amostra_s: float = 10.0,
) -> LeakReport:
    proc = psutil.Process(os.getpid())
    rss_inicio_mb = proc.memory_info().rss / (1024 * 1024)
    fd_inicio = _fd_count(proc)

    intervalo_call = 1.0 / rps_alvo
    inicio = time.monotonic()
    proximo_call = inicio
    proxima_amostra = inicio + intervalo_amostra_s
    amostras: list[tuple[float, float, int]] = [
        (0.0, rss_inicio_mb, fd_inicio),
    ]

    while time.monotonic() - inicio < duracao_s:
        if time.monotonic() >= proximo_call:
            try:
                fn()
            except Exception:
                pass
            proximo_call += intervalo_call
        if time.monotonic() >= proxima_amostra:
            t = time.monotonic() - inicio
            rss_mb = proc.memory_info().rss / (1024 * 1024)
            amostras.append((t, rss_mb, _fd_count(proc)))
            proxima_amostra += intervalo_amostra_s

    xs_min = [t / 60.0 for t, _, _ in amostras]
    ys_rss = [rss for _, rss, _ in amostras]
    slope = _slope_linear(xs_min, ys_rss)

    return LeakReport(
        rss_mb_inicio=rss_inicio_mb,
        rss_mb_fim=amostras[-1][1],
        rss_slope_mb_por_min=slope,
        fd_delta=amostras[-1][2] - fd_inicio,
        amostras=amostras,
    )
