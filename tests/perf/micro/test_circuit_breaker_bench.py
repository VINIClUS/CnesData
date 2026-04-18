"""Micro-bench: overhead de call() e call_async()."""
import asyncio

import pytest

from cnes_domain.pipeline.circuit_breaker import CircuitBreaker

pytestmark = pytest.mark.perf_micro


def _noop_sync() -> int:
    return 1


async def _noop_async() -> int:
    return 1


def test_call_sync_overhead(benchmark) -> None:
    cb = CircuitBreaker()
    benchmark(cb.call, _noop_sync)


def test_call_async_overhead(benchmark) -> None:
    cb = CircuitBreaker()
    loop = asyncio.new_event_loop()
    try:
        benchmark(lambda: loop.run_until_complete(cb.call_async(_noop_async)))
    finally:
        loop.close()
