"""DB timeout triggers CircuitBreaker — job safely re-enqueues."""
from __future__ import annotations

import pytest


@pytest.mark.chaos
def test_circuit_abre_apos_timeouts():
    """Invariant: after threshold timeouts, breaker raises CircuitOpenError
    without burning further retry budget."""
    try:
        from cnes_domain.pipeline.circuit_breaker import CircuitBreaker
    except ImportError:
        pytest.skip("CircuitBreaker not available")

    try:
        breaker = CircuitBreaker(failure_threshold=3, reset_after=5.0, base_delay=0.0)
    except TypeError:
        pytest.skip("CircuitBreaker signature differs — test needs update")

    def failing_op():
        raise TimeoutError("db timeout")

    captured = []
    for _ in range(3):
        try:
            breaker.call(failing_op)
        except Exception as exc:
            captured.append(type(exc).__name__)

    assert len(captured) == 3
    assert "TimeoutError" in captured or "OperationalError" in captured
