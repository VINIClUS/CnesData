"""Shared hypothesis settings + executors for property-based race tests."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest
from hypothesis import HealthCheck, settings

settings.register_profile(
    "race",
    deadline=None,
    max_examples=50,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
settings.load_profile("race")


@pytest.fixture
def executor():
    with ThreadPoolExecutor(max_workers=8) as ex:
        yield ex
