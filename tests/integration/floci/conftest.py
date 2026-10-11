"""Fixtures da camada Floci: emulador fixado e pronto, nomes efêmeros e teardown garantido."""
from __future__ import annotations

import os
import time
from contextlib import ExitStack
from typing import TYPE_CHECKING

import pytest

from tests.integration.floci._floci import (
    FlociContext,
    FlociIsolationError,
    floci_endpoint,
    health_problem,
    http_client,
    isolated_session,
    unique_name,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

_READY_TIMEOUT_SECONDS = 20
_HINT = "docker compose --profile floci up -d --wait floci"


def _wait_until_ready(endpoint: str) -> None:
    deadline = time.monotonic() + _READY_TIMEOUT_SECONDS
    with http_client() as http:
        while (problem := health_problem(http, endpoint)) is not None:
            if problem.startswith("version=") or time.monotonic() >= deadline:
                pytest.fail(f"floci_unready endpoint={endpoint} problem={problem} hint={_HINT}")
            time.sleep(1)


@pytest.fixture(scope="session")
def floci() -> FlociContext:
    try:
        endpoint = floci_endpoint(os.environ)
    except FlociIsolationError as error:
        pytest.fail(str(error))
    _wait_until_ready(endpoint)
    return FlociContext(endpoint, isolated_session())


@pytest.fixture
def resource_name() -> str:
    return unique_name()


@pytest.fixture
def cleanup() -> Iterator[ExitStack]:
    with ExitStack() as stack:
        yield stack


@pytest.fixture(scope="module")
def module_cleanup() -> Iterator[ExitStack]:
    with ExitStack() as stack:
        yield stack
