"""Contrato do object store usado pelo profile local no Windows."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from cnes_infra.object_store.windows_filesystem import WindowsFilesystemObjectStore
from packages.cnes_infra.tests.contracts import object_store_contract as contract
from packages.cnes_infra.tests.contracts.clock import MutableClock


@pytest.mark.parametrize("case", contract.object_store_cases(), ids=lambda case: case.name)
def test_contrato_portatil(case: contract.ObjectStoreCase, tmp_path: Path) -> None:
    adapter = WindowsFilesystemObjectStore(tmp_path)
    case.run(adapter, MutableClock(datetime(2026, 7, 15, tzinfo=UTC)))
