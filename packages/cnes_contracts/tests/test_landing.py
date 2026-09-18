"""Tests for landing contracts."""
from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import date
from uuid import uuid4

import pytest

from cnes_contracts.landing import ClaimedExtraction


def test_claimed_extraction_frozen_dataclass() -> None:
    job_id = uuid4()
    dep = uuid4()
    claimed = ClaimedExtraction(
        job_id=job_id,
        tenant_id="354130",
        source_type="BPA_MAG",
        competencia=date(2026, 1, 1),
        files=[{"minio_key": "x.parquet.gz"}],
        depends_on=[dep],
    )
    assert claimed.job_id == job_id
    assert claimed.tenant_id == "354130"
    assert claimed.depends_on == [dep]
    with pytest.raises(FrozenInstanceError):
        claimed.tenant_id = "999999"  # type: ignore[misc]
