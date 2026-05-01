"""Tests for landing contracts."""
from __future__ import annotations

from datetime import date
from uuid import uuid4

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
    try:
        claimed.tenant_id = "999999"  # type: ignore[misc]
    except Exception as exc:
        assert "frozen" in str(exc).lower() or "cannot assign" in str(exc).lower()
    else:  # pragma: no cover
        msg = "ClaimedExtraction must be frozen"
        raise AssertionError(msg)
