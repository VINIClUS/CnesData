"""Concurrent claim of jobs must never result in double-lease."""
from __future__ import annotations

import pytest

pytestmark = [pytest.mark.race, pytest.mark.postgres]


@pytest.mark.skip(reason="race: job_queue_race requires live postgres + real repo; issue TBD")
def test_nunca_dois_workers_leam_mesmo_job():
    """Invariant: for a given job, at most one worker can claim a lease at
    any moment. Verified by spawning N threads calling
    cnes_infra.storage.job_queue.acquire_for_agent concurrently on the same
    job and asserting len(winners) <= 1.
    """
