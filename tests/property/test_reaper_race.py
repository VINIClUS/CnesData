"""Reaper must not un-claim jobs with active heartbeat."""
from __future__ import annotations

import pytest

pytestmark = [pytest.mark.race, pytest.mark.postgres]


@pytest.mark.skip(reason="race: reaper_race requires live postgres; issue TBD")
def test_reaper_respeita_heartbeat_ativo():
    """Invariant: a job whose heartbeat is renewed via
    cnes_infra.storage.job_queue.renew_heartbeat every 100ms should never
    be reaped by reap_expired_leases running concurrently.
    """
