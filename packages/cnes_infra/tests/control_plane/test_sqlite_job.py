from datetime import UTC, datetime

import pytest

from cnes_domain.control_plane.enums import JobState
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.contracts.clock import MutableClock, _event, _job


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(datetime(2026, 7, 15, 12, tzinfo=UTC))


@pytest.fixture
def adapter(tmp_path, clock) -> SQLiteControlPlane:
    control_plane = SQLiteControlPlane(tmp_path / "control-plane.sqlite3", clock.now)
    control_plane.initialize()
    return control_plane


@pytest.mark.parametrize(
    "update",
    [
        {"state": JobState.LEASED, "lease_owner": "worker-a", "lease_until": "clock"},
        {"attempt": 1},
        {"fencing_token": 1},
        {"result_manifest_id": "manifest-a", "result_manifest_key": "raw/manifest-a.json"},
        {"error_code": "failed"},
    ],
)
def test_rejeita_job_criado_fora_do_estado_inicial(adapter, clock, update) -> None:
    values = {key: clock.now() if value == "clock" else value for key, value in update.items()}
    job = _job("job-a").model_copy(update=values)
    with pytest.raises(Conflict, match="job_initial_state_invalid"):
        adapter.create_job(job, _event("job-created"))
    assert adapter.get_job("354130", "job-a") is None
    assert adapter.pending_outbox(10) == ()
