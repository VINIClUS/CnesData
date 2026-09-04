from datetime import UTC, datetime

import pytest

from cnes_domain.control_plane.entities import AccessRequest
from cnes_domain.control_plane.enums import AccessRequestState
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.contracts.clock import MutableClock, _event


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(datetime(2026, 7, 15, 12, tzinfo=UTC))


@pytest.fixture
def sqlite_control_plane(tmp_path, clock: MutableClock) -> SQLiteControlPlane:
    adapter = SQLiteControlPlane(tmp_path / "control-plane.sqlite3", clock.now)
    adapter.initialize()
    return adapter


@pytest.mark.parametrize(
    ("invalid_kind", "error"),
    [("state", "access_request_decision_state"), ("identity", "access_request_identity_conflict")],
)
def test_replays_e_conflitos_de_acesso(sqlite_control_plane, clock, invalid_kind, error) -> None:
    pending = AccessRequest(tenant_id="354130", request_id="request-a", user_id="user-a",
        state=AccessRequestState.PENDING, decided_by=None, decided_at=None)
    created = _event("access-requested")
    ignored = _event("access-replay")
    approved = pending.model_copy(update={
        "state": AccessRequestState.APPROVED, "decided_by": "admin-a", "decided_at": clock.now(),
    })
    with pytest.raises(Conflict, match="access_request_creation_state"):
        sqlite_control_plane.put_access_request(approved, ignored)
    sqlite_control_plane.put_access_request(pending, created)
    sqlite_control_plane.put_access_request(pending, created)
    replay = created.model_copy(update={"tenant_id": "other", "payload": {"changed": True},
        "delivered_at": clock.now()})
    with pytest.raises(Conflict, match="access_request_creation_conflict"):
        sqlite_control_plane.put_access_request(pending, replay)
    divergent = pending.model_copy(update={"user_id": "user-b"})
    with pytest.raises(Conflict, match="access_request_conflict"):
        sqlite_control_plane.put_access_request(divergent, _event("access-conflict"))
    before = (sqlite_control_plane.get_access_request("354130", "request-a"), (created,))
    invalid = pending if invalid_kind == "state" else approved.model_copy(
        update={"user_id": "user-b"})
    with pytest.raises(Conflict, match=error):
        sqlite_control_plane.decide_access_request(invalid, ignored)
    assert before == (
        sqlite_control_plane.get_access_request("354130", "request-a"),
        sqlite_control_plane.pending_outbox(100),
    )
    decided = _event("access-approved")
    assert sqlite_control_plane.decide_access_request(approved, decided) == approved
    with sqlite_control_plane.write_transaction() as connection:
        connection.executescript(
            "ALTER TABLE access_requests DROP COLUMN creation_request_data;"
            "ALTER TABLE access_requests DROP COLUMN creation_event_data;"
        )
    reopened = SQLiteControlPlane(sqlite_control_plane._database_path, clock.now)
    reopened.initialize()
    assert reopened.decide_access_request(approved, decided) == approved
    with pytest.raises(Conflict, match="access_request_creation_conflict"):
        reopened.put_access_request(pending, created)
    with pytest.raises(Conflict, match="access_request_decision_conflict"):
        reopened.decide_access_request(approved, ignored)
    with pytest.raises(Conflict, match="access_request_state_conflict"):
        sqlite_control_plane.decide_access_request(
            approved.model_copy(update={"state": AccessRequestState.REJECTED}), ignored)
    with pytest.raises(Conflict, match="access_request_state_conflict"):
        sqlite_control_plane.decide_access_request(
            pending.model_copy(update={"request_id": "missing"}), ignored)
    assert sqlite_control_plane.pending_outbox(100) == (decided, created)
