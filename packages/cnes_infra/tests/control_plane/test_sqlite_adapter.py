from datetime import UTC, datetime

import pytest

from cnes_domain.control_plane.commands import CancelJob
from cnes_domain.control_plane.entities import AccessRequest, Tenant
from cnes_domain.control_plane.enums import AccessRequestState, JobState
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.contracts.clock import (
    MutableClock,
    _agent,
    _claim_job,
    _event,
    _job,
)
from packages.cnes_infra.tests.contracts.control_plane_contract import control_plane_cases


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(datetime(2026, 7, 15, 12, tzinfo=UTC))


@pytest.fixture
def sqlite_control_plane(tmp_path, clock: MutableClock) -> SQLiteControlPlane:
    adapter = SQLiteControlPlane(tmp_path / "control-plane.sqlite3", clock.now)
    adapter.initialize()
    return adapter


@pytest.mark.parametrize("case", control_plane_cases(), ids=lambda case: case.name)
def test_sqlite_control_plane_cumpre_contrato(case, sqlite_control_plane, clock) -> None:
    case.run(sqlite_control_plane, clock)


def test_persiste_tenant_solicitacao_de_acesso_e_entrega_outbox(
    sqlite_control_plane, clock
) -> None:
    tenant = Tenant(
        tenant_id="354130", municipality_name="Presidente Epitácio", created_at=clock.now()
    )
    pending = AccessRequest(
        tenant_id=tenant.tenant_id,
        request_id="request-a",
        user_id="user-a",
        state=AccessRequestState.PENDING,
        decided_by=None,
        decided_at=None,
    )
    created = _event("access-requested")
    sqlite_control_plane.put_tenant(tenant)
    sqlite_control_plane.put_access_request(pending, created)
    approved = pending.model_copy(
        update={
            "state": AccessRequestState.APPROVED,
            "decided_by": "admin-a",
            "decided_at": clock.now(),
        }
    )
    decided = _event("access-approved")

    assert sqlite_control_plane.decide_access_request(approved, decided) == approved
    assert sqlite_control_plane.get_tenant(tenant.tenant_id) == tenant
    assert sqlite_control_plane.get_access_request(tenant.tenant_id, pending.request_id) == approved
    assert sqlite_control_plane.pending_outbox(10) == (decided, created)

    sqlite_control_plane.mark_outbox_delivered(decided.event_id, clock.now())

    assert sqlite_control_plane.pending_outbox(10) == (created,)


def test_cancela_job_leased_e_torna_replay_idempotente(sqlite_control_plane, clock) -> None:
    sqlite_control_plane.put_agent(_agent("agent-a"))
    sqlite_control_plane.create_job(_job("job-a"), _event("job-created"))
    leased = sqlite_control_plane.claim_job(_claim_job("job-a", "worker-a", clock))
    command = CancelJob(tenant_id="354130", job_id="job-a", requested_by="user-a")
    event = _event("job-cancel-requested")

    canceled = sqlite_control_plane.cancel_job(command, event)

    assert leased is not None
    assert canceled.state is JobState.CANCEL_REQUESTED
    assert sqlite_control_plane.cancel_job(command, _event("replay-ignored")) == canceled
    assert sqlite_control_plane.pending_outbox(10) == (event, _event("job-created"))


@pytest.mark.parametrize(
    "state",
    [
        pytest.param(JobState.PENDING),
        pytest.param(JobState.FAILED_RETRYABLE),
        pytest.param(JobState.FAILED_FINAL),
        pytest.param(JobState.CANCELED),
    ],
)
def test_rejeita_cancelamento_fora_de_leased_sem_mutacao(
    sqlite_control_plane, state
) -> None:
    job = _job("job-a").model_copy(update={"state": state})
    created = _event("job-created")
    sqlite_control_plane.create_job(job, created)
    command = CancelJob(tenant_id="354130", job_id="job-a", requested_by="user-a")

    with pytest.raises(Conflict, match="job_not_leased"):
        sqlite_control_plane.cancel_job(command, _event("cancel-rejected"))

    assert sqlite_control_plane.get_job("354130", "job-a") == job
    assert sqlite_control_plane.pending_outbox(10) == (created,)
