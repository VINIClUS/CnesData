from datetime import UTC, datetime

import pytest

from cnes_domain.control_plane.commands import CancelJob, CompleteJob
from cnes_domain.control_plane.entities import AccessRequest, Job, RawManifestRecord, Tenant
from cnes_domain.control_plane.enums import AccessRequestState, JobState
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.contracts import control_plane_contract
from packages.cnes_infra.tests.contracts.clock import (
    MutableClock,
    _agent,
    _claim_job,
    _event,
    _job,
    _raw_record,
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
def test_sqlite_control_plane_cumpre_contrato(
    case, sqlite_control_plane, clock, monkeypatch
) -> None:
    if case.name == "raw_chains":
        monkeypatch.setattr(
            control_plane_contract, "_store_record", _store_record_with_matching_mode
        )
    case.run(sqlite_control_plane, clock)


def _store_record_with_matching_mode(adapter, record, clock) -> None:
    job_id = f"job-{record.agent_id}-{record.snapshot_id}"
    job = _job(job_id, record.agent_id, record.tenant_id).model_copy(
        update={
            "source_type": record.source_type,
            "file_subtype": record.file_subtype,
            "competencia": record.competencia,
            "requested_snapshot_mode": record.snapshot_mode,
        }
    )
    adapter.put_agent(_agent(record.agent_id, tenant_id=record.tenant_id))
    adapter.create_job(job, _event(f"created-{job_id}", tenant_id=record.tenant_id))
    claimed = adapter.claim_job(_claim_job(job_id, "raw-worker", clock, record.tenant_id))
    assert claimed is not None
    command = CompleteJob(
        tenant_id=record.tenant_id,
        job_id=job_id,
        owner="raw-worker",
        fencing_token=claimed.fencing_token,
        manifest=record,
    )
    adapter.complete_job(
        command, _event(f"completed-{job_id}", tenant_id=record.tenant_id)
    )


def test_aceita_argumentos_nomeados_e_limites_padrao(sqlite_control_plane) -> None:
    identity = {
        "tenant_id": "354130",
        "source_type": "CNES",
        "file_subtype": "ST",
        "competencia": "2026-07",
    }

    assert sqlite_control_plane.latest_succeeded_job(
        agent_id="agent-a", **identity
    ) is None
    assert sqlite_control_plane.list_raw_manifest_chain(**identity) == ()
    assert sqlite_control_plane.list_waiting_runs_for_dependency(**identity) == ()
    positional = tuple(identity.values())
    assert sqlite_control_plane.list_raw_manifest_chain(*positional) == ()
    assert sqlite_control_plane.list_waiting_runs_for_dependency(*positional) == ()


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
        pytest.param(JobState.SUCCEEDED),
        pytest.param(JobState.CANCELED),
    ],
)
def test_rejeita_cancelamento_fora_de_leased_sem_mutacao(
    sqlite_control_plane, state
) -> None:
    updates = {"state": state}
    if state is JobState.SUCCEEDED:
        updates |= {
            "result_manifest_id": "manifest-result",
            "result_manifest_key": "raw/354130/CNES/2026-07/result/manifest.json",
        }
    job = Job.model_validate(_job("job-a").model_dump() | updates)
    created = _event("job-created")
    sqlite_control_plane.create_job(job, created)
    command = CancelJob(tenant_id="354130", job_id="job-a", requested_by="user-a")

    with pytest.raises(Conflict, match="job_not_leased"):
        sqlite_control_plane.cancel_job(command, _event("cancel-rejected"))

    assert sqlite_control_plane.get_job("354130", "job-a") == job
    assert sqlite_control_plane.pending_outbox(10) == (created,)


def _manifesto_com_identidade(base: RawManifestRecord, field: str) -> RawManifestRecord:
    updates = {
        "agent_id": {"agent_id": "agent-b"},
        "source_type": {
            "source_type": "SIHD",
            "manifest_key": "raw/354130/SIHD/2026-07/result/manifest.json",
        },
        "file_subtype": {"file_subtype": "PF"},
        "competencia": {
            "competencia": "2026-06",
            "manifest_key": "raw/354130/CNES/2026-06/result/manifest.json",
        },
        "snapshot_mode": {
            "snapshot_mode": "DELTA",
            "sequence": 2,
            "base_snapshot_id": "base-agent-a",
            "previous_manifest_sha256": "a" * 64,
        },
    }
    return RawManifestRecord.model_validate(base.model_dump() | updates[field])


@pytest.mark.parametrize(
    "field",
    [
        pytest.param("agent_id"),
        pytest.param("source_type"),
        pytest.param("file_subtype"),
        pytest.param("competencia"),
        pytest.param("snapshot_mode"),
    ],
)
def test_rejeita_manifesto_com_identidade_divergente_sem_mutacao(
    sqlite_control_plane, clock, field
) -> None:
    sqlite_control_plane.put_agent(_agent("agent-a"))
    original = _job("job-a")
    created = _event("job-created")
    sqlite_control_plane.create_job(original, created)
    claimed = sqlite_control_plane.claim_job(_claim_job("job-a", "worker-a", clock))
    assert claimed is not None
    manifest = _manifesto_com_identidade(_raw_record("result", "agent-a", 1, clock.now()), field)
    command = CompleteJob(
        tenant_id="354130",
        job_id="job-a",
        owner="worker-a",
        fencing_token=claimed.fencing_token,
        manifest=manifest,
    )

    with pytest.raises(Conflict, match="manifest_identity_mismatch"):
        sqlite_control_plane.complete_job(command, _event("job-completed"))

    assert sqlite_control_plane.get_job("354130", "job-a") == claimed
    assert sqlite_control_plane.list_raw_manifest_chain(
        "354130", "CNES", "ST", "2026-07"
    ) == ()
    assert sqlite_control_plane.pending_outbox(10) == (created,)
