from datetime import UTC, datetime, timedelta

import pytest

from central_api.services.job_lifecycle import DEFAULT_MAX_ATTEMPTS, JobLifecycle
from cnes_domain.control_plane.commands import (
    CancelJob,
    ClaimJob,
    CompleteJob,
    FailJob,
    RenewJobLease,
)
from cnes_domain.control_plane.entities import Agent, Job, OutboxEvent, RawManifestRecord
from cnes_domain.control_plane.enums import AgentState, JobState
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost, NotFound
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode

NOW = datetime(2026, 1, 20, 9, tzinfo=UTC)
TENANT = "354130"
COMPETENCIA = "2026-01"


def agent(**updates: object) -> Agent:
    values = {
        "tenant_id": TENANT,
        "agent_id": "agent-1",
        "state": AgentState.ACTIVE,
        "version": "1.0.0",
        "certificate_fingerprint": "a" * 64,
        "last_seen_at": None,
        "created_at": NOW,
    }
    return Agent(**(values | updates))


def pending_job(**updates: object) -> Job:
    values = {
        "tenant_id": TENANT,
        "job_id": "job-1",
        "agent_id": "agent-1",
        "source_type": "CNES_LOCAL",
        "file_subtype": "CNES_VINCULO",
        "competencia": COMPETENCIA,
        "requested_snapshot_mode": "FULL",
        "state": JobState.PENDING,
        "attempt": 0,
        "fencing_token": 0,
        "lease_owner": None,
        "lease_until": None,
        "result_manifest_id": None,
        "result_manifest_key": None,
        "error_code": None,
        "created_at": NOW,
    }
    return Job(**(values | updates))


def leased_job(**updates: object) -> Job:
    values = {
        "state": JobState.LEASED,
        "fencing_token": 1,
        "lease_owner": "agent-1",
        "lease_until": NOW + timedelta(minutes=5),
    }
    return pending_job(**(values | updates))


def claim_for(job: Job, owner: Agent, **updates: object) -> ClaimJob:
    values = {
        "tenant_id": job.tenant_id,
        "job_id": job.job_id,
        "owner": owner.agent_id,
        "now": NOW,
        "lease_seconds": 300,
    }
    return ClaimJob(**(values | updates))


def renew_for(job: Job, **updates: object) -> RenewJobLease:
    values = {
        "tenant_id": job.tenant_id,
        "job_id": job.job_id,
        "owner": job.lease_owner,
        "fencing_token": job.fencing_token,
        "now": NOW,
        "lease_seconds": 300,
    }
    return RenewJobLease(**(values | updates))


def _manifest_for(job: Job, **updates: object) -> RawManifestRecord:
    values = {
        "tenant_id": job.tenant_id,
        "manifest_id": "manifest-1",
        "manifest_key": (
            f"raw/{job.tenant_id}/{job.source_type}/{job.competencia}/snap-1/manifest.json"
        ),
        "agent_id": job.agent_id,
        "source_type": job.source_type,
        "file_subtype": job.file_subtype,
        "competencia": job.competencia,
        "snapshot_mode": "FULL",
        "snapshot_id": "snap-1",
        "base_snapshot_id": None,
        "sequence": 1,
        "previous_manifest_sha256": None,
        "manifest_sha256": "a" * 64,
        "created_at": NOW,
    }
    return RawManifestRecord(**(values | updates))


def complete_for(job: Job, fencing_token: int | None = None, **updates: object) -> CompleteJob:
    values = {
        "tenant_id": job.tenant_id,
        "job_id": job.job_id,
        "owner": job.lease_owner,
        "fencing_token": job.fencing_token if fencing_token is None else fencing_token,
        "manifest": _manifest_for(job),
    }
    return CompleteJob(**(values | updates))


def fail_for(job: Job, **updates: object) -> FailJob:
    values = {
        "tenant_id": job.tenant_id,
        "job_id": job.job_id,
        "owner": job.lease_owner,
        "fencing_token": job.fencing_token,
        "error_code": "RAW_TIMEOUT",
        "retryable": True,
    }
    return FailJob(**(values | updates))


def cancel_for(job: Job, **updates: object) -> CancelJob:
    values = {
        "tenant_id": job.tenant_id,
        "job_id": job.job_id,
        "requested_by": "operator-1",
    }
    return CancelJob(**(values | updates))


class ControlPlane:
    def __init__(self, current_agent: Agent | None = None, jobs: tuple[Job, ...] = ()) -> None:
        self.agents = {} if current_agent is None else {
            (current_agent.tenant_id, current_agent.agent_id): current_agent
        }
        self.jobs = {(job.tenant_id, job.job_id): job for job in jobs}
        self.mutations: list[str] = []
        self.events: list[OutboxEvent] = []
        self.fail_commands: list[FailJob] = []
        self.claim_error: Exception | None = None
        self.renew_error: Exception | None = None
        self.complete_error: Exception | None = None
        self.fail_error: Exception | None = None
        self.cancel_error: Exception | None = None

    def get_agent(self, tenant_id: str, agent_id: str) -> Agent | None:
        return self.agents.get((tenant_id, agent_id))

    def get_job(self, tenant_id: str, job_id: str) -> Job | None:
        return self.jobs.get((tenant_id, job_id))

    def claim_job(self, command: ClaimJob) -> Job | None:
        self.mutations.append("claim_job")
        if self.claim_error is not None:
            raise self.claim_error
        current = self.jobs.get((command.tenant_id, command.job_id))
        if current is None or current.state is not JobState.PENDING:
            return None
        claimed = current.model_copy(update={
            "state": JobState.LEASED,
            "attempt": current.attempt + 1,
            "fencing_token": current.fencing_token + 1,
            "lease_owner": command.owner,
            "lease_until": command.now + timedelta(seconds=command.lease_seconds),
        })
        self.jobs[(command.tenant_id, command.job_id)] = claimed
        return claimed

    def renew_job_lease(self, command: RenewJobLease) -> Job:
        self.mutations.append("renew_job_lease")
        if self.renew_error is not None:
            raise self.renew_error
        current = self.jobs[(command.tenant_id, command.job_id)]
        renewed = current.model_copy(update={
            "lease_until": command.now + timedelta(seconds=command.lease_seconds),
        })
        self.jobs[(command.tenant_id, command.job_id)] = renewed
        return renewed

    def complete_job(self, command: CompleteJob, event: OutboxEvent) -> Job:
        self.mutations.append("complete_job")
        self.events.append(event)
        if self.complete_error is not None:
            raise self.complete_error
        current = self.jobs[(command.tenant_id, command.job_id)]
        completed = current.model_copy(update={
            "state": JobState.SUCCEEDED,
            "lease_owner": None,
            "lease_until": None,
            "result_manifest_id": command.manifest.manifest_id,
            "result_manifest_key": command.manifest.manifest_key.replace(
                "data.parquet", "manifest.json"
            ),
        })
        self.jobs[(command.tenant_id, command.job_id)] = completed
        return completed

    def fail_job(self, command: FailJob, event: OutboxEvent) -> Job:
        self.mutations.append("fail_job")
        self.events.append(event)
        self.fail_commands.append(command)
        if self.fail_error is not None:
            raise self.fail_error
        current = self.jobs[(command.tenant_id, command.job_id)]
        state = JobState.FAILED_RETRYABLE if command.retryable else JobState.FAILED_FINAL
        failed = current.model_copy(update={
            "state": state,
            "lease_owner": None,
            "lease_until": None,
            "error_code": command.error_code,
        })
        self.jobs[(command.tenant_id, command.job_id)] = failed
        return failed

    def cancel_job(self, command: CancelJob, event: OutboxEvent) -> Job:
        self.mutations.append("cancel_job")
        self.events.append(event)
        if self.cancel_error is not None:
            raise self.cancel_error
        current = self.jobs[(command.tenant_id, command.job_id)]
        canceled = current.model_copy(update={"state": JobState.CANCELED})
        self.jobs[(command.tenant_id, command.job_id)] = canceled
        return canceled


def service(control: ControlPlane, **updates: object) -> JobLifecycle:
    values = {"max_attempts": DEFAULT_MAX_ATTEMPTS}
    return JobLifecycle(control, lambda: NOW, **(values | updates))


def test_agente_revogado_nao_reclama_job() -> None:
    revoked = agent(state=AgentState.REVOKED)
    job = pending_job()
    control = ControlPlane(current_agent=revoked, jobs=(job,))

    with pytest.raises(Conflict) as excinfo:
        service(control).claim(claim_for(job, revoked))

    assert excinfo.value.code is ErrorCode.AGENT_REVOKED
    assert control.mutations == []


def test_complete_rejeita_fence_antigo() -> None:
    job = leased_job()
    control = ControlPlane(current_agent=agent(), jobs=(job,))
    control.complete_error = FenceRejected(ErrorCode.FENCE_MISMATCH)

    with pytest.raises(FenceRejected):
        service(control).complete(complete_for(job, fencing_token=job.fencing_token - 1))

    assert control.mutations == ["complete_job"]
    assert control.jobs[(job.tenant_id, job.job_id)].state is JobState.LEASED


def test_agente_revogado_nao_completa_job() -> None:
    revoked = agent(state=AgentState.REVOKED)
    job = leased_job()
    control = ControlPlane(current_agent=revoked, jobs=(job,))

    with pytest.raises(Conflict) as excinfo:
        service(control).complete(complete_for(job))

    assert excinfo.value.code is ErrorCode.AGENT_REVOKED
    assert "complete_job" not in control.mutations


def test_agente_ausente_nao_bloqueia_claim() -> None:
    job = pending_job()
    control = ControlPlane(jobs=(job,))

    claimed = service(control).claim(claim_for(job, agent()))

    assert claimed is not None
    assert claimed.state is JobState.LEASED


def test_claim_emite_evento_job_claimed() -> None:
    job = pending_job()
    active = agent()
    control = ControlPlane(current_agent=active, jobs=(job,))
    emitted: list[OutboxEvent] = []

    service(control, emit=emitted.append).claim(claim_for(job, active))

    assert len(emitted) == 1
    event = emitted[0]
    assert event.event_type == "job.claimed"
    assert event.tenant_id == job.tenant_id
    assert event.aggregate_id == job.job_id
    assert event.delivered_at is None
    assert event.created_at == NOW


def test_claim_sem_candidato_nao_emite_evento() -> None:
    job = leased_job()
    active = agent()
    control = ControlPlane(current_agent=active, jobs=(job,))
    emitted: list[OutboxEvent] = []

    result = service(control, emit=emitted.append).claim(claim_for(job, active))

    assert result is None
    assert emitted == []


def test_renew_emite_evento_lease_renewed() -> None:
    job = leased_job()
    control = ControlPlane(jobs=(job,))
    emitted: list[OutboxEvent] = []

    service(control, emit=emitted.append).renew(renew_for(job))

    assert len(emitted) == 1
    assert emitted[0].event_type == "job.lease_renewed"
    assert emitted[0].aggregate_id == job.job_id


def test_renew_propaga_erro_sem_emitir_evento() -> None:
    job = leased_job()
    control = ControlPlane(jobs=(job,))
    control.renew_error = LeaseLost(ErrorCode.LEASE_EXPIRED)
    emitted: list[OutboxEvent] = []

    with pytest.raises(LeaseLost):
        service(control, emit=emitted.append).renew(renew_for(job))

    assert emitted == []


def test_complete_envia_evento_job_succeeded() -> None:
    job = leased_job()
    control = ControlPlane(current_agent=agent(), jobs=(job,))

    service(control).complete(complete_for(job))

    assert len(control.events) == 1
    assert control.events[0].event_type == "job.succeeded"
    assert control.events[0].aggregate_id == job.job_id


@pytest.mark.parametrize("bad_code", ["raw_timeout", "ERR-1", "A" * 65, "ERR.X"])
def test_fail_rejeita_error_code_fora_do_padrao(bad_code: str) -> None:
    job = leased_job()
    control = ControlPlane(jobs=(job,))

    with pytest.raises(ValueError, match="invalid_error_code"):
        service(control).fail(fail_for(job, error_code=bad_code))

    assert control.mutations == []


def test_fail_no_limite_de_tentativas_vira_final() -> None:
    job = leased_job(attempt=3)
    control = ControlPlane(jobs=(job,))

    service(control, max_attempts=3).fail(fail_for(job, retryable=True))

    assert control.fail_commands[-1].retryable is False


def test_fail_abaixo_do_limite_preserva_retryable() -> None:
    job = leased_job(attempt=1)
    control = ControlPlane(jobs=(job,))

    service(control, max_attempts=3).fail(fail_for(job, retryable=True))

    assert control.fail_commands[-1].retryable is True


def test_fail_nao_retryable_ignora_limite() -> None:
    job = leased_job(attempt=1)
    control = ControlPlane(jobs=(job,))

    service(control, max_attempts=3).fail(fail_for(job, retryable=False))

    assert control.fail_commands[-1].retryable is False


def test_fail_resync_preserva_retryable_no_limite() -> None:
    job = leased_job(attempt=5)
    control = ControlPlane(jobs=(job,))
    command = fail_for(
        job,
        retryable=False,
        error_code="RAW_RESYNC_BASE_UNKNOWN",
        rejected_manifest_sha256="c" * 64,
    )

    service(control, max_attempts=3).fail(command)

    assert control.fail_commands[-1].error_code == "RAW_RESYNC_BASE_UNKNOWN"


def test_fail_de_job_inexistente_delega_ao_port() -> None:
    control = ControlPlane()
    control.fail_error = NotFound(ErrorCode.JOB_MISSING)
    job = leased_job()

    with pytest.raises(NotFound):
        service(control, max_attempts=3).fail(fail_for(job, retryable=True))


def test_request_cancel_emite_evento_cancel_requested() -> None:
    job = leased_job()
    control = ControlPlane(jobs=(job,))

    service(control).request_cancel(cancel_for(job))

    assert len(control.events) == 1
    assert control.events[0].event_type == "job.cancel_requested"
    assert control.events[0].aggregate_id == job.job_id


def test_cancel_de_job_terminal_propaga_conflito() -> None:
    job = pending_job(state=JobState.SUCCEEDED, result_manifest_id="m", result_manifest_key=(
        f"raw/{TENANT}/CNES_LOCAL/{COMPETENCIA}/snap-1/manifest.json"
    ))
    control = ControlPlane(jobs=(job,))
    control.cancel_error = Conflict(ErrorCode.JOB_TERMINAL_CONFLICT)

    with pytest.raises(Conflict):
        service(control).request_cancel(cancel_for(job))


def test_max_attempts_invalido_rejeitado() -> None:
    control = ControlPlane()

    with pytest.raises(ValueError, match="positive_value_required"):
        JobLifecycle(control, lambda: NOW, max_attempts=0)


def test_event_id_e_deterministico() -> None:
    job = leased_job()
    control_a = ControlPlane(jobs=(job,))
    control_b = ControlPlane(jobs=(job,))
    events_a: list[OutboxEvent] = []
    events_b: list[OutboxEvent] = []

    service(control_a, emit=events_a.append).renew(renew_for(job))
    service(control_b, emit=events_b.append).renew(renew_for(job))

    assert events_a[0].event_id == events_b[0].event_id
