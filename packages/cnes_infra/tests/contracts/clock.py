"""Relógio mutável para suites de contrato."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import Any

from cnes_domain.control_plane.commands import (
    ClaimJob,
    ClaimRunUnit,
    CommitRunUnit,
    CompleteJob,
    FailJob,
    PutRunUnits,
    RenewJobLease,
    ReserveRunDispatch,
)
from cnes_domain.control_plane.entities import (
    Agent,
    Job,
    ManifestRef,
    OutboxEvent,
    RawManifestRecord,
    Run,
    RunDependency,
    RunUnit,
)
from cnes_domain.control_plane.enums import AgentState, JobState, RunStage, RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost
from cnes_domain.control_plane.transitions import transition_run

_NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)
_TENANT = "354130"
_HASH_A = "a" * 64
_WAVE_A = "a" * 16


@dataclass(slots=True)
class MutableClock:
    """Controla deterministicamente o instante observado pelo adapter."""

    instant: datetime

    def __init__(self, initial: datetime) -> None:
        self.instant = initial

    def now(self) -> datetime:
        """Retorna o instante atual."""
        return self.instant

    def advance(self, delta: timedelta) -> None:
        """Avança o relógio pelo intervalo informado."""
        self.instant += delta


def _claim_job(job_id: str, owner: str, clock: MutableClock) -> ClaimJob:
    return ClaimJob(
        tenant_id=_TENANT, job_id=job_id, owner=owner, now=clock.now(), lease_seconds=30
    )


def _renew_job(clock: MutableClock) -> RenewJobLease:
    return RenewJobLease(
        tenant_id=_TENANT, job_id="job-a", owner="worker-a", fencing_token=1,
        now=clock.now(), lease_seconds=60,
    )


def _fail_job(owner: str, fence: int, error_code: str) -> FailJob:
    return FailJob(
        tenant_id=_TENANT, job_id="job-a", owner=owner, fencing_token=fence,
        error_code=error_code, retryable=True,
    )


def _claim_unit_command(
    dispatch_id: str, owner: str, clock: MutableClock, unit_id: str = "unit-a"
) -> ClaimRunUnit:
    return ClaimRunUnit(
        tenant_id=_TENANT, run_id="run-a", unit_id=unit_id, dispatch_id=dispatch_id,
        owner=owner, now=clock.now(), lease_seconds=30,
    )


def _commit_command(dispatch_id: str, owner: str, fence: int) -> CommitRunUnit:
    return CommitRunUnit(
        tenant_id=_TENANT, run_id="run-a", unit_id="unit-a", dispatch_id=dispatch_id,
        owner=owner, fencing_token=fence, output_manifests=(_ref("output"),),
    )


def _expect_error(
    expected: type[Exception] | tuple[type[Exception], ...], action: Callable[[], Any]
) -> None:
    try:
        action()
    except expected:
        return
    raise AssertionError(f"error_not_raised={expected}")


def _assert_active_job_fences(adapter: Any, clock: MutableClock) -> CompleteJob:
    manifest = _raw_record("result", "agent-a", 1, clock.now())
    complete = CompleteJob(
        tenant_id=_TENANT, job_id="job-a", owner="worker-a", fencing_token=1,
        manifest=manifest,
    )
    renewals = (
        _renew_job(clock).model_copy(update={"owner": "other"}),
        _renew_job(clock).model_copy(update={"fencing_token": 2}),
    )
    for command in renewals:
        _expect_error(
            (FenceRejected, LeaseLost),
            lambda command=command: adapter.renew_job_lease(command),
        )
    completions = (
        complete.model_copy(update={"owner": "other"}),
        complete.model_copy(update={"fencing_token": 2}),
    )
    for command in completions:
        _expect_error((FenceRejected, LeaseLost), lambda command=command: adapter.complete_job(
            command, _event("invalid-complete")
        ))
    return complete


def _assert_job_failures(adapter: Any, clock: MutableClock) -> None:
    failed_event = _event("failed-retryable")
    failed = adapter.fail_job(_fail_job("worker-b", 2, "retry"), failed_event)
    assert (failed.state, failed.lease_owner, failed.lease_until) == (
        JobState.FAILED_RETRYABLE, None, None
    )
    assert adapter.pending_outbox(100).count(failed_event) == 1
    final_claim = adapter.claim_job(_claim_job("job-a", "worker-c", clock))
    assert final_claim is not None
    final_event = _event("failed-final")
    final_command = _fail_job("worker-c", 3, "permanent").model_copy(
        update={"retryable": False}
    )
    final = adapter.fail_job(final_command, final_event)
    assert final.state is JobState.FAILED_FINAL
    assert adapter.get_job(_TENANT, "job-a") == final
    assert adapter.pending_outbox(100).count(final_event) == 1


def _event(event_id: str, now: datetime = _NOW, aggregate_id: str | None = None) -> OutboxEvent:
    return OutboxEvent(
        tenant_id=_TENANT, event_id=event_id, event_type="test.event",
        aggregate_id=aggregate_id or event_id, payload={"event_id": event_id},
        created_at=now, delivered_at=None,
    )


def _agent(agent_id: str, state: AgentState = AgentState.ACTIVE) -> Agent:
    return Agent(
        tenant_id=_TENANT, agent_id=agent_id, state=state, version="1.0",
        certificate_fingerprint=_HASH_A, last_seen_at=None, created_at=_NOW,
    )


def _job(job_id: str, agent_id: str = "agent-a") -> Job:
    return Job(
        tenant_id=_TENANT, job_id=job_id, agent_id=agent_id, source_type="CNES",
        file_subtype="ST", competencia="2026-07", requested_snapshot_mode="FULL",
        state=JobState.PENDING, attempt=0, fencing_token=0, lease_owner=None,
        lease_until=None, result_manifest_id=None, result_manifest_key=None,
        error_code=None, created_at=_NOW,
    )


def _raw_record(
    snapshot_id: str,
    agent_id: str,
    sequence: int,
    created_at: datetime,
) -> RawManifestRecord:
    base = None if sequence == 1 else f"base-{agent_id}"
    return RawManifestRecord(
        tenant_id=_TENANT,
        manifest_id=f"manifest-{agent_id}-{snapshot_id}",
        manifest_key=f"raw/{_TENANT}/CNES/2026-07/{snapshot_id}/manifest.json",
        agent_id=agent_id,
        source_type="CNES",
        file_subtype="ST",
        competencia="2026-07",
        snapshot_mode="FULL" if sequence == 1 else "DELTA",
        snapshot_id=snapshot_id,
        base_snapshot_id=base,
        sequence=sequence,
        previous_manifest_sha256=None if sequence == 1 else _HASH_A,
        manifest_sha256=_HASH_A,
        created_at=created_at,
    )


def _store_record(adapter: Any, record: RawManifestRecord, clock: MutableClock) -> None:
    job = _job(f"job-{record.agent_id}-{record.snapshot_id}", record.agent_id)
    adapter.put_agent(_agent(record.agent_id))
    adapter.create_job(job, _event(f"created-{job.job_id}"))
    claimed = adapter.claim_job(_claim_job(job.job_id, "raw-worker", clock))
    assert claimed is not None
    complete = CompleteJob(
        tenant_id=_TENANT,
        job_id=job.job_id,
        owner="raw-worker",
        fencing_token=claimed.fencing_token,
        manifest=record,
    )
    event = _event(f"completed-{job.job_id}")
    completed = adapter.complete_job(complete, event)
    assert (completed.state, completed.lease_owner, completed.lease_until) == (
        JobState.SUCCEEDED, None, None
    )
    assert (completed.result_manifest_id, completed.result_manifest_key) == (
        record.manifest_id, record.manifest_key
    )
    assert adapter.get_job(record.tenant_id, job.job_id) == completed
    assert adapter.latest_succeeded_job(
        record.tenant_id, record.agent_id, record.source_type,
        record.file_subtype, record.competencia,
    ) == completed
    assert adapter.pending_outbox(100).count(event) == 1


def _run(
    run_id: str,
    state: RunState = RunState.PROCESSING,
    dependencies: tuple[RunDependency, ...] | None = None,
) -> Run:
    deps = dependencies or (RunDependency(source_type="CNES", file_subtype="ST", required=True),)
    return Run(
        tenant_id=_TENANT, run_id=run_id, competencia="2026-07", dataset_name="gold",
        state=state, dependencies=deps, missing_sources=(), created_at=_NOW,
    )


def _ref(name: str) -> ManifestRef:
    key = f"raw/{_TENANT}/CNES/2026-07/{name}/manifest.json"
    return ManifestRef(manifest_id=name, manifest_key=key)


def _unit(unit_id: str, state: RunUnitState = RunUnitState.PENDING) -> RunUnit:
    return RunUnit(
        tenant_id=_TENANT, run_id="run-a", unit_id=unit_id, stage=RunStage.NORMALIZE,
        source_type="CNES", file_subtype="ST", partition="all", depends_on_unit_ids=(),
        input_manifests=(_ref(f"input-{unit_id}"),), state=state, attempt=0,
        fencing_token=0, lease_owner=None, lease_until=None, dispatch_id=None,
        output_manifests=(), error_code=None,
    )


def _put_units(adapter: Any, units: tuple[Any, ...]) -> tuple[Any, ...]:
    return adapter.put_run_units(
        PutRunUnits(
            tenant_id=_TENANT,
            run_id="run-a",
            expected_run_state=RunState.PROCESSING,
            units=units,
        )
    )


def _reserve(
    adapter: Any,
    clock: MutableClock,
    wave: str = _WAVE_A,
    unit_ids: tuple[str, ...] = ("unit-a",),
) -> Any:
    return adapter.reserve_run_dispatch(
        ReserveRunDispatch(
            tenant_id=_TENANT,
            run_id="run-a",
            wave_id=wave,
            unit_ids=unit_ids,
            now=clock.now(),
            lease_seconds=30,
        )
    )


def _claim_unit(adapter: Any, clock: MutableClock, dispatch_id: str, owner: str) -> Any:
    return adapter.claim_run_unit(_claim_unit_command(dispatch_id, owner, clock))


def _prepare_unit(
    adapter: Any, clock: MutableClock, unit_ids: tuple[str, ...] = ("unit-a",)
) -> Any:
    adapter.put_run(_run("run-a"))
    _put_units(adapter, (_unit("unit-a"), _unit("unit-b")))
    return _reserve(adapter, clock, unit_ids=unit_ids)


class _HarnessState:
    def __init__(self, clock: MutableClock, mutation: str | None = None) -> None:
        self.clock = clock
        self.mutation = mutation
        self.memberships: dict[tuple[str, str], Any] = {}
        self.tenants: dict[str, Any] = {}
        self.agents: dict[tuple[str, str], Any] = {}
        self.jobs: dict[tuple[str, str], Any] = {}
        self.raw_records: list[Any] = []
        self.runs: dict[tuple[str, str], Any] = {}
        self.units: dict[tuple[str, str, str], Any] = {}
        self.dispatches: dict[tuple[str, str], Any] = {}
        self.idempotency: dict[tuple[str, str, str], Any] = {}
        self.versions: dict[tuple[str, str, str], Any] = {}
        self.pointers: dict[tuple[str, str, str], Any] = {}
        self.access_requests: dict[tuple[str, str], Any] = {}
        self.outbox: dict[str, Any] = {}

    def put_tenant(self, tenant: Any) -> None:
        self.tenants[tenant.tenant_id] = tenant

    def get_tenant(self, tenant_id: str) -> Any | None:
        return self.tenants.get(tenant_id)

    def put_membership(self, membership: Any) -> None:
        self.memberships[(membership.tenant_id, membership.user_id)] = membership

    def get_membership(self, tenant_id: str, user_id: str) -> Any | None:
        if self.mutation == "authorization_jobs":
            return next(iter(self.memberships.values()), None)
        return self.memberships.get((tenant_id, user_id))

    def put_agent(self, agent: Any) -> None:
        self.agents[(agent.tenant_id, agent.agent_id)] = agent

    def get_agent(self, tenant_id: str, agent_id: str) -> Any | None:
        return self.agents.get((tenant_id, agent_id))

    def create_job(self, job: Any, event: Any) -> Any:
        key = (job.tenant_id, job.job_id)
        existing = self.jobs.get(key)
        if existing is not None:
            if existing != job:
                raise Conflict("job_conflict")
            return existing
        self.jobs[key] = job
        self.outbox[event.event_id] = event
        return job

    def get_job(self, tenant_id: str, job_id: str) -> Any | None:
        return self.jobs.get((tenant_id, job_id))

    def latest_succeeded_job(self, *args: str) -> Any | None:
        tenant_id, agent_id, source_type, file_subtype, competencia = args
        matches = [
            job
            for job in self.jobs.values()
            if (job.tenant_id, job.agent_id, job.source_type, job.file_subtype, job.competencia)
            == (tenant_id, agent_id, source_type, file_subtype, competencia)
            and job.state.value == "SUCCEEDED"
        ]
        return max(matches, key=lambda job: (job.created_at, job.job_id), default=None)

    def cancel_job(self, command: Any, event: Any) -> Any:
        job = self.get_job(command.tenant_id, command.job_id)
        if job is None:
            raise Conflict("job_missing")
        canceled = job.model_copy(update={"state": "CANCEL_REQUESTED"})
        self.jobs[(job.tenant_id, job.job_id)] = canceled
        self.outbox[event.event_id] = event
        return canceled

    def put_run(self, run: Any) -> None:
        self.runs[(run.tenant_id, run.run_id)] = run

    def get_run(self, tenant_id: str, run_id: str) -> Any | None:
        return self.runs.get((tenant_id, run_id))

    def transition_run(self, command: Any, event: Any) -> Any:
        run = self.get_run(command.tenant_id, command.run_id)
        if run is None or run.state is not command.expected_state:
            raise Conflict("run_state_conflict")
        updated = transition_run(run, command.new_state).model_copy(
            update={"missing_sources": command.missing_sources}
        )
        self.put_run(updated)
        self.outbox[event.event_id] = event
        return updated

    def list_run_units(self, tenant_id: str, run_id: str) -> tuple[Any, ...]:
        values = [unit for key, unit in self.units.items() if key[:2] == (tenant_id, run_id)]
        return tuple(sorted(values, key=lambda item: item.unit_id))

    def get_dataset_version(self, *args: str) -> Any | None:
        return self.versions.get(tuple(args))

    def get_dataset_pointer(self, tenant_id: str, dataset_name: str) -> Any | None:
        return self.pointers.get((tenant_id, dataset_name, "current"))

    def put_access_request(self, request: Any, event: Any) -> None:
        self.access_requests[(request.tenant_id, request.request_id)] = request
        self.outbox[event.event_id] = event

    def get_access_request(self, tenant_id: str, request_id: str) -> Any | None:
        return self.access_requests.get((tenant_id, request_id))

    def decide_access_request(self, request: Any, event: Any) -> Any:
        self.put_access_request(request, event)
        return request

    def pending_outbox(self, limit: int) -> tuple[Any, ...]:
        values = sorted(self.outbox.values(), key=lambda item: (item.created_at, item.event_id))
        return tuple(event for event in values if event.delivered_at is None)[:limit]

    def mark_outbox_delivered(self, event_id: str, delivered_at: datetime) -> None:
        event = self.outbox[event_id]
        self.outbox[event_id] = event.model_copy(update={"delivered_at": delivered_at})

    def get_active_run_dispatch(self, tenant_id: str, run_id: str) -> Any | None:
        dispatch = self.dispatches.get((tenant_id, run_id))
        active = dispatch and dispatch.state.value != "TERMINAL"
        return dispatch if active and dispatch.lease_until > self.clock.now() else None

    @staticmethod
    def _select_raw_chain(records: list[Any]) -> list[Any]:
        candidates = sorted(
            records,
            key=lambda item: (item.created_at, item.agent_id, item.snapshot_id),
            reverse=True,
        )
        for head in candidates:
            base = head.snapshot_id if head.sequence == 1 else head.base_snapshot_id
            chain = [
                item
                for item in records
                if item.agent_id == head.agent_id
                and base in (item.snapshot_id, item.base_snapshot_id)
                and item.sequence <= head.sequence
            ]
            chain.sort(key=lambda item: item.sequence)
            sequences = [item.sequence for item in chain]
            hashes_match = all(
                current.previous_manifest_sha256 == previous.manifest_sha256
                for previous, current in pairwise(chain)
            )
            if sequences == list(range(1, head.sequence + 1)) and hashes_match:
                return chain
        return []
