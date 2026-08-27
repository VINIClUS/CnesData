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
    FailJob,
    RenewJobLease,
)
from cnes_domain.control_plane.entities import (
    Agent,
    Job,
    ManifestRef,
    OutboxEvent,
    Run,
    RunDependency,
    RunUnit,
)
from cnes_domain.control_plane.enums import AgentState, JobState, RunStage, RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.transitions import transition_run

_NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)
_TENANT = "354130"
_HASH_A = "a" * 64


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


class _HarnessState:
    def __init__(self, clock: MutableClock, mutation: str | None = None) -> None:
        self.clock = clock
        self.mutation = mutation
        self.memberships: dict[tuple[str, str], Any] = {}
        self.agents: dict[tuple[str, str], Any] = {}
        self.jobs: dict[tuple[str, str], Any] = {}
        self.raw_records: list[Any] = []
        self.runs: dict[tuple[str, str], Any] = {}
        self.units: dict[tuple[str, str, str], Any] = {}
        self.dispatches: dict[tuple[str, str], Any] = {}
        self.idempotency: dict[tuple[str, str, str], Any] = {}
        self.versions: dict[tuple[str, str, str], Any] = {}
        self.pointers: dict[tuple[str, str, str], Any] = {}
        self.outbox: dict[str, Any] = {}

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

    def pending_outbox(self, limit: int) -> tuple[Any, ...]:
        values = sorted(self.outbox.values(), key=lambda item: (item.created_at, item.event_id))
        return tuple(event for event in values if event.delivered_at is None)[:limit]

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
