"""Casos reutilizáveis de conformidade do plano de controle."""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from cnes_domain.control_plane.commands import (
    BeginIdempotency,
    BindRunDispatch,
    CompleteJob,
    FailRunUnit,
    FinalizeRunCancellation,
    FinishRunDispatch,
    PublicationPermit,
    PublishDataset,
    PutRunUnits,
    ReserveRunDispatch,
    TransitionRun,
)
from cnes_domain.control_plane.entities import (
    DatasetVersion,
    Membership,
    RawManifestRecord,
    RunDependency,
)
from cnes_domain.control_plane.enums import (
    AgentState,
    DispatchOutcome,
    RunState,
    RunUnitState,
)
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost
from cnes_domain.control_plane.ids import run_dependency_key
from packages.cnes_infra.tests.contracts.clock import (
    _HASH_A,
    _NOW,
    _TENANT,
    MutableClock,
    _agent,
    _claim_job,
    _claim_unit_command,
    _commit_command,
    _event,
    _expect_error,
    _fail_job,
    _job,
    _renew_job,
    _run,
    _unit,
)

_Runner = Callable[[Any, MutableClock], None]
_HASH_B = "b" * 64
_WAVE_A = "a" * 16
_WAVE_B = "b" * 16


@dataclass(frozen=True, slots=True)
class ControlPlaneCase:
    """Executa uma invariável contra um adapter de plano de controle."""

    name: str
    _runner: _Runner = field(repr=False, compare=False)

    def run(self, adapter: Any, clock: MutableClock) -> None:
        """Executa o caso e identifica qualquer falha pelo nome."""
        try:
            self._runner(adapter, clock)
        except Exception as error:
            raise AssertionError(f"case={self.name}") from error


def _case_authorization_jobs(adapter: Any, clock: MutableClock) -> None:
    membership = Membership(
        tenant_id=_TENANT, user_id="user-a", role="admin", created_at=clock.now()
    )
    adapter.put_membership(membership)
    assert adapter.get_membership(_TENANT, "user-a") == membership
    assert adapter.get_membership("other", "user-a") is None
    adapter.put_agent(_agent("agent-a", AgentState.REVOKED))
    adapter.create_job(_job("job-a"), _event("job-created"))
    assert adapter.list_claimable_jobs(_TENANT, "agent-a", 10) == ()
    assert adapter.claim_job(_claim_job("job-a", "worker-a", clock)) is None
    adapter.put_agent(_agent("agent-a"))
    claimed = adapter.claim_job(_claim_job("job-a", "worker-a", clock))
    assert claimed is not None
    assert (claimed.attempt, claimed.fencing_token) == (1, 1)
    assert claimed.lease_until == clock.now() + timedelta(seconds=30)
    renewed = adapter.renew_job_lease(_renew_job(clock))
    assert renewed.lease_until == clock.now() + timedelta(seconds=60)
    assert adapter.claim_job(_claim_job("job-a", "worker-b", clock)) is None
    clock.advance(timedelta(seconds=61))
    expired = _fail_job("worker-a", 1, "expired")
    _expect_error(LeaseLost, lambda: adapter.fail_job(expired, _event("expired")))
    retried = adapter.claim_job(_claim_job("job-a", "worker-b", clock))
    assert retried is not None
    assert retried.job_id == "job-a"
    assert (retried.attempt, retried.fencing_token) == (2, 2)
    stale = _fail_job("worker-a", 1, "stale")
    _expect_error((FenceRejected, LeaseLost), lambda: adapter.fail_job(stale, _event("stale")))
    stale_fence = stale.model_copy(update={"owner": "worker-b"})
    _expect_error(
        FenceRejected, lambda: adapter.fail_job(stale_fence, _event("stale-fence"))
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
    adapter.complete_job(complete, _event(f"completed-{job.job_id}"))


def _case_raw_chains(adapter: Any, clock: MutableClock) -> None:
    records = (
        _raw_record("base-agent-a", "agent-a", 1, _NOW),
        _raw_record("delta-2", "agent-a", 2, _NOW + timedelta(seconds=1)),
        _raw_record("delta-3", "agent-a", 3, _NOW + timedelta(seconds=2)),
        _raw_record("base-agent-b", "agent-b", 1, _NOW),
        _raw_record("delta-z", "agent-b", 2, _NOW + timedelta(seconds=2)),
        _raw_record("orphan", "agent-z", 2, _NOW + timedelta(seconds=3)),
    )
    for record in records:
        _store_record(adapter, record, clock)
    chain = adapter.list_raw_manifest_chain(_TENANT, "CNES", "ST", "2026-07", 2)
    assert tuple(ref.manifest_id for ref in chain) == (
        "manifest-agent-b-base-agent-b",
        "manifest-agent-b-delta-z",
    )
    try:
        short = adapter.list_raw_manifest_chain(_TENANT, "CNES", "ST", "2026-07", 1)
    except Conflict:
        pass
    else:
        assert short == ()


def _case_run_discovery(adapter: Any, clock: MutableClock) -> None:
    deps = (
        RunDependency(source_type="CNES", file_subtype="ST", required=True),
        RunDependency(source_type="CNES_ST", file_subtype="X", required=True),
    )
    adapter.put_run(_run("waiting-a", RunState.WAITING_INPUTS, deps))
    adapter.put_run(_run("waiting-b", RunState.WAITING_INPUTS))
    adapter.put_run(_run("collision", RunState.WAITING_INPUTS, (deps[1],)))
    adapter.put_run(_run("processing", RunState.PROCESSING))
    adapter.put_run(_run("publishing", RunState.PUBLISHING))
    adapter.put_run(_run("canceling", RunState.CANCEL_REQUESTED))
    adapter.put_run(_run("published", RunState.PUBLISHED))
    waiting = adapter.list_waiting_runs_for_dependency(_TENANT, "CNES", "ST", "2026-07", 10)
    assert tuple(run.run_id for run in waiting) == ("waiting-a", "waiting-b")
    assert run_dependency_key(_TENANT, "CNES", "ST", "2026-07") != run_dependency_key(
        _TENANT, "CNES_ST", "X", "2026-07"
    )
    recoverable = adapter.list_recoverable_runs(clock.now(), 10)
    assert tuple(run.run_id for run in recoverable) == (
        "canceling",
        "collision",
        "processing",
        "publishing",
        "waiting-a",
        "waiting-b",
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


def _case_run_units_atomic(adapter: Any, clock: MutableClock) -> None:
    adapter.put_run(_run("run-a"))
    units = (_unit("unit-a"), _unit("unit-b"))
    assert _put_units(adapter, units) == units
    assert _put_units(adapter, units) == units
    divergent = (units[0], _unit("unit-c"))
    _expect_error(Conflict, lambda: _put_units(adapter, divergent))
    assert adapter.list_run_units(_TENANT, "run-a") == units


def _reserve(adapter: Any, clock: MutableClock, wave: str = _WAVE_A) -> Any:
    return adapter.reserve_run_dispatch(
        ReserveRunDispatch(
            tenant_id=_TENANT,
            run_id="run-a",
            wave_id=wave,
            unit_ids=("unit-a",),
            now=clock.now(),
            lease_seconds=30,
        )
    )


def _claim_unit(adapter: Any, clock: MutableClock, dispatch_id: str, owner: str) -> Any:
    return adapter.claim_run_unit(_claim_unit_command(dispatch_id, owner, clock))


def _prepare_unit(adapter: Any, clock: MutableClock) -> Any:
    adapter.put_run(_run("run-a"))
    _put_units(adapter, (_unit("unit-a"), _unit("unit-b")))
    return _reserve(adapter, clock)


def _case_unit_claim(adapter: Any, clock: MutableClock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    assert claimed is not None
    not_dispatched = _claim_unit_command(dispatch.dispatch_id, "worker-a", clock, "unit-b")
    assert adapter.claim_run_unit(not_dispatched) is None
    assert (claimed.attempt, claimed.fencing_token) == (1, 1)
    assert claimed.dispatch_id == dispatch.dispatch_id
    assert _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-b") is None
    clock.advance(timedelta(seconds=31))
    replacement = _reserve(adapter, clock)
    retried = _claim_unit(adapter, clock, replacement.dispatch_id, "worker-b")
    assert retried is not None
    assert retried.unit_id == "unit-a"
    assert (retried.attempt, retried.fencing_token) == (2, 2)
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    clock.advance(timedelta(seconds=31))
    assert _claim_unit(adapter, clock, replacement.dispatch_id, "worker-c") is None


def _case_unit_fences(adapter: Any, clock: MutableClock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    assert claimed is not None
    invalid = (
        _commit_command(dispatch.dispatch_id, "other", claimed.fencing_token),
        _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token + 1),
        _commit_command(_WAVE_B, "worker-a", claimed.fencing_token),
    )
    for command in invalid:
        _expect_error((FenceRejected, LeaseLost), lambda command=command: adapter.commit_run_unit(
            command, _event(f"invalid-{command.owner}-{command.fencing_token}")
        ))
    stale_fail = FailRunUnit(
        tenant_id=_TENANT, run_id="run-a", unit_id="unit-a", dispatch_id=_WAVE_B,
        owner="worker-a", fencing_token=claimed.fencing_token, error_code="stale",
        retryable=True,
    )
    _expect_error(
        (FenceRejected, LeaseLost),
        lambda: adapter.fail_run_unit(stale_fail, _event("invalid-fail")),
    )
    clock.advance(timedelta(seconds=31))
    expired = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    _expect_error(LeaseLost, lambda: adapter.commit_run_unit(expired, _event("expired-unit")))
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    _expect_error(
        (FenceRejected, LeaseLost),
        lambda: adapter.commit_run_unit(
            _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token),
            _event("invalid-parent"),
        ),
    )


def _case_degraded_unit(adapter: Any, clock: MutableClock) -> None:
    optional = (RunDependency(source_type="CNES", file_subtype="ST", required=False),)
    adapter.put_run(_run("run-a", dependencies=optional))
    _put_units(adapter, (_unit("unit-a"),))
    dispatch = _reserve(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    assert claimed is not None
    command = FailRunUnit(
        tenant_id=_TENANT,
        run_id="run-a",
        unit_id="unit-a",
        dispatch_id=dispatch.dispatch_id,
        owner="worker-a",
        fencing_token=claimed.fencing_token,
        error_code="optional_failed",
        retryable=False,
    )
    failed = adapter.fail_run_unit(command, _event("unit-degraded"))
    assert failed.state is RunUnitState.SUCCEEDED_DEGRADED
    assert failed.output_manifests == ()
    assert adapter.get_run(_TENANT, "run-a").missing_sources == ("CNES/ST",)


def _case_cancellation(adapter: Any, clock: MutableClock) -> None:
    adapter.put_run(_run("run-a"))
    _put_units(adapter, (_unit("unit-a"), _unit("unit-b")))
    dispatch = _reserve(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    assert claimed is not None
    adapter.commit_run_unit(
        _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token),
        _event("unit-completed"),
    )
    adapter.transition_run(
        TransitionRun(
            tenant_id=_TENANT,
            run_id="run-a",
            expected_state=RunState.PROCESSING,
            new_state=RunState.CANCEL_REQUESTED,
        ),
        _event("run-cancel-requested"),
    )
    command = FinalizeRunCancellation(
        tenant_id=_TENANT,
        run_id="run-a",
        expected_state=RunState.CANCEL_REQUESTED,
        canceled_at=clock.now(),
    )
    event = _event("run-canceled")
    result = adapter.finalize_run_cancellation(command, event)
    assert result.state is RunState.CANCELED
    states = {unit.unit_id: unit.state for unit in adapter.list_run_units(_TENANT, "run-a")}
    assert states == {"unit-a": RunUnitState.SUCCEEDED, "unit-b": RunUnitState.CANCELED}
    assert adapter.finalize_run_cancellation(command, event) == result
    assert adapter.pending_outbox(10).count(event) == 1


def _case_dispatch(adapter: Any, clock: MutableClock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    assert _reserve(adapter, clock) == dispatch
    _expect_error(Conflict, lambda: _reserve(adapter, clock, _WAVE_B))
    bind = BindRunDispatch(
        tenant_id=_TENANT,
        run_id="run-a",
        dispatch_id=dispatch.dispatch_id,
        execution_ref="exec-a",
        now=clock.now(),
        lease_seconds=30,
    )
    started = adapter.bind_run_dispatch(bind)
    assert adapter.bind_run_dispatch(bind) == started
    _expect_error(
        Conflict,
        lambda: adapter.bind_run_dispatch(bind.model_copy(update={"execution_ref": "x"})),
    )
    _expect_error(Conflict, lambda: adapter.finish_run_dispatch(FinishRunDispatch(
        tenant_id=_TENANT, run_id="run-a", dispatch_id=_WAVE_B,
        outcome=DispatchOutcome.SUCCEEDED, finished_at=clock.now()
    )))
    finished = adapter.finish_run_dispatch(FinishRunDispatch(
        tenant_id=_TENANT, run_id="run-a", dispatch_id=dispatch.dispatch_id,
        outcome=DispatchOutcome.SUCCEEDED, finished_at=clock.now()
    ))
    assert finished.terminal_outcome is DispatchOutcome.SUCCEEDED
    assert _reserve(adapter, clock, _WAVE_B).generation == 2


def _case_dispatch_expiry(adapter: Any, clock: MutableClock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    assert claimed is not None
    clock.advance(timedelta(seconds=31))
    assert adapter.get_active_run_dispatch(_TENANT, "run-a") is None
    renewed = _reserve(adapter, clock, _WAVE_B)
    assert renewed.generation == 2
    claimed = _claim_unit(adapter, clock, renewed.dispatch_id, "worker-b")
    assert claimed is not None
    adapter.bind_run_dispatch(
        BindRunDispatch(
            tenant_id=_TENANT,
            run_id="run-a",
            dispatch_id=renewed.dispatch_id,
            execution_ref="short-dispatch",
            now=clock.now(),
            lease_seconds=1,
        )
    )
    clock.advance(timedelta(seconds=2))
    _expect_error(Conflict, lambda: _reserve(adapter, clock, "c" * 16))


def _case_idempotency(adapter: Any, clock: MutableClock) -> None:
    first = BeginIdempotency(
        tenant_id=_TENANT, scope="jobs", key="key-a", request_hash=_HASH_A,
        resource_id="resource-a", now=clock.now(), expires_at=clock.now() + timedelta(minutes=5)
    )
    created = adapter.begin_idempotency(first)
    replay = adapter.begin_idempotency(first.model_copy(update={"resource_id": "resource-b"}))
    assert created.created
    assert not replay.created
    assert replay.record.resource_id == "resource-a"
    _expect_error(Conflict, lambda: adapter.begin_idempotency(
        first.model_copy(update={"request_hash": _HASH_B})
    ))
    clock.advance(timedelta(minutes=6))
    replaced = adapter.begin_idempotency(first.model_copy(update={
        "request_hash": _HASH_B,
        "resource_id": "resource-b",
        "now": clock.now(),
        "expires_at": clock.now() + timedelta(minutes=5),
    }))
    assert replaced.created
    assert replaced.record.resource_id == "resource-b"


def _publish(run_id: str, event_id: str, expected: str | None, degraded: bool) -> PublishDataset:
    state = RunState.PUBLISHED_DEGRADED if degraded else RunState.PUBLISHED
    version = DatasetVersion(
        tenant_id=_TENANT, dataset_name="gold", version_id=run_id, run_id=run_id,
        run_manifest_key=f"reconciliation/{_TENANT}/2026-07/{run_id}/run-manifest.json",
        created_at=_NOW,
    )
    return PublishDataset(
        version=version,
        pointer_name="current",
        expected_version_id=expected,
        final_state=state,
        missing_sources=("CNES/ST",) if degraded else (),
        publication_permit=PublicationPermit(
            tenant_id=_TENANT, run_id=run_id, policy_version=1, fencing_token=1
        ),
        event=_event(event_id, aggregate_id=run_id),
    )


def _case_publication(adapter: Any, clock: MutableClock) -> None:
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    first = _publish("run-a", "published-a", None, False)
    pointer = adapter.publish_dataset(first)
    assert pointer.version_id == "run-a"
    assert adapter.get_run(_TENANT, "run-a").state is RunState.PUBLISHED
    assert adapter.publish_dataset(first) == pointer
    assert adapter.pending_outbox(10).count(first.event) == 1
    changed = first.model_copy(update={
        "version": first.version.model_copy(update={
            "run_manifest_key": f"reconciliation/{_TENANT}/2026-06/run-a/run-manifest.json"
        })
    })
    _expect_error(Conflict, lambda: adapter.publish_dataset(changed))
    assert adapter.get_dataset_version(_TENANT, "gold", "run-a") == first.version
    adapter.put_run(_run("run-b", RunState.PUBLISHING))
    conflict = _publish("run-b", "published-b", "stale", True)
    _expect_error(Conflict, lambda: adapter.publish_dataset(conflict))
    assert adapter.get_dataset_version(_TENANT, "gold", "run-b") is None
    assert adapter.get_run(_TENANT, "run-b").state is RunState.PUBLISHING
    assert conflict.event not in adapter.pending_outbox(10)
    success = conflict.model_copy(update={"expected_version_id": "run-a"})
    adapter.publish_dataset(success)
    run = adapter.get_run(_TENANT, "run-b")
    assert run.state is RunState.PUBLISHED_DEGRADED
    assert run.missing_sources == ("CNES/ST",)


def control_plane_cases() -> tuple[ControlPlaneCase, ...]:
    """Retorna o catálogo estável de invariáveis do plano de controle."""
    return (
        ControlPlaneCase("authorization_jobs", _case_authorization_jobs),
        ControlPlaneCase("raw_chains", _case_raw_chains),
        ControlPlaneCase("run_discovery", _case_run_discovery),
        ControlPlaneCase("run_units_atomic", _case_run_units_atomic),
        ControlPlaneCase("unit_claim", _case_unit_claim),
        ControlPlaneCase("unit_fences", _case_unit_fences),
        ControlPlaneCase("degraded_unit", _case_degraded_unit),
        ControlPlaneCase("cancellation", _case_cancellation),
        ControlPlaneCase("dispatch", _case_dispatch),
        ControlPlaneCase("dispatch_expiry", _case_dispatch_expiry),
        ControlPlaneCase("idempotency", _case_idempotency),
        ControlPlaneCase("publication", _case_publication),
    )
