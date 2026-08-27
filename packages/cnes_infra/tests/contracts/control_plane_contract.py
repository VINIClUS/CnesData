"""Casos reutilizáveis de conformidade do plano de controle."""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from functools import partial
from typing import Any

from cnes_domain.control_plane.commands import (
    BeginIdempotency,
    BindRunDispatch,
    FailRunUnit,
    FinalizeRunCancellation,
    FinishRunDispatch,
    PublicationPermit,
    PublishDataset,
    ReserveRunDispatch,
    TransitionRun,
)
from cnes_domain.control_plane.entities import (
    DatasetVersion,
    Membership,
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
from cnes_domain.ports.control_plane import ControlPlanePort
from packages.cnes_infra.tests.contracts.clock import (
    _HASH_A,
    _NOW,
    _TENANT,
    _WAVE_B,
    MutableClock,
    _agent,
    _assert_active_job_fences,
    _assert_committed_unit,
    _assert_job_failures,
    _assert_retryable_unit_failure,
    _assert_revoked_completion,
    _assert_unit_rejected,
    _claim_job,
    _claim_unit,
    _claim_unit_command,
    _commit_command,
    _event,
    _expect_error,
    _fail_job,
    _job,
    _prepare_unit,
    _put_units,
    _raw_record,
    _renew_job,
    _reserve,
    _run,
    _store_record,
    _unit,
)

_Runner = Callable[[Any, MutableClock], None]
_HASH_B = "b" * 64


@dataclass(frozen=True, slots=True)
class ControlPlaneCase:
    """Executa uma invariável contra um adapter de plano de controle."""

    name: str
    _runner: _Runner = field(repr=False, compare=False)

    def run(self, adapter: Any, clock: MutableClock) -> None:
        """Executa o caso e identifica qualquer falha pelo nome."""
        try:
            assert isinstance(adapter, ControlPlanePort)
            self._runner(adapter, clock)
        except Exception as error:
            raise AssertionError(f"case={self.name}") from error


def _case_authorization_jobs(adapter: Any, clock: MutableClock) -> None:
    membership = Membership(
        tenant_id=_TENANT, user_id="user-a", role="admin", created_at=clock.now())
    adapter.put_membership(membership)
    assert adapter.get_membership(_TENANT, "user-a") == membership
    assert adapter.get_membership("other", "user-a") is None
    adapter.put_agent(_agent("agent-a", AgentState.REVOKED))
    created_event = _event("job-created")
    adapter.create_job(_job("job-a"), created_event)
    assert adapter.pending_outbox(100).count(created_event) == 1
    assert adapter.list_claimable_jobs(_TENANT, "agent-a", 10) == ()
    assert adapter.claim_job(_claim_job("job-a", "worker-a", clock)) is None
    adapter.put_agent(_agent("agent-a"))
    second_event = _event("job-b-created")
    adapter.create_job(_job("job-b"), second_event)
    assert adapter.pending_outbox(100).count(second_event) == 1
    claimable = adapter.list_claimable_jobs(_TENANT, "agent-a", 1)
    assert tuple(job.job_id for job in claimable) == ("job-a",)
    claimed = adapter.claim_job(_claim_job("job-a", "worker-a", clock))
    assert claimed is not None
    assert (claimed.attempt, claimed.fencing_token) == (1, 1)
    assert claimed.lease_until == clock.now() + timedelta(seconds=30)
    renewed = adapter.renew_job_lease(_renew_job(clock))
    assert renewed.lease_until == clock.now() + timedelta(seconds=60)
    assert adapter.get_job(_TENANT, "job-a") == renewed
    complete = _assert_active_job_fences(adapter, clock)
    _assert_revoked_completion(adapter, complete)
    clock.advance(timedelta(seconds=31))
    assert adapter.claim_job(_claim_job("job-a", "worker-b", clock)) is None
    clock.advance(timedelta(seconds=30))
    _expect_error(LeaseLost, lambda: adapter.renew_job_lease(_renew_job(clock)))
    _expect_error(LeaseLost, lambda: adapter.complete_job(complete, _event("expired-complete")))
    expired = _fail_job("worker-a", 1, "expired")
    _expect_error(LeaseLost, lambda: adapter.fail_job(expired, _event("expired")))
    retried = adapter.claim_job(_claim_job("job-a", "worker-b", clock))
    assert retried is not None
    assert retried.job_id == "job-a"
    assert (retried.attempt, retried.fencing_token) == (2, 2)
    stale = _fail_job("worker-a", 1, "stale")
    _expect_error((FenceRejected, LeaseLost), lambda: adapter.fail_job(stale, _event("stale")))
    stale_fence = stale.model_copy(update={"owner": "worker-b"})
    _expect_error(FenceRejected,
                  lambda: adapter.fail_job(stale_fence, _event("stale-fence")))
    _assert_job_failures(adapter, clock)


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
    identities = ({"tenant_id": "other"}, {"source_type": "SIHD"},
                  {"file_subtype": "PF"}, {"competencia": "2026-06"})
    for index, identity in enumerate(identities):
        snapshot_id = f"foreign-{index}"
        update = {
            "agent_id": snapshot_id, "snapshot_id": snapshot_id,
            "manifest_id": f"manifest-{snapshot_id}", **identity,
            "created_at": _NOW + timedelta(minutes=index + 1),
        }
        item = records[0].model_copy(update=update)
        key = f"raw/{item.tenant_id}/{item.source_type}/{item.competencia}"
        item = item.model_copy(update={"manifest_key": f"{key}/{snapshot_id}/manifest.json"})
        _store_record(adapter, item, clock)
    chain = adapter.list_raw_manifest_chain(_TENANT, "CNES", "ST", "2026-07", 2)
    assert tuple((ref.manifest_id, ref.manifest_key) for ref in chain) == tuple(
        (record.manifest_id, record.manifest_key) for record in records[3:5]
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
    future = {"created_at": clock.now() + timedelta(days=1)}
    foreign = _run("foreign-tenant", RunState.WAITING_INPUTS)
    adapter.put_run(foreign.model_copy(update={"tenant_id": "other", **future}))
    foreign = _run("foreign-period", RunState.WAITING_INPUTS)
    adapter.put_run(foreign.model_copy(update={"competencia": "2026-06", **future}))
    waiting = adapter.list_waiting_runs_for_dependency(_TENANT, "CNES", "ST", "2026-07", 10)
    assert tuple(run.run_id for run in waiting) == ("waiting-a", "waiting-b")
    limited = adapter.list_waiting_runs_for_dependency(_TENANT, "CNES", "ST", "2026-07", 1)
    assert tuple(run.run_id for run in limited) == ("waiting-a",)
    assert run_dependency_key(_TENANT, "CNES", "ST", "2026-07") != run_dependency_key(
        _TENANT, "CNES_ST", "X", "2026-07"
    )
    recoverable = adapter.list_recoverable_runs(clock.now(), 6)
    assert tuple(run.run_id for run in recoverable) == (
        "canceling", "collision", "processing", "publishing", "waiting-a", "waiting-b")
    assert tuple(run.run_id for run in adapter.list_recoverable_runs(clock.now(), 2)) == (
        "canceling", "collision"
    )


def _case_run_units_atomic(adapter: Any, clock: MutableClock) -> None:
    adapter.put_run(_run("run-a"))
    units = (_unit("unit-a"), _unit("unit-b"))
    assert _put_units(adapter, units) == units
    assert _put_units(adapter, units) == units
    divergent = (units[0], _unit("unit-c"))
    _expect_error(Conflict, lambda: _put_units(adapter, divergent))
    assert adapter.list_run_units(_TENANT, "run-a") == units


def _case_unit_claim(adapter: Any, clock: MutableClock) -> None:
    unit_ids = ("unit-a", "unit-b")
    dispatch = _prepare_unit(adapter, clock, unit_ids)
    assert dispatch.unit_ids == unit_ids
    assert adapter.get_active_run_dispatch(_TENANT, "run-a") == dispatch
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    assert claimed is not None
    second_command = _claim_unit_command(dispatch.dispatch_id, "worker-a", clock, "unit-b")
    assert adapter.claim_run_unit(second_command) is not None
    not_dispatched = _claim_unit_command(dispatch.dispatch_id, "worker-a", clock, "unit-z")
    assert adapter.claim_run_unit(not_dispatched) is None
    assert (claimed.attempt, claimed.fencing_token) == (1, 1)
    assert claimed.dispatch_id == dispatch.dispatch_id
    assert _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-b") is None
    clock.advance(timedelta(seconds=31))
    expired_pending = _claim_unit_command(dispatch.dispatch_id, "worker-b", clock, "unit-b")
    assert adapter.claim_run_unit(expired_pending) is None
    replacement = _reserve(adapter, clock, unit_ids=unit_ids)
    retried = _claim_unit(adapter, clock, replacement.dispatch_id, "worker-b")
    assert retried is not None
    assert retried.unit_id == "unit-a"
    assert (retried.attempt, retried.fencing_token) == (2, 2)
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    assert _claim_unit(adapter, clock, replacement.dispatch_id, "worker-c") is None
    pending = _claim_unit_command(replacement.dispatch_id, "worker-c", clock, "unit-b")
    assert adapter.claim_run_unit(pending) is None


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
        event = _event(f"invalid-{command.owner}-{command.fencing_token}")
        action = partial(adapter.commit_run_unit, command, event)
        _assert_unit_rejected(adapter, (FenceRejected, LeaseLost), action)
    fail = FailRunUnit(
        tenant_id=_TENANT, run_id="run-a", unit_id="unit-a", dispatch_id=_WAVE_B,
        owner="worker-a", fencing_token=claimed.fencing_token, error_code="stale",
        retryable=True,
    )
    invalid_failures = (
        fail.model_copy(update={"dispatch_id": dispatch.dispatch_id, "owner": "other"}),
        fail.model_copy(update={
            "dispatch_id": dispatch.dispatch_id, "fencing_token": claimed.fencing_token + 1
        }),
        fail,
    )
    for command in invalid_failures:
        action = partial(adapter.fail_run_unit, command, _event("invalid-fail"))
        _assert_unit_rejected(adapter, (FenceRejected, LeaseLost), action)
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    parent_commit = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    parent_fail = fail.model_copy(update={"dispatch_id": dispatch.dispatch_id})
    _assert_unit_rejected(adapter, (FenceRejected, LeaseLost), lambda: adapter.commit_run_unit(
        parent_commit, _event("invalid-parent-commit")
    ))
    _assert_unit_rejected(adapter, (FenceRejected, LeaseLost), lambda: adapter.fail_run_unit(
        parent_fail, _event("invalid-parent-fail")
    ))
    adapter.put_run(_run("run-a"))
    clock.advance(timedelta(seconds=31))
    expired = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    _assert_unit_rejected(
        adapter, LeaseLost, lambda: adapter.commit_run_unit(expired, _event("expired-unit"))
    )
    expired_fail = fail.model_copy(update={"dispatch_id": dispatch.dispatch_id})
    _assert_unit_rejected(
        adapter, LeaseLost,
        lambda: adapter.fail_run_unit(expired_fail, _event("expired-fail")),
    )
    _assert_retryable_unit_failure(adapter, clock, fail)


def _case_degraded_unit(adapter: Any, clock: MutableClock) -> None:
    optional = (RunDependency(source_type="CNES", file_subtype="ST", required=False),)
    adapter.put_run(_run("run-a", dependencies=optional))
    _put_units(adapter, (_unit("unit-a"),))
    dispatch = _reserve(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    assert claimed is not None
    command = FailRunUnit(
        tenant_id=_TENANT, run_id="run-a", unit_id="unit-a",
        dispatch_id=dispatch.dispatch_id, owner="worker-a",
        fencing_token=claimed.fencing_token, error_code="optional_failed", retryable=False,
    )
    event = _event("unit-degraded")
    failed = adapter.fail_run_unit(command, event)
    assert failed.state is RunUnitState.SUCCEEDED_DEGRADED
    assert failed.output_manifests == ()
    assert (failed.lease_owner, failed.lease_until) == (None, None)
    assert adapter.list_run_units(_TENANT, "run-a")[0] == failed
    assert adapter.get_run(_TENANT, "run-a").missing_sources == ("CNES/ST",)
    assert adapter.pending_outbox(100).count(event) == 1


def _case_cancellation(adapter: Any, clock: MutableClock) -> None:
    adapter.put_run(_run("run-a"))
    _put_units(adapter, (_unit("unit-a"), _unit("unit-b"), _unit("unit-c")))
    dispatch = _reserve(adapter, clock, unit_ids=("unit-a", "unit-b"))
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    assert claimed is not None
    leased = _claim_unit_command(dispatch.dispatch_id, "worker-b", clock, "unit-b")
    assert adapter.claim_run_unit(leased) is not None
    _assert_committed_unit(adapter, dispatch, claimed)
    requested_event = _event("run-cancel-requested")
    adapter.transition_run(
        TransitionRun(
            tenant_id=_TENANT, run_id="run-a", expected_state=RunState.PROCESSING,
            new_state=RunState.CANCEL_REQUESTED), requested_event,
    )
    assert adapter.pending_outbox(100).count(requested_event) == 1
    command = FinalizeRunCancellation(
        tenant_id=_TENANT, run_id="run-a", expected_state=RunState.CANCEL_REQUESTED,
        canceled_at=clock.now())
    event = _event("run-canceled")
    result = adapter.finalize_run_cancellation(command, event)
    assert result.state is RunState.CANCELED
    assert adapter.get_run(_TENANT, "run-a") == result
    states = {unit.unit_id: (unit.state, unit.lease_owner, unit.lease_until)
              for unit in adapter.list_run_units(_TENANT, "run-a")}
    assert states == {
        "unit-a": (RunUnitState.SUCCEEDED, None, None),
        "unit-b": (RunUnitState.CANCELED, None, None),
        "unit-c": (RunUnitState.CANCELED, None, None)}
    assert adapter.finalize_run_cancellation(command, event) == result
    assert adapter.pending_outbox(10).count(event) == 1


def _case_dispatch(adapter: Any, clock: MutableClock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    assert _reserve(adapter, clock) == dispatch
    _expect_error(Conflict, lambda: _reserve(adapter, clock, _WAVE_B))
    bind = BindRunDispatch(
        tenant_id=_TENANT, run_id="run-a", dispatch_id=dispatch.dispatch_id,
        execution_ref="exec-a", now=clock.now(), lease_seconds=30)
    clock.advance(timedelta(seconds=10))
    bind = bind.model_copy(update={"now": clock.now()})
    started = adapter.bind_run_dispatch(bind)
    assert started.lease_until == clock.now() + timedelta(seconds=30)
    assert adapter.get_active_run_dispatch(_TENANT, "run-a") == started
    assert adapter.bind_run_dispatch(bind) == started
    _expect_error(Conflict, lambda: adapter.bind_run_dispatch(
        bind.model_copy(update={"execution_ref": "x"})))
    _expect_error(Conflict, lambda: adapter.finish_run_dispatch(FinishRunDispatch(
        tenant_id=_TENANT, run_id="run-a", dispatch_id=_WAVE_B,
        outcome=DispatchOutcome.SUCCEEDED, finished_at=clock.now())))
    finish = FinishRunDispatch(
        tenant_id=_TENANT, run_id="run-a", dispatch_id=dispatch.dispatch_id,
        outcome=DispatchOutcome.SUCCEEDED, finished_at=clock.now())
    finished = adapter.finish_run_dispatch(finish)
    assert finished.terminal_outcome is DispatchOutcome.SUCCEEDED
    assert adapter.get_active_run_dispatch(_TENANT, "run-a") is None
    assert adapter.finish_run_dispatch(finish) == finished
    _expect_error(Conflict, lambda: adapter.finish_run_dispatch(
        finish.model_copy(update={"outcome": DispatchOutcome.FAILED})
    ))
    adapter.put_run(_run("run-b"))
    other_unit = _unit("unit-a").model_copy(update={"run_id": "run-b"})
    _put_units(adapter, (other_unit,), "run-b")
    other_command = ReserveRunDispatch(
        tenant_id=_TENANT, run_id="run-b", wave_id=_WAVE_B, unit_ids=("unit-a",),
        now=clock.now(), lease_seconds=30)
    other = adapter.reserve_run_dispatch(other_command)
    assert other.generation == 1
    assert other.dispatch_id != dispatch.dispatch_id
    assert adapter.reserve_run_dispatch(other_command) == other
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
    adapter.bind_run_dispatch(BindRunDispatch(
        tenant_id=_TENANT, run_id="run-a", dispatch_id=renewed.dispatch_id,
        execution_ref="short-dispatch", now=clock.now(), lease_seconds=1))
    clock.advance(timedelta(seconds=2))
    commit = _commit_command(renewed.dispatch_id, "worker-b", claimed.fencing_token)
    fail = FailRunUnit(
        tenant_id=_TENANT, run_id="run-a", unit_id="unit-a",
        dispatch_id=renewed.dispatch_id, owner="worker-b",
        fencing_token=claimed.fencing_token, error_code="dispatch-expired", retryable=True,
    )
    _assert_unit_rejected(adapter, LeaseLost, lambda: adapter.commit_run_unit(
        commit, _event("expired-dispatch-commit")))
    _assert_unit_rejected(adapter, LeaseLost, lambda: adapter.fail_run_unit(
        fail, _event("expired-dispatch-fail")))
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
        first.model_copy(update={"request_hash": _HASH_B})))
    clock.advance(timedelta(minutes=6))
    replaced = adapter.begin_idempotency(first.model_copy(update={
        "request_hash": _HASH_B, "resource_id": "resource-b", "now": clock.now(),
        "expires_at": clock.now() + timedelta(minutes=5)}))
    assert replaced.created
    assert replaced.record.resource_id == "resource-b"


def _publish(run_id: str, event_id: str, expected: str | None, degraded: bool) -> PublishDataset:
    state = RunState.PUBLISHED_DEGRADED if degraded else RunState.PUBLISHED
    version = DatasetVersion(
        tenant_id=_TENANT, dataset_name="gold", version_id=run_id, run_id=run_id,
        run_manifest_key=f"reconciliation/{_TENANT}/2026-07/{run_id}/run-manifest.json",
        created_at=_NOW)
    return PublishDataset(
        version=version, pointer_name="current", expected_version_id=expected,
        final_state=state,
        missing_sources=("CNES/ST",) if degraded else (),
        publication_permit=PublicationPermit(
            tenant_id=_TENANT, run_id=run_id, policy_version=1, fencing_token=1),
        event=_event(event_id, aggregate_id=run_id))


def _case_publication(adapter: Any, clock: MutableClock) -> None:
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    first = _publish("run-a", "published-a", None, False)
    pointer = adapter.publish_dataset(first)
    assert pointer.version_id == "run-a"
    assert adapter.get_dataset_pointer(_TENANT, "gold") == pointer
    assert adapter.get_run(_TENANT, "run-a").state is RunState.PUBLISHED
    assert adapter.publish_dataset(first) == pointer
    assert adapter.pending_outbox(10).count(first.event) == 1
    adapter.put_run(_run("run-c"))
    invalid_parent = _publish("run-c", "published-c", "run-a", False)
    before = (adapter.get_dataset_pointer(_TENANT, "gold"),
              adapter.get_run(_TENANT, "run-c"), adapter.pending_outbox(100))
    _expect_error(Conflict, lambda: adapter.publish_dataset(invalid_parent))
    after = (adapter.get_dataset_pointer(_TENANT, "gold"),
             adapter.get_run(_TENANT, "run-c"), adapter.pending_outbox(100))
    assert after == before
    assert adapter.get_dataset_version(_TENANT, "gold", "run-c") is None
    changed = first.model_copy(update={"version": first.version.model_copy(update={
        "run_manifest_key": f"reconciliation/{_TENANT}/2026-06/run-a/run-manifest.json"})})
    _expect_error(Conflict, lambda: adapter.publish_dataset(changed))
    assert adapter.get_dataset_version(_TENANT, "gold", "run-a") == first.version
    adapter.put_run(_run("run-b", RunState.PUBLISHING))
    conflict = _publish("run-b", "published-b", "stale", True)
    _expect_error(Conflict, lambda: adapter.publish_dataset(conflict))
    assert adapter.get_dataset_version(_TENANT, "gold", "run-b") is None
    assert adapter.get_run(_TENANT, "run-b").state is RunState.PUBLISHING
    assert conflict.event not in adapter.pending_outbox(10)
    success = conflict.model_copy(update={"expected_version_id": "run-a"})
    degraded_pointer = adapter.publish_dataset(success)
    assert degraded_pointer.version_id == "run-b"
    assert adapter.get_dataset_pointer(_TENANT, "gold") == degraded_pointer
    assert adapter.get_dataset_version(_TENANT, "gold", "run-b") == success.version
    assert adapter.pending_outbox(100).count(success.event) == 1
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
