from datetime import timedelta
from typing import Any
from warnings import catch_warnings, simplefilter

import pytest

from cnes_domain.control_plane.commands import CompleteJob
from cnes_domain.control_plane.entities import RunDependency
from cnes_domain.control_plane.enums import RunState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.ids import run_dependency_key
from cnes_domain.control_plane.queries import (
    AgentRawManifestChainQuery,
    LatestSucceededJobQuery,
    RawIdentity,
    RawManifestByIdQuery,
    RawManifestChainQuery,
    RawResyncStateQuery,
    WaitingRunsForDependencyQuery,
)
from packages.cnes_infra.tests.contracts.clock import (
    _NOW,
    _TENANT,
    MutableClock,
    _agent,
    _claim_job,
    _event,
    _fail_job,
    _job,
    _raw_record,
    _run,
    _store_record,
)

_HASH_B = "b" * 64


def _accept_record(adapter: Any, record: Any, clock: MutableClock, job_id: str) -> None:
    job = _job(job_id).model_copy(update={
        "requested_snapshot_mode": record.snapshot_mode,
        "created_at": record.created_at,
    })
    adapter.create_job(job, _event(f"{job_id}-created"))
    claim = adapter.claim_job(_claim_job(job_id, "worker", clock))
    adapter.complete_job(
        CompleteJob(
            tenant_id=_TENANT,
            job_id=job_id,
            owner="worker",
            fencing_token=claim.fencing_token,
            manifest=record,
        ),
        _event(f"{job_id}-accepted"),
    )


def _reject_twice(adapter: Any, clock: MutableClock, identity: RawIdentity) -> None:
    adapter.put_agent(_agent("agent-a"))
    rejected = _job("job-rejected")
    adapter.create_job(rejected, _event("rejected-created"))
    claim = adapter.claim_job(_claim_job(rejected.job_id, "worker", clock))
    command = _fail_job("worker", claim.fencing_token, "RAW_RESYNC_BASE_UNKNOWN").model_copy(
        update={
            "job_id": rejected.job_id,
            "retryable": False,
            "rejected_manifest_sha256": _HASH_B,
            "expected_resync_marker": False,
        }
    )
    event = _event("resync-required")
    failed = adapter.fail_job(command, event)
    marker = adapter.query_raw_resync_state(RawResyncStateQuery(identity, "agent-a"))
    assert failed.rejected_manifest_sha256 == _HASH_B
    assert marker is not None
    assert marker.required_since == clock.now()
    assert adapter.pending_outbox(100).count(event) == 1

    repeated = _job("job-repeated")
    adapter.create_job(repeated, _event("repeated-created"))
    repeated_claim = adapter.claim_job(_claim_job(repeated.job_id, "worker", clock))
    repeated_command = command.model_copy(update={
        "job_id": repeated.job_id,
        "fencing_token": repeated_claim.fencing_token,
        "expected_resync_marker": True,
    })
    adapter.fail_job(repeated_command, _event("resync-repeated"))
    assert adapter.query_raw_resync_state(RawResyncStateQuery(identity, "agent-a")) == marker


def _accept_full_and_delta(adapter: Any, clock: MutableClock, identity: RawIdentity) -> None:
    marker = adapter.query_raw_resync_state(RawResyncStateQuery(identity, "agent-a"))
    delta = _raw_record("delta-preserves", "agent-a", 2, clock.now())
    _accept_record(adapter, delta, clock, "job-delta")
    assert adapter.query_raw_resync_state(RawResyncStateQuery(identity, "agent-a")) == marker
    assert adapter.query_agent_raw_manifest_chain(
        AgentRawManifestChainQuery(identity, "agent-a")
    ) == ()

    clock.advance(timedelta(seconds=1))
    full = _raw_record("full-clears", "agent-a", 1, clock.now())
    _accept_record(adapter, full, clock, "job-full")
    assert adapter.query_raw_manifest_by_id(
        RawManifestByIdQuery(_TENANT, full.manifest_id)
    ) == full
    assert adapter.query_raw_manifest_by_id(
        RawManifestByIdQuery("other", full.manifest_id)
    ) is None
    chain = adapter.query_agent_raw_manifest_chain(
        AgentRawManifestChainQuery(identity, "agent-a", 31)
    )
    assert tuple(ref.manifest_id for ref in chain) == (full.manifest_id,)
    assert adapter.query_raw_resync_state(RawResyncStateQuery(identity, "agent-a")) is None

    clock.advance(timedelta(seconds=1))
    linked = _raw_record("linked", "agent-a", 2, clock.now()).model_copy(update={
        "base_snapshot_id": full.snapshot_id,
        "previous_manifest_sha256": full.manifest_sha256,
    })
    _accept_record(adapter, linked, clock, "job-linked")
    assert adapter.query_agent_raw_manifest_chain(
        AgentRawManifestChainQuery(identity, "agent-a", 1)
    ) == ()


def _case_raw_registration_state(adapter: Any, clock: MutableClock) -> None:
    identity = RawIdentity(_TENANT, "CNES", "ST", "2026-07")
    assert adapter.query_agent_raw_manifest_chain(
        AgentRawManifestChainQuery(identity, "agent-a", 0)
    ) == ()
    assert adapter.query_agent_raw_manifest_chain(
        AgentRawManifestChainQuery(identity, "agent-a")
    ) == ()
    _reject_twice(adapter, clock, identity)
    _accept_full_and_delta(adapter, clock, identity)


def _delta_completion(adapter: Any, record: Any, clock: MutableClock, job_id: str) -> CompleteJob:
    job = _job(job_id).model_copy(update={"requested_snapshot_mode": "DELTA"})
    adapter.create_job(job, _event(f"{job_id}-created"))
    claim = adapter.claim_job(_claim_job(job_id, "worker", clock))
    return CompleteJob(
        tenant_id=_TENANT,
        job_id=job_id,
        owner="worker",
        fencing_token=claim.fencing_token,
        manifest=record,
        expected_head_manifest_id="manifest-agent-a-base-agent-a",
    )


def _case_delta_completion_cas(adapter: Any, clock: MutableClock) -> None:
    adapter.put_agent(_agent("agent-a"))
    base = _raw_record("base-agent-a", "agent-a", 1, clock.now())
    _accept_record(adapter, base, clock, "job-z-base")
    deltas = tuple(
        _raw_record(f"delta-{suffix}", "agent-a", 2, clock.now()).model_copy(
            update={"previous_manifest_sha256": base.manifest_sha256}
        )
        for suffix in ("a", "b")
    )
    commands = tuple(
        _delta_completion(adapter, item, clock, f"job-{suffix}-delta")
        for item, suffix in zip(deltas, ("a", "b"), strict=True)
    )
    adapter.complete_job(commands[0], _event("delta-a-accepted"))
    identity = RawIdentity(_TENANT, "CNES", "ST", "2026-07")
    assert adapter.query_latest_succeeded_job(
        LatestSucceededJobQuery(identity, "agent-a")
    ).result_manifest_id == deltas[0].manifest_id
    with pytest.raises(Conflict):
        adapter.complete_job(commands[1], _event("delta-b-accepted"))
    assert adapter.query_raw_manifest_by_id(
        RawManifestByIdQuery(_TENANT, deltas[1].manifest_id)
    ) is None


def _case_delta_completion_rejects_marker(adapter: Any, clock: MutableClock) -> None:
    adapter.put_agent(_agent("agent-a"))
    base = _raw_record("base-agent-a", "agent-a", 1, clock.now())
    _accept_record(adapter, base, clock, "job-base")
    delta = _raw_record("delta-marker", "agent-a", 2, clock.now()).model_copy(
        update={"previous_manifest_sha256": base.manifest_sha256}
    )
    completion = _delta_completion(adapter, delta, clock, "job-delta-marker")
    marker_job = _job("job-marker")
    adapter.create_job(marker_job, _event("marker-created"))
    marker_claim = adapter.claim_job(_claim_job("job-marker", "marker-worker", clock))
    failure = _fail_job(
        "marker-worker", marker_claim.fencing_token, "RAW_RESYNC_BASE_UNKNOWN"
    ).model_copy(update={
        "job_id": "job-marker", "retryable": False,
        "rejected_manifest_sha256": _HASH_B,
    })
    adapter.fail_job(failure, _event("marker-required"))
    with pytest.raises(Conflict):
        adapter.complete_job(completion, _event("delta-marker-accepted"))


def _case_raw_chains(adapter: Any, clock: MutableClock) -> None:
    records = (
        _raw_record("base-agent-a", "agent-a", 1, _NOW),
        _raw_record("delta-2", "agent-a", 2, _NOW + timedelta(seconds=1)),
        _raw_record("delta-3", "agent-a", 3, _NOW + timedelta(seconds=2)),
        _raw_record("wrong-base", "agent-a", 4, _NOW + timedelta(seconds=3)).model_copy(
            update={"base_snapshot_id": "base-agent-b"}),
        _raw_record("base-agent-b", "agent-b", 1, _NOW),
        _raw_record("delta-z", "agent-b", 2, _NOW + timedelta(seconds=2)),
        _raw_record("orphan", "agent-z", 2, _NOW + timedelta(seconds=3)),
        _raw_record("base-agent-y", "agent-y", 1, _NOW - timedelta(seconds=1)),
        _raw_record("broken", "agent-y", 2, _NOW + timedelta(seconds=5)).model_copy(
            update={"previous_manifest_sha256": _HASH_B}),)
    for record in records:
        _store_record(adapter, record, clock)
    identities = ({"tenant_id": "other"}, {"source_type": "SIHD"}, {"file_subtype": "PF"},
                  {"competencia": "2026-06"})
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
    failed = _job("job-agent-b-failed", "agent-b").model_copy(
        update={"created_at": _NOW + timedelta(minutes=10)})
    adapter.create_job(failed, _event("failed-created"))
    failed_claim = adapter.claim_job(_claim_job(failed.job_id, "failed-worker", clock))
    failed_command = _fail_job("failed-worker", failed_claim.fencing_token, "failed").model_copy(
        update={"job_id": failed.job_id, "retryable": False})
    adapter.fail_job(failed_command, _event("failed-final"))
    identity = RawIdentity(_TENANT, "CNES", "ST", "2026-07")
    latest = adapter.query_latest_succeeded_job(LatestSucceededJobQuery(identity, "agent-b"))
    assert latest == adapter.get_job(
        _TENANT, "job-agent-b-delta-z")
    chain = adapter.query_raw_manifest_chain(RawManifestChainQuery(identity, 2))
    assert tuple((ref.manifest_id, ref.manifest_key) for ref in chain) == tuple(
        (record.manifest_id, record.manifest_key) for record in records[4:6])
    try:
        short = adapter.query_raw_manifest_chain(RawManifestChainQuery(identity, 1))
    except Conflict:
        pass
    else:
        assert short == ()
def _case_run_discovery(adapter: Any, clock: MutableClock) -> None:
    deps = (
        RunDependency(source_type="CNES", file_subtype="ST", required=True),
        RunDependency(source_type="CNES_ST", file_subtype="X", required=True),)
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
    identity = RawIdentity(_TENANT, "CNES", "ST", "2026-07")
    waiting = adapter.query_waiting_runs_for_dependency(WaitingRunsForDependencyQuery(identity, 10))
    assert tuple(run.run_id for run in waiting) == ("waiting-a", "waiting-b")
    limited = adapter.query_waiting_runs_for_dependency(WaitingRunsForDependencyQuery(identity, 1))
    assert tuple(run.run_id for run in limited) == ("waiting-a",)
    assert run_dependency_key(_TENANT, "CNES", "ST", "2026-07") != run_dependency_key(
        _TENANT, "CNES_ST", "X", "2026-07")
    recoverable = adapter.list_recoverable_runs(clock.now(), 6)
    assert tuple(run.run_id for run in recoverable) == (
        "canceling", "collision", "processing", "publishing", "waiting-a", "waiting-b")
    assert tuple(run.run_id for run in adapter.list_recoverable_runs(clock.now(), 2)) == (
        "canceling", "collision")

def _case_legacy_shims(adapter: Any, clock: MutableClock) -> None:
    record = _raw_record("legacy-base", "agent-a", 1, clock.now())
    _store_record(adapter, record, clock)
    adapter.put_run(_run("legacy-waiting", RunState.WAITING_INPUTS))
    identity = {"tenant_id": _TENANT, "source_type": "CNES",
                "file_subtype": "ST", "competencia": "2026-07"}
    cases = (
        ("latest_succeeded_job", "query_latest_succeeded_job", {"agent_id": "agent-a", **identity}),
        ("list_raw_manifest_chain", "query_raw_manifest_chain", identity),
        ("list_waiting_runs_for_dependency", "query_waiting_runs_for_dependency", identity),
    )
    for legacy, typed, kwargs in cases:
        with catch_warnings(record=True) as warnings:
            simplefilter("always")
            result = getattr(adapter, legacy)(**kwargs)
        assert len(warnings) == 1
        assert warnings[0].category is DeprecationWarning
        assert str(warnings[0].message) == f"method={legacy} replacement={typed}"
        if legacy == "latest_succeeded_job":
            assert result.job_id == "job-agent-a-legacy-base"
        elif legacy == "list_raw_manifest_chain":
            assert tuple(ref.manifest_id for ref in result) == (record.manifest_id,)
        else:
            assert tuple(run.run_id for run in result) == ("legacy-waiting",)
