from datetime import timedelta
from typing import Any
from warnings import catch_warnings, simplefilter

from cnes_domain.control_plane.entities import RunDependency
from cnes_domain.control_plane.enums import RunState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.ids import run_dependency_key
from cnes_domain.control_plane.queries import (
    LatestSucceededJobQuery,
    RawIdentity,
    RawManifestChainQuery,
    WaitingRunsForDependencyQuery,
)
from packages.cnes_infra.tests.contracts.clock import (
    _NOW,
    _TENANT,
    MutableClock,
    _claim_job,
    _event,
    _fail_job,
    _job,
    _raw_record,
    _run,
    _store_record,
)

_HASH_B = "b" * 64


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
