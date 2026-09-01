from datetime import UTC, datetime, timedelta  # noqa: I001

import pytest

from cnes_domain.control_plane.commands import (
    CancelJob,
    CompleteJob,
    FailRunUnit,
    FinishRunDispatch,
)
from cnes_domain.control_plane.entities import (
    AccessRequest,
    Job,
    RawManifestRecord,
    RunDependency,
    Tenant,
)
from cnes_domain.control_plane.enums import (
    AccessRequestState,
    DispatchOutcome,
    JobState,
    RunUnitState,
)
from cnes_domain.control_plane.errors import Conflict, LeaseLost, NotFound
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.contracts import control_plane_contract
from packages.cnes_infra.tests.contracts.clock import (
    MutableClock,
    _agent,
    _claim_job,
    _claim_unit,
    _commit_command,
    _event,
    _fail_job,
    _job,
    _prepare_unit,
    _raw_record,
    _reserve,
    _run,
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
def test_cumpre_contrato(case, sqlite_control_plane, clock, monkeypatch) -> None:
    if case.name == "raw_chains":
        monkeypatch.setattr(
            control_plane_contract, "_store_record", _store_record_with_matching_mode)
    case.run(sqlite_control_plane, clock)
    if case.name == "cancellation":
        command = control_plane_contract.FinalizeRunCancellation(
            tenant_id="354130", run_id="run-a",
            expected_state=control_plane_contract.RunState.CANCEL_REQUESTED,
            canceled_at=clock.now())
        event = _event("run-canceled")
        reopened = SQLiteControlPlane(sqlite_control_plane._database_path, clock.now)
        reopened.initialize()
        result = reopened.finalize_run_cancellation(command, event)
        before = (result, reopened.pending_outbox(100))
        with pytest.raises(Conflict, match="run_cancellation_conflict"):
            reopened.finalize_run_cancellation(command.model_copy(
                update={"canceled_at": clock.now() + timedelta(seconds=1)}), event)
        with pytest.raises(Conflict, match="run_cancellation_conflict"):
            reopened.finalize_run_cancellation(command, event.model_copy(update={"payload": {}}))
        assert before == (reopened.get_run("354130", "run-a"), reopened.pending_outbox(100))
def _store_record_with_matching_mode(adapter, record, clock) -> None:
    job_id = f"job-{record.agent_id}-{record.snapshot_id}"
    job = _job(job_id, record.agent_id, record.tenant_id).model_copy(update={
        "source_type": record.source_type, "file_subtype": record.file_subtype,
        "competencia": record.competencia, "requested_snapshot_mode": record.snapshot_mode,
    })
    adapter.put_agent(_agent(record.agent_id, tenant_id=record.tenant_id))
    created = _event(f"created-{job_id}", tenant_id=record.tenant_id)
    adapter.create_job(job, created)
    claimed = adapter.claim_job(_claim_job(job_id, "raw-worker", clock, record.tenant_id))
    assert claimed is not None
    command = CompleteJob(
        tenant_id=record.tenant_id, job_id=job_id, owner="raw-worker",
        fencing_token=claimed.fencing_token, manifest=record)
    event = _event(f"completed-{job_id}", tenant_id=record.tenant_id)
    adapter.complete_job(command, event)
def _claim_job_for_record(adapter, record, job_id, clock) -> Job:
    job = _job(job_id, record.agent_id, record.tenant_id).model_copy(update={
        "source_type": record.source_type, "file_subtype": record.file_subtype,
        "competencia": record.competencia, "requested_snapshot_mode": record.snapshot_mode,
    })
    adapter.put_agent(_agent(record.agent_id, tenant_id=record.tenant_id))
    adapter.create_job(job, _event(f"created-{job_id}", tenant_id=record.tenant_id))
    claimed = adapter.claim_job(_claim_job(job_id, "raw-worker", clock, record.tenant_id))
    assert claimed is not None
    return claimed
def test_aceita_argumentos_nomeados_e_limites_padrao(sqlite_control_plane) -> None:
    identity = {"tenant_id": "354130", "source_type": "CNES",
                "file_subtype": "ST", "competencia": "2026-07"}
    assert sqlite_control_plane.latest_succeeded_job(agent_id="agent-a", **identity) is None
    assert sqlite_control_plane.list_raw_manifest_chain(**identity) == ()
    assert sqlite_control_plane.list_waiting_runs_for_dependency(**identity) == ()
    positional = tuple(identity.values())
    assert sqlite_control_plane.list_raw_manifest_chain(*positional) == ()
    assert sqlite_control_plane.list_waiting_runs_for_dependency(*positional) == ()
def test_rejeita_formas_invalidas_das_fronteiras_variadicas(sqlite_control_plane) -> None:
    identity = ("354130", "agent-a", "CNES", "ST", "2026-07")
    with pytest.raises(TypeError, match="too_many_arguments=6"):
        sqlite_control_plane.latest_succeeded_job(*identity, "extra")
    with pytest.raises(TypeError, match="unexpected_argument=extra"):
        sqlite_control_plane.latest_succeeded_job(
            tenant_id="354130", agent_id="agent-a", source_type="CNES",
            file_subtype="ST", competencia="2026-07", extra="value",
        )
    with pytest.raises(TypeError, match="duplicate_argument=tenant_id"):
        sqlite_control_plane.latest_succeeded_job("354130", tenant_id="354130")
    with pytest.raises(TypeError, match="missing_arguments=agent_id"):
        sqlite_control_plane.latest_succeeded_job("354130")
def test_manifesto_repetido_e_divergente(sqlite_control_plane, clock) -> None:
    record = _raw_record("snapshot-a", "agent-a", 1, clock.now())
    first = _claim_job_for_record(sqlite_control_plane, record, "job-first", clock)
    replay = _claim_job_for_record(sqlite_control_plane, record, "job-replay", clock)
    divergent = RawManifestRecord.model_validate(
        record.model_dump() | {"manifest_sha256": "b" * 64})
    stale = _claim_job_for_record(sqlite_control_plane, record, "job-stale", clock)
    sqlite_control_plane.complete_job(
        CompleteJob(
            tenant_id="354130", job_id=first.job_id, owner="raw-worker",
            fencing_token=first.fencing_token, manifest=record),
        _event("completed-first"))
    replayed = sqlite_control_plane.complete_job(
        CompleteJob(
            tenant_id="354130", job_id=replay.job_id, owner="raw-worker",
            fencing_token=replay.fencing_token, manifest=record),
        _event("completed-replay"))
    before_outbox = sqlite_control_plane.pending_outbox(100)
    divergent_command = CompleteJob(
        tenant_id="354130", job_id=stale.job_id, owner="raw-worker",
        fencing_token=stale.fencing_token, manifest=divergent)
    with pytest.raises(Conflict, match="manifest_immutable"):
        sqlite_control_plane.complete_job(divergent_command, _event("completed-stale"))
    assert replayed.state is JobState.SUCCEEDED
    assert sqlite_control_plane.get_job("354130", stale.job_id) == stale
    assert sqlite_control_plane.pending_outbox(100) == before_outbox
    recovered = sqlite_control_plane.complete_job(
        divergent_command.model_copy(update={"manifest": record}), _event("completed-recovered"))
    assert recovered.result_manifest_id == record.manifest_id
@pytest.mark.parametrize(
    ("operation", "field"),
    [
        ("complete_job", "fence"), ("complete_job", "manifest"),
        ("complete_job", "event"), ("fail_job", "fence"),
        ("fail_job", "error"), ("fail_job", "retryable"), ("fail_job", "event"),
    ],
)
def test_replay_terminal_exato_e_divergente(sqlite_control_plane, clock, operation, field) -> None:
    sqlite_control_plane.put_agent(_agent("agent-a"))
    sqlite_control_plane.create_job(_job("job-a"), _event("job-created"))
    claimed = sqlite_control_plane.claim_job(_claim_job("job-a", "worker-a", clock))
    assert claimed is not None
    command = _fail_job("worker-a", claimed.fencing_token, "failed")
    if operation == "complete_job":
        command = CompleteJob(
            tenant_id="354130", job_id="job-a", owner="worker-a",
            fencing_token=claimed.fencing_token,
            manifest=_raw_record("terminal", "agent-a", 1, clock.now()),
        )
    event = _event("job-terminal")
    terminal = getattr(sqlite_control_plane, operation)(command, event)
    reopened = SQLiteControlPlane(sqlite_control_plane._database_path, clock.now)
    reopened.initialize()
    assert getattr(reopened, operation)(command, event) == terminal
    assert reopened.get_job("354130", "job-a") == terminal
    assert reopened.pending_outbox(100).count(event) == 1
    replay_event = event
    replay_command = command
    if field == "event":
        replay_event = event.model_copy(update={"payload": {"changed": True}})
    elif field == "fence":
        replay_command = command.model_copy(update={"fencing_token": 2})
    elif field == "manifest":
        manifest = command.manifest.model_copy(update={"manifest_sha256": "b" * 64})
        replay_command = command.model_copy(update={"manifest": manifest})
    elif field == "error":
        replay_command = command.model_copy(update={"error_code": "changed"})
    else:
        replay_command = command.model_copy(update={"retryable": False})
    before = (reopened.get_job("354130", "job-a"),
              reopened.list_raw_manifest_chain("354130", "CNES", "ST", "2026-07"),
              reopened.pending_outbox(100))
    with pytest.raises(Conflict, match="job_terminal_conflict"):
        getattr(reopened, operation)(replay_command, replay_event)
    assert before == (reopened.get_job("354130", "job-a"),
                      reopened.list_raw_manifest_chain("354130", "CNES", "ST", "2026-07"),
                      reopened.pending_outbox(100))
def test_seleciona_delta_mais_nova(sqlite_control_plane, clock) -> None:
    base = _raw_record("base-agent-a", "agent-a", 1, clock.now())
    sibling_old = _raw_record(
        "delta-a", "agent-a", 2, clock.now() + timedelta(seconds=1)
    ).model_copy(update={"manifest_sha256": "b" * 64})
    sibling_new = _raw_record(
        "delta-b", "agent-a", 2, clock.now() + timedelta(seconds=2)
    ).model_copy(update={"manifest_sha256": "c" * 64})
    sibling_broken = _raw_record(
        "delta-c", "agent-a", 2, clock.now() + timedelta(milliseconds=2500)
    ).model_copy(
        update={"previous_manifest_sha256": "e" * 64, "manifest_sha256": "c" * 64}
    )
    head = _raw_record(
        "delta-d", "agent-a", 3, clock.now() + timedelta(seconds=3)
    ).model_copy(
        update={"previous_manifest_sha256": "c" * 64, "manifest_sha256": "d" * 64}
    )
    for record in (base, sibling_old, sibling_new, sibling_broken, head):
        _store_record_with_matching_mode(sqlite_control_plane, record, clock)
    chain = sqlite_control_plane.list_raw_manifest_chain("354130", "CNES", "ST", "2026-07", 3)
    assert tuple(reference.manifest_id for reference in chain) == (
        base.manifest_id, sibling_new.manifest_id, head.manifest_id)
def test_pagina_heads_com_empate_ate_encontrar_chain_valida(sqlite_control_plane, clock) -> None:
    base = _raw_record("same", "agent-tie", 1, clock.now()).model_copy(
        update={"manifest_id": "manifest-x"})
    broken = _raw_record("same", "agent-tie", 2, clock.now()).model_copy(
        update={"previous_manifest_sha256": "e" * 64})
    with sqlite_control_plane.write_transaction() as connection:
        for manifest_id in ("manifest-z", "manifest-y"):
            sqlite_control_plane.put_manifest_record(
                connection, broken.model_copy(update={"manifest_id": manifest_id}))
        sqlite_control_plane.put_manifest_record(connection, base)
    chain = sqlite_control_plane.list_raw_manifest_chain(
        "354130", "CNES", "ST", "2026-07", 2)
    assert tuple(item.manifest_id for item in chain) == (base.manifest_id,)
def test_persiste_acesso_e_outbox(sqlite_control_plane, clock) -> None:
    tenant = Tenant(
        tenant_id="354130", municipality_name="Presidente Epitácio", created_at=clock.now()
    )
    pending = AccessRequest(
        tenant_id=tenant.tenant_id, request_id="request-a", user_id="user-a",
        state=AccessRequestState.PENDING, decided_by=None, decided_at=None,
    )
    created = _event("access-requested")
    sqlite_control_plane.put_tenant(tenant)
    sqlite_control_plane.put_access_request(pending, created)
    approved = pending.model_copy(update={
        "state": AccessRequestState.APPROVED, "decided_by": "admin-a", "decided_at": clock.now(),
    })
    decided = _event("access-approved")
    assert sqlite_control_plane.decide_access_request(approved, decided) == approved
    assert sqlite_control_plane.get_tenant(tenant.tenant_id) == tenant
    assert sqlite_control_plane.get_access_request(tenant.tenant_id, pending.request_id) == approved
    assert sqlite_control_plane.pending_outbox(10) == (decided, created)
    sqlite_control_plane.mark_outbox_delivered(decided.event_id, clock.now())
    assert sqlite_control_plane.pending_outbox(10) == (created,)
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
    replay = created.model_copy(update={"tenant_id": "other", "payload": {"changed": True},
        "delivered_at": clock.now()})
    with pytest.raises(Conflict, match="access_request_creation_conflict"):
        sqlite_control_plane.put_access_request(pending, replay)
    sqlite_control_plane.put_access_request(pending, created)
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
    reopened = SQLiteControlPlane(sqlite_control_plane._database_path, clock.now)
    reopened.initialize()
    assert reopened.decide_access_request(approved, decided) == approved
    with pytest.raises(Conflict, match="access_request_decision_conflict"):
        reopened.decide_access_request(approved, ignored)
    with pytest.raises(Conflict, match="access_request_state_conflict"):
        sqlite_control_plane.decide_access_request(
            approved.model_copy(update={"state": AccessRequestState.REJECTED}), ignored)
    with pytest.raises(Conflict, match="access_request_state_conflict"):
        sqlite_control_plane.decide_access_request(
            pending.model_copy(update={"request_id": "missing"}), ignored)
    assert sqlite_control_plane.get_access_request("354130", "request-a") == approved
    assert sqlite_control_plane.pending_outbox(100) == (decided, created)
def test_replays_e_conflitos_de_job(sqlite_control_plane, clock) -> None:
    agent = _agent("agent-a")
    sqlite_control_plane.put_agent(agent)
    assert sqlite_control_plane.get_agent("354130", "agent-a") == agent
    assert sqlite_control_plane.get_agent("other", "agent-a") is None
    job = _job("job-a")
    created = _event("job-created")
    assert sqlite_control_plane.create_job(job, created) == job
    reopened = SQLiteControlPlane(sqlite_control_plane._database_path, clock.now)
    reopened.initialize()
    assert reopened.create_job(job, created) == job
    before = reopened.pending_outbox(100)
    with pytest.raises(Conflict, match="job_creation_conflict"):
        reopened.create_job(job, created.model_copy(update={"payload": {}}))
    assert reopened.pending_outbox(100) == before
    with pytest.raises(Conflict, match="job_conflict"):
        sqlite_control_plane.create_job(
            job.model_copy(update={"source_type": "SIHD"}), _event("job-conflict"))
    second = _job("job-b")
    assert sqlite_control_plane.create_job(second, created) == second
    assert sqlite_control_plane.pending_outbox(100) == (created,)
    with pytest.raises(NotFound, match="outbox_event_missing"):
        sqlite_control_plane.mark_outbox_delivered("missing", datetime.now(UTC))
@pytest.mark.parametrize("operation", ["commit_run_unit", "fail_run_unit"])
def test_rejeita_unidade_terminal(sqlite_control_plane, clock, operation) -> None:
    dispatch = _prepare_unit(sqlite_control_plane, clock)
    claimed = _claim_unit(sqlite_control_plane, clock, dispatch.dispatch_id, "worker-a")
    assert claimed is not None
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    if operation == "fail_run_unit":
        command = FailRunUnit(
            tenant_id="354130", run_id="run-a", unit_id="unit-a",
            dispatch_id=dispatch.dispatch_id, owner="worker-a",
            fencing_token=claimed.fencing_token, error_code="late", retryable=True,
        )
    sqlite_control_plane.finish_run_dispatch(
        FinishRunDispatch(
            tenant_id="354130", run_id="run-a", dispatch_id=dispatch.dispatch_id,
            outcome=DispatchOutcome.FAILED, finished_at=clock.now(),
        )
    )
    action = getattr(sqlite_control_plane, operation)
    before = _unit_snapshot(sqlite_control_plane)
    with pytest.raises(LeaseLost, match="dispatch_inactive"):
        action(command, _event("late-result"))
    assert before == _unit_snapshot(sqlite_control_plane)
    clock.advance(timedelta(seconds=31))
    replacement = _reserve(sqlite_control_plane, clock)
    stale = command.model_copy(update={"dispatch_id": replacement.dispatch_id})
    before_stale = _unit_snapshot(sqlite_control_plane)
    with pytest.raises(LeaseLost, match="unit_dispatch_mismatch"):
        action(stale, _event("stale-result"))
    assert before_stale == _unit_snapshot(sqlite_control_plane)
def _unit_snapshot(adapter):
    return adapter.list_run_units("354130", "run-a"), adapter.pending_outbox(100)
@pytest.mark.parametrize(
    ("operation", "retry", "required", "state"),
    [("commit_run_unit", True, True, RunUnitState.SUCCEEDED),
     ("fail_run_unit", True, True, RunUnitState.FAILED_RETRYABLE),
     ("fail_run_unit", False, True, RunUnitState.FAILED_FINAL),
     ("fail_run_unit", False, False, RunUnitState.SUCCEEDED_DEGRADED)],
)
def test_replay_unidade(sqlite_control_plane, clock, operation, retry, required, state) -> None:
    dispatch = _prepare_unit(sqlite_control_plane, clock)
    if not required:
        dependency = (RunDependency(source_type="CNES", file_subtype="ST", required=False),)
        sqlite_control_plane.put_run(_run("run-a", dependencies=dependency))
    claimed = _claim_unit(sqlite_control_plane, clock, dispatch.dispatch_id, "worker-a")
    assert claimed is not None
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    if operation == "fail_run_unit":
        command = FailRunUnit(
            tenant_id="354130", run_id="run-a", unit_id="unit-a",
            dispatch_id=dispatch.dispatch_id, owner="worker-a",
            fencing_token=claimed.fencing_token, error_code="failed", retryable=retry,
        )
    event = _event("unit-terminal")
    terminal = getattr(sqlite_control_plane, operation)(command, event)
    reopened = SQLiteControlPlane(sqlite_control_plane._database_path, clock.now)
    reopened.initialize()
    assert terminal.state is state
    assert getattr(reopened, operation)(command, event) == terminal
    before = _unit_snapshot(reopened)
    with pytest.raises(Conflict, match="unit_terminal_conflict"):
        getattr(reopened, operation)(
            command.model_copy(update={"fencing_token": command.fencing_token + 1}), event)
    with pytest.raises(Conflict, match="unit_terminal_conflict"):
        getattr(reopened, operation)(command, event.model_copy(update={"payload": {}}))
    assert before == _unit_snapshot(reopened)
def test_cancela_job_leased_e_torna_replay_idempotente(sqlite_control_plane, clock) -> None:
    sqlite_control_plane.put_agent(_agent("agent-a"))
    sqlite_control_plane.create_job(_job("job-a"), _event("job-created"))
    leased = sqlite_control_plane.claim_job(_claim_job("job-a", "worker-a", clock))
    command = CancelJob(tenant_id="354130", job_id="job-a", requested_by="user-a")
    event = _event("job-cancel-requested")
    canceled = sqlite_control_plane.cancel_job(command, event)
    assert leased is not None
    assert canceled.state is JobState.CANCEL_REQUESTED
    reopened = SQLiteControlPlane(sqlite_control_plane._database_path, clock.now)
    reopened.initialize()
    assert reopened.cancel_job(command, event) == canceled
    with pytest.raises(Conflict, match="job_cancellation_conflict"):
        reopened.cancel_job(command.model_copy(update={"requested_by": "user-b"}), event)
    with pytest.raises(Conflict, match="job_cancellation_conflict"):
        reopened.cancel_job(command, event.model_copy(update={"payload": {}}))
    assert sqlite_control_plane.pending_outbox(10) == (event, _event("job-created"))
@pytest.mark.parametrize(
    "state",
    [JobState.PENDING, JobState.FAILED_RETRYABLE, JobState.FAILED_FINAL,
     JobState.SUCCEEDED, JobState.CANCELED],
)
def test_rejeita_cancelamento_fora_de_leased(sqlite_control_plane, state) -> None:
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
        "tenant_id": {"tenant_id": "other",
                      "manifest_key": "raw/other/CNES/2026-07/result/manifest.json"},
        "agent_id": {"agent_id": "agent-b"},
        "source_type": {"source_type": "SIHD",
                        "manifest_key": "raw/354130/SIHD/2026-07/result/manifest.json"},
        "file_subtype": {"file_subtype": "PF"},
        "competencia": {"competencia": "2026-06",
                        "manifest_key": "raw/354130/CNES/2026-06/result/manifest.json"},
        "snapshot_mode": {"snapshot_mode": "DELTA", "sequence": 2,
                          "base_snapshot_id": "base-agent-a",
                          "previous_manifest_sha256": "a" * 64},
    }
    return RawManifestRecord.model_validate(base.model_dump() | updates[field])
@pytest.mark.parametrize(
    "field",
    ["tenant_id", "agent_id", "source_type", "file_subtype", "competencia", "snapshot_mode"],
)
def test_rejeita_manifesto_identidade_divergente(sqlite_control_plane, clock, field) -> None:
    sqlite_control_plane.put_agent(_agent("agent-a"))
    original = _job("job-a")
    created = _event("job-created")
    sqlite_control_plane.create_job(original, created)
    claimed = sqlite_control_plane.claim_job(_claim_job("job-a", "worker-a", clock))
    assert claimed is not None
    manifest = _manifesto_com_identidade(_raw_record("result", "agent-a", 1, clock.now()), field)
    command_values = {
        "tenant_id": "354130", "job_id": "job-a", "owner": "worker-a",
        "fencing_token": claimed.fencing_token, "manifest": manifest,
    }
    command = (CompleteJob.model_construct(**command_values) if field == "tenant_id"
               else CompleteJob(**command_values))
    with pytest.raises(Conflict, match="manifest_identity_mismatch"):
        sqlite_control_plane.complete_job(command, _event("job-completed"))
    assert sqlite_control_plane.get_job("354130", "job-a") == claimed
    assert sqlite_control_plane.list_raw_manifest_chain("354130", "CNES", "ST", "2026-07") == ()
    assert sqlite_control_plane.list_raw_manifest_chain("other", "CNES", "ST", "2026-07") == ()
    assert sqlite_control_plane.pending_outbox(10) == (created,)
@pytest.mark.parametrize("operation", ["job", "transition", "unit", "publication", "access"])
@pytest.mark.parametrize("invalid", ["delivered", "tenant"])
def test_evento_invalido_reverte_mutacao(sqlite_control_plane, clock, operation, invalid) -> None:
    states = control_plane_contract.RunState
    sqlite_control_plane.put_run(_run("run-transition"))
    sqlite_control_plane.put_run(_run("run-pub", states.PUBLISHING))
    dispatch = _prepare_unit(sqlite_control_plane, clock)
    claimed = _claim_unit(sqlite_control_plane, clock, dispatch.dispatch_id, "worker-a")
    assert claimed is not None
    event = _event("invalid", tenant_id="other" if invalid == "tenant" else "354130")
    if invalid == "delivered":
        event = event.model_copy(update={"delivered_at": clock.now()})
    transition = control_plane_contract.TransitionRun(
        tenant_id="354130", run_id="run-transition", expected_state=states.PROCESSING,
        new_state=states.PUBLISHING)
    request = AccessRequest(tenant_id="354130", request_id="request-delivered", user_id="user-a",
        state=AccessRequestState.PENDING, decided_by=None, decided_at=None)
    publication = control_plane_contract._publish("run-pub", "published", None, False)
    publication = publication.model_copy(update={"event": event})
    actions = {
        "job": lambda: sqlite_control_plane.create_job(_job("job-invalid"), event),
        "transition": lambda: sqlite_control_plane.transition_run(transition, event),
        "unit": lambda: sqlite_control_plane.commit_run_unit(
            _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token), event),
        "publication": lambda: sqlite_control_plane.publish_dataset(publication),
        "access": lambda: sqlite_control_plane.put_access_request(request, event),
    }
    def snapshot():
        with sqlite_control_plane.read_connection() as connection:
            return tuple(connection.iterdump())
    before = snapshot()
    error = "outbox_event_already_delivered" if invalid == "delivered" else "tenant_mismatch"
    with pytest.raises(Conflict, match=error):
        actions[operation]()
    assert snapshot() == before
