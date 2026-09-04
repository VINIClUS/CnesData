import sqlite3  # noqa: I001
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from typing import Any
import pytest

from cnes_domain.control_plane.commands import (
    BindRunDispatch,
    CancelJob,
    ClaimJob,
    FailRunUnit,
    FinishRunDispatch,
    PutRunUnits,
    RenewJobLease,
    TransitionRun,
)
from cnes_domain.control_plane.entities import DatasetPointer, RunDependency, Tenant
from cnes_domain.control_plane.enums import DispatchOutcome, RunState
from cnes_domain.control_plane.errors import Conflict, LeaseLost
from cnes_infra.control_plane import sqlite_adapter, sqlite_publication, sqlite_schema
from cnes_infra.control_plane.sqlite_adapter import (
    SQLiteControlPlane,
    _is_network_filesystem,
    _SQLiteBusyError,
    _SQLiteFilesystemError,
)
from packages.cnes_infra.tests.contracts.clock import (
    MutableClock,
    _agent,
    _claim_job,
    _claim_unit_command,
    _commit_command,
    _event,
    _job,
    _prepare_unit,
    _put_units,
    _raw_record,
    _reserve,
    _run,
    _store_record,
    _unit,
)
from packages.cnes_infra.tests.contracts.control_plane_contract import _publish
@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(datetime(2026, 7, 15, 12, tzinfo=UTC))
@pytest.fixture
def database_path(tmp_path):
    return tmp_path / "control-plane.sqlite3"
@pytest.fixture
def adapter(database_path, clock) -> SQLiteControlPlane:
    control_plane = SQLiteControlPlane(database_path, clock.now)
    control_plane.initialize()
    return control_plane
def _capture(action: Any, barrier: Barrier) -> Any:
    barrier.wait()
    try:
        return action()
    except Exception as error:
        return error
def _no(error, match, action) -> None:
    with pytest.raises(error, match=match):
        action()
def _write_dispatch(adapter, dispatch) -> None:
    with adapter.write_transaction() as connection:
        connection.execute("UPDATE run_dispatches SET lease_until = ?, data = ? WHERE run_id = ?",
            (dispatch.lease_until.isoformat(), dispatch.model_dump_json(), dispatch.run_id))
def _race(first: Any, second: Any) -> tuple[Any, Any]:
    barrier = Barrier(3)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (executor.submit(_capture, first, barrier),
                   executor.submit(_capture, second, barrier))
        barrier.wait()
        return tuple(future.result() for future in futures)
def _prepare_job(adapter: SQLiteControlPlane) -> None:
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
def test_reverte_transicao_e_outbox_quando_evento_conflita(adapter, clock) -> None:
    adapter.put_run(_run("run-a"))
    adapter.put_agent(_agent("agent-a"))
    existing = _event("event-collision")
    adapter.create_job(_job("job-a"), existing)
    transition = TransitionRun(
        tenant_id="354130", run_id="run-a", expected_state=RunState.PROCESSING,
        new_state=RunState.PUBLISHING)
    _no(Conflict, "run_state_conflict", lambda: adapter.transition_run(transition.model_copy(
        update={"expected_state": RunState.WAITING_INPUTS}), _event("invalid-transition")))
    _no(Conflict, "outbox_event_conflict", lambda: adapter.transition_run(transition, existing))
    assert adapter.pending_outbox(10) == (existing,)
    event = _event("run-transitioned")
    adapter.transition_run(transition, event)
    retry = SQLiteControlPlane(adapter._database_path, clock.now)
    retry.initialize()
    later = transition.model_copy(update={
        "expected_state": RunState.PUBLISHING, "new_state": RunState.PUBLISHED})
    current = retry.transition_run(later, _event("run-published"))
    assert retry.transition_run(transition, event) == current
    _no(Conflict, "run_transition_conflict", lambda: retry.transition_run(
        transition.model_copy(update={"missing_sources": ("CNES/ST",)}), event))
    _no(Conflict, "run_transition_conflict", lambda: retry.transition_run(
        transition, event.model_copy(update={"payload": {}})))
def test_serializa_escritores_concorrentes(adapter, database_path, clock) -> None:
    _prepare_job(adapter)
    writers = (SQLiteControlPlane(database_path, clock.now),
               SQLiteControlPlane(database_path, clock.now))
    claims = _race(lambda: writers[0].claim_job(_claim_job("job-a", "worker-a", clock)),
                   lambda: writers[1].claim_job(_claim_job("job-a", "worker-b", clock)))
    winners = [result for result in claims if result is not None]
    assert len(winners) == 1
    claimed = winners[0]
    renew = RenewJobLease(
        tenant_id="354130", job_id="job-a", owner=claimed.lease_owner,
        fencing_token=claimed.fencing_token, now=clock.now(), lease_seconds=60)
    renewals = _race(lambda: writers[0].renew_job_lease(renew),
                     lambda: writers[1].renew_job_lease(renew))
    assert renewals[0] == renewals[1]
    adapter.put_run(_run("run-pub-a", RunState.PUBLISHING))
    adapter.put_run(_run("run-pub-b", RunState.PUBLISHING))
    publications = _race(
        lambda: writers[0].publish_dataset(_publish("run-pub-a", "pub-a", None, False)),
        lambda: writers[1].publish_dataset(_publish("run-pub-b", "pub-b", None, False)))
    assert sum(isinstance(result, DatasetPointer) for result in publications) == 1
    assert sum(isinstance(result, Conflict) for result in publications) == 1
def test_serializa_renovacao_contra_reclaim_concorrente(adapter, database_path, clock) -> None:
    _prepare_job(adapter)
    claimed = adapter.claim_job(_claim_job("job-a", "worker-a", clock))
    writers = (SQLiteControlPlane(database_path, clock.now),
               SQLiteControlPlane(database_path, clock.now))
    renew = RenewJobLease(
        tenant_id="354130", job_id="job-a", owner="worker-a",
        fencing_token=claimed.fencing_token, now=clock.now(), lease_seconds=60)
    reclaim = ClaimJob(
        tenant_id="354130", job_id="job-a", owner="worker-b",
        now=clock.now() + timedelta(seconds=31), lease_seconds=30)
    renewed, reclaimed = _race(lambda: writers[0].renew_job_lease(renew),
                               lambda: writers[1].claim_job(reclaim))
    if isinstance(renewed, LeaseLost):
        assert reclaimed.fencing_token == 2
        assert adapter.get_job("354130", "job-a") == reclaimed
    else:
        assert reclaimed is None
        assert renewed.fencing_token == 1
        assert adapter.get_job("354130", "job-a") == renewed
def test_incrementa_fence_apos_expiracao_ou_lease_ausente(adapter, clock) -> None:
    _prepare_job(adapter)
    first = adapter.claim_job(_claim_job("job-a", "worker-a", clock))
    clock.advance(timedelta(seconds=31))
    second = adapter.claim_job(_claim_job("job-a", "worker-b", clock))
    assert (first.fencing_token, second.fencing_token) == (1, 2)
    missing_lease = _job("job-b").model_copy(update={"state": first.state})
    adapter.create_job(missing_lease, _event("job-b-created"))
    assert missing_lease in adapter.list_claimable_jobs("354130", "agent-a", 10)
    reclaimed = adapter.claim_job(_claim_job("job-b", "worker-c", clock))
    assert (reclaimed.attempt, reclaimed.fencing_token) == (1, 1)
def test_configura_busy_timeout_de_cinco_segundos(adapter) -> None:
    with adapter.read_connection() as connection:
        assert connection.execute("PRAGMA busy_timeout").fetchone() == (5000,)
def test_converte_busy_em_erro_local(adapter, database_path, clock, monkeypatch) -> None:
    monkeypatch.setattr(sqlite_adapter, "_BUSY_TIMEOUT_MS", 1)
    blocker = sqlite3.connect(database_path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    tenant = Tenant(tenant_id="354130", municipality_name="Epitácio", created_at=clock.now())
    try:
        _no(_SQLiteBusyError, "sqlite_busy", lambda: adapter.put_tenant(tenant))
    finally:
        blocker.rollback()
        blocker.close()
def test_traduz_falha_de_conexao_em_erro_local(tmp_path, clock, monkeypatch) -> None:
    broken = SQLiteControlPlane(tmp_path / "broken.sqlite3", clock.now)
    def fail_connect():
        raise sqlite3.OperationalError("disk_unavailable")
    monkeypatch.setattr(broken, "_connect", fail_connect)
    _no(_SQLiteFilesystemError, "sqlite_filesystem", broken.initialize)
    _no(_SQLiteFilesystemError, "sqlite_filesystem", lambda: broken.get_tenant("354130"))
def test_propaga_erro_operacional_que_nao_e_contencao(tmp_path, clock) -> None:
    uninitialized = SQLiteControlPlane(tmp_path / "empty.sqlite3", clock.now)
    tenant = Tenant(tenant_id="354130", municipality_name="Epitácio", created_at=clock.now())
    _no(sqlite3.OperationalError, "no such table", lambda: uninitialized.put_tenant(tenant))
def test_rejeita_job_nao_leased_cancelamento_ausente_e_estado_de_run(adapter, clock) -> None:
    _prepare_job(adapter)
    renew = RenewJobLease(
        tenant_id="354130", job_id="job-a", owner="worker-a", fencing_token=0,
        now=clock.now(), lease_seconds=30)
    _no(LeaseLost, "job_not_leased", lambda: adapter.renew_job_lease(renew))
    with pytest.raises(Conflict, match="job_missing"):
        adapter.cancel_job(CancelJob(
            tenant_id="354130", job_id="missing", requested_by="user-a"), _event("cancel-missing"))
    adapter.put_run(_run("run-a", RunState.WAITING_INPUTS))
    command = PutRunUnits(
        tenant_id="354130", run_id="run-a", expected_run_state=RunState.PROCESSING,
        units=(_unit("unit-a"),))
    _no(Conflict, "run_state_conflict", lambda: adapter.put_run_units(command))
def test_reabertura_canonicaliza_unidades(adapter, database_path, clock) -> None:
    _prepare_job(adapter)
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    publication = _publish("run-a", "published", None, False)
    publication = publication.model_copy(update={"publication_permit":
        publication.publication_permit.model_copy(update={"binding_context": object()})})
    pointer = adapter.publish_dataset(publication)
    adapter.put_run(_run("run-units"))
    unit_a = _unit("unit-a").model_copy(update={"run_id": "run-units"})
    unit_b = _unit("unit-b").model_copy(update={"run_id": "run-units"})
    canonical = (unit_a, unit_b)
    assert _put_units(adapter, (unit_b, unit_a), "run-units") == canonical
    adapter.put_run(_run("run-units", RunState.PUBLISHING))
    with adapter.write_transaction() as connection:
        connection.executescript("ALTER TABLE job_creation_writes DROP COLUMN job_data;"
                                 "ALTER TABLE runs DROP COLUMN unit_registry_data;")
    reopened = SQLiteControlPlane(database_path, clock.now)
    reopened.initialize()
    assert reopened.create_job(_job("job-a"), _event("job-created")) == _job("job-a")
    reopened.initialize()
    permit = publication.publication_permit.model_copy(update={"binding_context": object()})
    assert reopened.publish_dataset(publication.model_copy(
        update={"publication_permit": permit})) == pointer
    assert _put_units(reopened, canonical, "run-units") == canonical
    before = reopened.list_run_units("354130", "run-units")
    divergent = (unit_a, _unit("unit-c").model_copy(update={"run_id": "run-units"}))
    _no(Conflict, "units_conflict", lambda: _put_units(reopened, divergent, "run-units"))
    assert reopened.list_run_units("354130", "run-units") == before
def test_rejeita_banco_em_filesystem_de_rede(tmp_path, clock, monkeypatch) -> None:
    monkeypatch.setattr(sqlite_adapter, "_is_network_filesystem", lambda path: True)
    adapter = SQLiteControlPlane(tmp_path / "network" / "control.sqlite3", clock.now)
    _no(_SQLiteFilesystemError, "sqlite_network_filesystem", adapter.initialize)
@pytest.mark.parametrize(
    "network_path",
    [pytest.param(r"\\server\share\control.sqlite3"),
     pytest.param("smb://server/share/control.sqlite3"),
     pytest.param("nfs://server/share/control.sqlite3"),
     pytest.param("/net/server/share/control.sqlite3"),
     pytest.param("/Network/Servers/server/share/control.sqlite3")],)
def test_detecta_filesystem_de_rede(network_path, tmp_path, monkeypatch) -> None:
    def unavailable(*args, **kwargs):
        raise OSError("proc_unavailable")
    monkeypatch.setattr(sqlite_schema.Path, "read_text", unavailable)
    assert _is_network_filesystem(sqlite_adapter.Path(network_path))
    assert not _is_network_filesystem(tmp_path / "control.sqlite3")
def test_rejeita_inicializacao_quando_wal_nao_e_ativado(tmp_path, clock, monkeypatch) -> None:
    adapter = SQLiteControlPlane(tmp_path / "control.sqlite3", clock.now)
    monkeypatch.setattr(adapter, "_connect", lambda: sqlite3.connect(":memory:"))
    _no(_SQLiteFilesystemError, "sqlite_wal_unavailable", adapter.initialize)
def test_deduplica_fonte_ausente_em_unidades_degradadas(adapter, clock) -> None:
    dependency = (RunDependency(source_type="CNES", file_subtype="ST", required=False),)
    adapter.put_run(_run("run-a", dependencies=dependency))
    _put_units(adapter, (_unit("unit-a"), _unit("unit-b")))
    dispatch = _reserve(adapter, clock, unit_ids=("unit-a", "unit-b"))
    for unit_id in ("unit-a", "unit-b"):
        claimed = adapter.claim_run_unit(
            _claim_unit_command(dispatch.dispatch_id, f"worker-{unit_id}", clock, unit_id))
        adapter.fail_run_unit(
            FailRunUnit(
                tenant_id="354130", run_id="run-a", unit_id=unit_id,
                dispatch_id=dispatch.dispatch_id, owner=f"worker-{unit_id}",
                fencing_token=claimed.fencing_token,
                error_code="optional_failed", retryable=False),
            _event(f"degraded-{unit_id}"))
    assert adapter.get_run("354130", "run-a").missing_sources == ("CNES/ST",)
def test_replay_dispatch_e_recaptura(adapter, database_path, clock) -> None:
    dispatch = _prepare_unit(adapter, clock, ("unit-a", "unit-b"))
    reopened = SQLiteControlPlane(database_path, clock.now)
    assert _reserve(reopened, clock, unit_ids=("unit-a", "unit-b")) == dispatch
    before = reopened.get_active_run_dispatch("354130", "run-a")
    _no(Conflict, "dispatch_units_conflict", lambda: _reserve(
        reopened, clock, unit_ids=("unit-a",)))
    assert reopened.get_active_run_dispatch("354130", "run-a") == before
    claimed = adapter.claim_run_unit(_claim_unit_command(dispatch.dispatch_id, "worker-a", clock))
    _put_units(adapter, (_unit("unit-a"), _unit("unit-b")))
    bind = BindRunDispatch(
        tenant_id="354130", run_id="run-a", dispatch_id="b" * 16, execution_ref="exec-a",
        now=clock.now(), lease_seconds=30)
    _no(Conflict, "dispatch_stale", lambda: adapter.bind_run_dispatch(bind))
    bind = bind.model_copy(update={"dispatch_id": dispatch.dispatch_id})
    _no(Conflict, "dispatch_expired", lambda: adapter.bind_run_dispatch(
        bind.model_copy(update={"now": dispatch.lease_until})))
    started = adapter.bind_run_dispatch(bind)
    expired = started.model_copy(update={"lease_until": bind.now})
    _write_dispatch(adapter, expired)
    assert reopened.bind_run_dispatch(bind) == expired
    for update in ({"now": bind.now + timedelta(seconds=1)}, {"lease_seconds": 31},
                   {"execution_ref": "exec-b"}):
        _no(Conflict, "dispatch_bind_conflict", lambda update=update: reopened.bind_run_dispatch(
            bind.model_copy(update=update)))
    _write_dispatch(adapter, started)
    finish = FinishRunDispatch(
        tenant_id="354130", run_id="run-a", dispatch_id=dispatch.dispatch_id,
        outcome=DispatchOutcome.FAILED, finished_at=clock.now())
    _no(Conflict, "dispatch_expired", lambda: adapter.finish_run_dispatch(
        finish.model_copy(update={"finished_at": dispatch.lease_until})))
    finished = adapter.finish_run_dispatch(finish)
    _no(Conflict, "dispatch_terminal", lambda: adapter.bind_run_dispatch(bind))
    expired = finished.model_copy(update={"lease_until": finish.finished_at})
    _write_dispatch(reopened, expired)
    assert reopened.finish_run_dispatch(finish) == expired
    _no(Conflict, "dispatch_finish_conflict", lambda: reopened.finish_run_dispatch(
        finish.model_copy(update={"finished_at": clock.now() + timedelta(microseconds=1)})))
    _no(Conflict, "dispatch_units_conflict", lambda: _reserve(
        adapter, clock, unit_ids=("unit-a",)))
    _no(Conflict, "dispatch_live", lambda: _reserve(adapter, clock, unit_ids=dispatch.unit_ids))
    clock.advance(timedelta(seconds=31))
    replacement = _reserve(adapter, clock, unit_ids=dispatch.unit_ids)
    assert (replacement.generation, replacement.dispatch_id != dispatch.dispatch_id) == (2, True)
    reclaimed = adapter.claim_run_unit(
        _claim_unit_command(replacement.dispatch_id, "worker-b", clock))
    assert (reclaimed.attempt, reclaimed.fencing_token, reclaimed.dispatch_id) == (
        claimed.attempt + 1, claimed.fencing_token + 1, replacement.dispatch_id)
    other = _claim_unit_command(replacement.dispatch_id, "worker-c", clock)
    assert adapter.claim_run_unit(other) is None
    clock.advance(timedelta(seconds=31))
    _no(Conflict, "dispatch_units_conflict", lambda: _reserve(
        adapter, clock, unit_ids=("unit-a",)))
    third = _reserve(adapter, clock, unit_ids=dispatch.unit_ids)
    adapter.finish_run_dispatch(FinishRunDispatch(
        tenant_id="354130", run_id="run-a", dispatch_id=third.dispatch_id,
        outcome=DispatchOutcome.FAILED, finished_at=clock.now()))
    wave_b = _reserve(adapter, clock, "b" * 16, dispatch.unit_ids)
    adapter.finish_run_dispatch(FinishRunDispatch(
        tenant_id="354130", run_id="run-a", dispatch_id=wave_b.dispatch_id,
        outcome=DispatchOutcome.FAILED, finished_at=clock.now()))
    reopened = SQLiteControlPlane(database_path, clock.now)
    reopened.initialize()
    _no(Conflict, "dispatch_units_conflict", lambda: _reserve(
        reopened, clock, "a" * 16, ("unit-a",)))
    assert _reserve(reopened, clock, "a" * 16, dispatch.unit_ids).generation == 5
@pytest.mark.parametrize(
    "state", [None, RunState.PUBLISHING, RunState.CANCEL_REQUESTED, RunState.PUBLISHED])
def test_rejeita_dispatch_sem_run_processing(adapter, clock, state) -> None:
    if state is not None:
        adapter.put_run(_run("run-a", state))
    _no(Conflict, "parent_not_processing", lambda: _reserve(adapter, clock))
    adapter.put_run(_run("run-a"))
    _put_units(adapter, (_unit("unit-b"),))
    _no(Conflict, "dispatch_unit_missing", lambda: _reserve(adapter, clock, unit_ids=("missing",)))
    dispatch = _reserve(adapter, clock, unit_ids=("unit-b",))
    if state is None:
        with adapter.write_transaction() as connection:
            connection.execute("DELETE FROM runs WHERE tenant_id = ? AND run_id = ?",
                               ("354130", "run-a"))
    else:
        adapter.put_run(_run("run-a", state))
    bind = BindRunDispatch(
        tenant_id="354130", run_id="run-a", dispatch_id=dispatch.dispatch_id,
        execution_ref="exec-delayed", now=clock.now(), lease_seconds=30)
    _no(Conflict, "parent_not_processing", lambda: adapter.bind_run_dispatch(bind))
    assert adapter.get_active_run_dispatch("354130", "run-a") is None
def test_rejeita_commit_de_unidade_nao_leased_ou_expirada(adapter, clock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    pending = _commit_command(dispatch.dispatch_id, "worker-a", 0)
    _no(LeaseLost, "unit_not_leased", lambda: adapter.commit_run_unit(
        pending, _event("pending-commit")))
    claim = _claim_unit_command(dispatch.dispatch_id, "worker-a", clock).model_copy(
        update={"lease_seconds": 10})
    claimed = adapter.claim_run_unit(claim)
    clock.advance(timedelta(seconds=11))
    expired = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    _no(LeaseLost, "lease_expired", lambda: adapter.commit_run_unit(
        expired, _event("expired-commit")))
@pytest.mark.parametrize("field", ["final_state", "missing_sources", "permit", "event"])
def test_rejeita_dataset_e_replay_divergentes_apos_publicacao(adapter, field) -> None:
    run = _run("run-a", RunState.PUBLISHING)
    adapter.put_run(run)
    first = _publish("run-a", "published-a", None, True)
    invalid_pointer = first.model_copy(update={"pointer_name": "CURRENT"})
    _no(Conflict, "pointer_name_not_current", lambda: adapter.publish_dataset(invalid_pointer))
    assert adapter.get_run("354130", "run-a") == run
    assert adapter.get_dataset_pointer("354130", "gold") is None
    assert adapter.get_dataset_version("354130", "gold", "run-a") is None
    assert invalid_pointer.event not in adapter.pending_outbox(100)
    mismatched = first.model_copy(
        update={"version": first.version.model_copy(update={"dataset_name": "silver"})})
    _no(Conflict, "run_dataset_mismatch", lambda: adapter.publish_dataset(mismatched))
    assert adapter.get_run("354130", "run-a") == run
    assert adapter.get_dataset_pointer("354130", "silver") is None
    assert adapter.get_dataset_version("354130", "silver", "run-a") is None
    assert mismatched.event not in adapter.pending_outbox(100)
    pointer = adapter.publish_dataset(first)
    permit = first.publication_permit.model_copy(update={"policy_version": 2})
    event = first.event.model_copy(update={"payload": {"changed": True}})
    updates = {
        "final_state": {"final_state": RunState.PUBLISHED, "missing_sources": ()},
        "missing_sources": {"missing_sources": ("SIHD/ER",)},
        "permit": {"publication_permit": permit},
        "event": {"event": event},}
    divergent = first.model_copy(update=updates[field])
    before = (
        adapter.get_run("354130", "run-a"), pointer,
        adapter.get_dataset_version("354130", "gold", "run-a"),
        adapter.pending_outbox(100))
    _no(Conflict, "publication_replay_conflict", lambda: adapter.publish_dataset(divergent))
    assert before == (
        adapter.get_run("354130", "run-a"),
        adapter.get_dataset_pointer("354130", "gold"),
        adapter.get_dataset_version("354130", "gold", "run-a"),
        adapter.pending_outbox(100))
    assert adapter.publish_dataset(first) == pointer
    adapter.put_run(_run("run-b", RunState.PUBLISHING))
    adapter.publish_dataset(_publish("run-b", "published-b", "run-a", False))
    _no(Conflict, "pointer_cas", lambda: adapter.publish_dataset(first))
def test_ordena_e_limita_todos_os_metodos_de_listagem(adapter, clock, monkeypatch) -> None:
    adapter.put_agent(_agent("agent-order"))
    for job_id in ("job-b", "job-a"):
        adapter.create_job(_job(job_id, "agent-order"), _event(f"created-{job_id}"))
    with adapter.write_transaction() as connection:
        connection.execute("UPDATE jobs SET state = ?, data = ? WHERE job_id = ?",
                           ("SUCCEEDED", "{", "job-b"))
    deserialize = sqlite_adapter.deserialize_model
    calls = []
    monkeypatch.setattr(sqlite_adapter, "deserialize_model",
                        lambda *args: (calls.append(None), deserialize(*args))[1])
    adapter.put_run(_run("run-b", RunState.WAITING_INPUTS))
    adapter.put_run(_run("run-a", RunState.WAITING_INPUTS))
    adapter.put_run(_run("run-units"))
    units = tuple(
        _unit(unit_id).model_copy(update={"run_id": "run-units"})
        for unit_id in ("unit-a", "unit-b"))
    _put_units(adapter, units, "run-units")
    base = _raw_record("base-agent-raw", "agent-raw", 1, clock.now())
    _store_record(adapter, base, clock)
    calls.clear()
    jobs = adapter.list_claimable_jobs("354130", "agent-order", 1)
    assert tuple(job.job_id for job in jobs) == ("job-a",)
    assert len(calls) <= 2
    runs = adapter.list_waiting_runs_for_dependency("354130", "CNES", "ST", "2026-07", 1)
    assert tuple(run.run_id for run in runs) == ("run-a",)
    assert adapter.list_recoverable_runs(clock.now(), 1)[0].run_id == "run-a"
    run_units = adapter.list_run_units("354130", "run-units")
    assert tuple(unit.unit_id for unit in run_units) == ("unit-a", "unit-b")
    assert adapter.list_raw_manifest_chain("354130", "CNES", "ST", "2026-07", 0) == ()
    assert len(adapter.list_raw_manifest_chain("354130", "CNES", "ST", "2026-07", 1)) == 1
    pending = adapter.pending_outbox(1)
    assert pending[0] == min(adapter.pending_outbox(100), key=lambda event: (
        event.created_at, event.event_id))
def test_ordena_runs_recuperaveis_por_tenant_antes_do_limite(adapter, clock) -> None:
    runs = (
        _run("run-a").model_copy(update={"tenant_id": "b"}),
        _run("run-z").model_copy(update={"tenant_id": "a"}),
        _run("run-shared").model_copy(update={"tenant_id": "b"}),
        _run("run-shared").model_copy(update={"tenant_id": "a"}),)
    for run in runs:
        adapter.put_run(run)
    recoverable = adapter.list_recoverable_runs(clock.now(), 2)
    assert tuple((run.tenant_id, run.run_id) for run in recoverable) == (
        ("a", "run-shared"), ("a", "run-z"))
def test_limita_ancestralidade_longa_sem_recursao(adapter, clock, monkeypatch) -> None:
    base = _raw_record("deep-1", "deep-agent", 1, clock.now())
    previous = None
    with adapter.write_transaction() as connection:
        for sequence in range(1, 1102):
            snapshot = f"deep-{sequence}"
            record = base.model_copy(update={
                "manifest_id": f"manifest-deep-agent-{snapshot}", "snapshot_id": snapshot,
                "manifest_key": f"raw/354130/CNES/2026-07/{snapshot}/manifest.json",
                "snapshot_mode": "FULL" if sequence == 1 else "DELTA",
                "base_snapshot_id": None if sequence == 1 else "deep-1", "sequence": sequence,
                "previous_manifest_sha256": previous, "manifest_sha256": f"{sequence:064x}",
                "created_at": clock.now() + timedelta(seconds=sequence)})
            adapter.put_manifest_record(connection, record)
            previous = record.manifest_sha256
    deserialize = sqlite_publication.deserialize_model
    calls = []
    monkeypatch.setattr(sqlite_publication, "deserialize_model",
                        lambda *args: (calls.append(None), deserialize(*args))[1])
    assert adapter.list_raw_manifest_chain("354130", "CNES", "ST", "2026-07", 2) == ()
    with adapter.write_transaction() as connection:
        for index in range(2):
            shared = base.model_copy(update={
                "manifest_id": f"shared-{index}", "snapshot_id": f"shared-{index}",
                "manifest_key": f"raw/354130/CNES/2026-07/shared-{index}/manifest.json",
                "snapshot_mode": "DELTA", "sequence": 2, "base_snapshot_id": "missing",
                "previous_manifest_sha256": "d" * 64, "manifest_sha256": "e" * 64})
            adapter.put_manifest_record(connection, shared)
        for index in range(100):
            broken = base.model_copy(update={
                "manifest_id": f"invalid-{index:03}", "snapshot_id": f"invalid-{index:03}",
                "manifest_key": f"raw/354130/CNES/2026-07/invalid-{index:03}/manifest.json",
                "snapshot_mode": "DELTA", "sequence": 3, "base_snapshot_id": "missing",
                "previous_manifest_sha256": "e" * 64,
                "created_at": clock.now() + timedelta(days=1, seconds=index)})
            adapter.put_manifest_record(connection, broken)
    calls.clear()
    assert adapter.list_raw_manifest_chain("354130", "CNES", "ST", "2026-07", 2) == ()
    assert len(calls) <= 10
    levels = tuple(tuple(base.model_copy(update={
        "manifest_id": f"branch-{level}-{index}", "sequence": level + 2,
    }) for index in range(31)) for level in range(4))
    valid = tuple(base.model_copy(update={"manifest_id": f"valid-{sequence}",
        "sequence": sequence, "previous_manifest_sha256": f"{sequence + 10:064x}"})
        for sequence in range(2, 6))
    head = base.model_copy(update={"manifest_id": "branch-head", "sequence": 6})
    graph = {head.manifest_id: (*levels[-1], valid[-1]), valid[0].manifest_id: (base,)}
    graph.update({item.manifest_id: levels[level - 1] if level else ()
                  for level, siblings in enumerate(levels) for item in siblings})
    graph.update({valid[index].manifest_id: (valid[index - 1],) for index in range(1, 4)})
    def predecessors(connection, identity, current, limit):
        return graph.get(current.manifest_id, ())
    monkeypatch.setattr(sqlite_publication, "_predecessors", predecessors)
    assert sqlite_publication._build_ancestry(None, (), head, 100) is not None
    monkeypatch.setattr(sqlite_publication, "_build_ancestry", lambda *args: ())
    with adapter.read_connection() as connection:
        identity = ("354130", "CNES", "ST", "2026-07")
        assert sqlite_publication._select_raw_chain(connection, identity, 2) == ()
