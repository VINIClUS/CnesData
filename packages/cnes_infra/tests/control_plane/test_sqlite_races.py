import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from typing import Any

import pytest

from cnes_domain.control_plane.commands import (
    BeginIdempotency,
    RenewJobLease,
    TransitionRun,
)
from cnes_domain.control_plane.entities import DatasetPointer, Tenant
from cnes_domain.control_plane.enums import RunState
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane import sqlite_adapter
from cnes_infra.control_plane.sqlite_adapter import (
    SQLiteControlPlane,
    _SQLiteBusyError,
    _SQLiteFilesystemError,
)
from packages.cnes_infra.tests.contracts.clock import (
    MutableClock,
    _agent,
    _claim_job,
    _event,
    _job,
    _put_units,
    _raw_record,
    _run,
    _store_record,
    _unit,
)
from packages.cnes_infra.tests.contracts.control_plane_contract import _publish

_NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(_NOW)


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


def _race(first: Any, second: Any) -> tuple[Any, Any]:
    barrier = Barrier(3)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (
            executor.submit(_capture, first, barrier),
            executor.submit(_capture, second, barrier),
        )
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
        tenant_id="354130",
        run_id="run-a",
        expected_state=RunState.PROCESSING,
        new_state=RunState.PUBLISHING,
    )
    conflicting = existing.model_copy(update={"aggregate_id": "run-a"})

    with pytest.raises(Conflict, match="outbox_event_conflict"):
        adapter.transition_run(transition, conflicting)

    assert adapter.get_run("354130", "run-a") == _run("run-a")
    assert adapter.pending_outbox(10) == (existing,)


def test_serializa_claim_renovacao_e_publicacao_concorrentes(
    adapter, database_path, clock
) -> None:
    _prepare_job(adapter)
    writers = (
        SQLiteControlPlane(database_path, clock.now),
        SQLiteControlPlane(database_path, clock.now),
    )
    claims = _race(
        lambda: writers[0].claim_job(_claim_job("job-a", "worker-a", clock)),
        lambda: writers[1].claim_job(_claim_job("job-a", "worker-b", clock)),
    )
    winners = [result for result in claims if result is not None]
    assert len(winners) == 1
    claimed = winners[0]
    renew = RenewJobLease(
        tenant_id="354130",
        job_id="job-a",
        owner=claimed.lease_owner,
        fencing_token=claimed.fencing_token,
        now=clock.now(),
        lease_seconds=60,
    )
    renewals = _race(
        lambda: writers[0].renew_job_lease(renew),
        lambda: writers[1].renew_job_lease(renew),
    )
    assert renewals[0] == renewals[1]
    adapter.put_run(_run("run-pub-a", RunState.PUBLISHING))
    adapter.put_run(_run("run-pub-b", RunState.PUBLISHING))
    publications = _race(
        lambda: writers[0].publish_dataset(_publish("run-pub-a", "pub-a", None, False)),
        lambda: writers[1].publish_dataset(_publish("run-pub-b", "pub-b", None, False)),
    )
    assert sum(isinstance(result, DatasetPointer) for result in publications) == 1
    assert sum(isinstance(result, Conflict) for result in publications) == 1


def test_incrementa_fence_apos_expiracao(adapter, clock) -> None:
    _prepare_job(adapter)
    first = adapter.claim_job(_claim_job("job-a", "worker-a", clock))
    clock.advance(timedelta(seconds=31))

    second = adapter.claim_job(_claim_job("job-a", "worker-b", clock))

    assert first is not None
    assert second is not None
    assert (first.fencing_token, second.fencing_token) == (1, 2)


def test_converte_esgotamento_do_busy_timeout_em_erro_local(adapter, database_path, clock) -> None:
    blocker = sqlite3.connect(database_path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    tenant = Tenant(tenant_id="354130", municipality_name="Epitácio", created_at=clock.now())
    try:
        with pytest.raises(_SQLiteBusyError, match="sqlite_busy"):
            adapter.put_tenant(tenant)
    finally:
        blocker.rollback()
        blocker.close()


def test_reabertura_preserva_registros_leases_pointer_idempotencia_e_outbox(
    adapter, database_path, clock
) -> None:
    tenant = Tenant(tenant_id="354130", municipality_name="Epitácio", created_at=clock.now())
    adapter.put_tenant(tenant)
    _prepare_job(adapter)
    claimed = adapter.claim_job(_claim_job("job-a", "worker-a", clock))
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    pointer = adapter.publish_dataset(_publish("run-a", "published", None, False))
    idempotency = BeginIdempotency(
        tenant_id="354130",
        scope="jobs",
        key="key-a",
        request_hash="a" * 64,
        resource_id="job-a",
        now=clock.now(),
        expires_at=clock.now() + timedelta(minutes=5),
    )
    adapter.begin_idempotency(idempotency)

    reopened = SQLiteControlPlane(database_path, clock.now)
    reopened.initialize()

    assert reopened.get_tenant(tenant.tenant_id) == tenant
    assert reopened.get_job("354130", "job-a") == claimed
    assert reopened.get_dataset_pointer("354130", "gold") == pointer
    assert not reopened.begin_idempotency(idempotency).created
    assert reopened.pending_outbox(10) == adapter.pending_outbox(10)


def test_rejeita_banco_em_filesystem_de_rede(tmp_path, clock, monkeypatch) -> None:
    monkeypatch.setattr(sqlite_adapter, "_is_network_filesystem", lambda path: True)
    adapter = SQLiteControlPlane(tmp_path / "network" / "control.sqlite3", clock.now)

    with pytest.raises(_SQLiteFilesystemError, match="sqlite_network_filesystem"):
        adapter.initialize()


def test_ordena_e_limita_todos_os_metodos_de_listagem(adapter, clock) -> None:
    adapter.put_agent(_agent("agent-order"))
    for job_id in ("job-b", "job-a"):
        adapter.create_job(_job(job_id, "agent-order"), _event(f"created-{job_id}"))
    adapter.put_run(_run("run-b", RunState.WAITING_INPUTS))
    adapter.put_run(_run("run-a", RunState.WAITING_INPUTS))
    adapter.put_run(_run("run-units"))
    units = tuple(
        _unit(unit_id).model_copy(update={"run_id": "run-units"})
        for unit_id in ("unit-a", "unit-b")
    )
    _put_units(adapter, units, "run-units")
    base = _raw_record("base-agent-raw", "agent-raw", 1, clock.now())
    delta = _raw_record(
        "delta", "agent-raw", 2, clock.now() + timedelta(seconds=1)
    )
    _store_record(adapter, base, clock)
    _store_record(adapter, delta, clock)

    assert tuple(job.job_id for job in adapter.list_claimable_jobs(
        "354130", "agent-order", 1
    )) == ("job-a",)
    assert tuple(run.run_id for run in adapter.list_waiting_runs_for_dependency(
        "354130", "CNES", "ST", "2026-07", 1
    )) == ("run-a",)
    assert adapter.list_recoverable_runs(clock.now(), 1)[0].run_id == "run-a"
    assert tuple(unit.unit_id for unit in adapter.list_run_units(
        "354130", "run-units"
    )) == ("unit-a", "unit-b")
    assert adapter.list_raw_manifest_chain("354130", "CNES", "ST", "2026-07", 1) == ()
    assert len(adapter.list_raw_manifest_chain("354130", "CNES", "ST", "2026-07", 2)) == 2
    pending = adapter.pending_outbox(1)
    assert len(pending) == 1
    assert pending[0] == min(adapter.pending_outbox(100), key=lambda event: (
        event.created_at, event.event_id
    ))
