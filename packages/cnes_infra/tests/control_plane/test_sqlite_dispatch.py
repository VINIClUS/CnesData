from datetime import UTC, datetime

import pytest

from cnes_domain.control_plane.commands import BindRunDispatch, FinishRunDispatch
from cnes_domain.control_plane.enums import DispatchOutcome
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.contracts.clock import MutableClock, _prepare_unit


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(datetime(2026, 7, 15, 12, tzinfo=UTC))


@pytest.fixture
def adapter(tmp_path, clock) -> SQLiteControlPlane:
    control_plane = SQLiteControlPlane(tmp_path / "control-plane.sqlite3", clock.now)
    control_plane.initialize()
    return control_plane


def test_reexecuta_bind_apos_dispatch_terminal(adapter, clock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    bind = BindRunDispatch(
        tenant_id="354130",
        run_id="run-a",
        dispatch_id=dispatch.dispatch_id,
        execution_ref="exec-a",
        now=clock.now(),
        lease_seconds=30,
    )
    started = adapter.bind_run_dispatch(bind)
    adapter.finish_run_dispatch(
        FinishRunDispatch(
            tenant_id="354130",
            run_id="run-a",
            dispatch_id=dispatch.dispatch_id,
            outcome=DispatchOutcome.SUCCEEDED,
            finished_at=clock.now(),
        )
    )
    assert adapter.bind_run_dispatch(bind) == started


def test_cria_indice_para_outbox_pendente(adapter) -> None:
    with adapter.read_connection() as connection:
        indexes = connection.execute("PRAGMA index_list(outbox_events)").fetchall()
    assert "ix_outbox_pending" in {row[1] for row in indexes}


def test_migra_resposta_de_bind(adapter, clock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    bind = BindRunDispatch(
        tenant_id="354130",
        run_id="run-a",
        dispatch_id=dispatch.dispatch_id,
        execution_ref="exec-a",
        now=clock.now(),
        lease_seconds=30,
    )
    started = adapter.bind_run_dispatch(bind)
    with adapter.write_transaction() as connection:
        connection.execute("ALTER TABLE run_dispatch_bind_writes DROP COLUMN response_data")
    reopened = SQLiteControlPlane(adapter._database_path, clock.now)
    reopened.initialize()
    assert reopened.bind_run_dispatch(bind) == started
