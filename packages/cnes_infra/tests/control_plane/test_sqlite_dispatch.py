from datetime import UTC, datetime

import pytest

from cnes_domain.control_plane.commands import BindRunDispatch, FinishRunDispatch
from cnes_domain.control_plane.enums import DispatchOutcome, RunState
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.contracts.clock import MutableClock, _prepare_unit, _reserve, _run
from packages.cnes_infra.tests.contracts.control_plane_contract import _publish


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


def test_rejeita_publicacao_de_competencia_divergente(adapter) -> None:
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    command = _publish("run-a", "published-a", None, False)
    version = command.version.model_copy(
        update={"run_manifest_key": "reconciliation/354130/2026-06/run-a/run-manifest.json"}
    )
    with pytest.raises(Conflict, match="run_competencia_mismatch"):
        adapter.publish_dataset(command.model_copy(update={"version": version}))
    assert adapter.get_dataset_pointer("354130", "gold") is None


def test_migra_resposta_de_publicacao(adapter) -> None:
    adapter.put_run(_run("run-a", RunState.PUBLISHING))
    command = _publish("run-a", "published-a", None, False)
    pointer = adapter.publish_dataset(command)
    with adapter.write_transaction() as connection:
        connection.execute("ALTER TABLE dataset_publications DROP COLUMN response_data")
    reopened = SQLiteControlPlane(adapter._database_path, adapter.now)
    reopened.initialize()
    assert reopened.publish_dataset(command) == pointer


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
    adapter.finish_run_dispatch(
        FinishRunDispatch(
            tenant_id="354130",
            run_id="run-a",
            dispatch_id=dispatch.dispatch_id,
            outcome=DispatchOutcome.SUCCEEDED,
            finished_at=clock.now(),
        )
    )
    with adapter.write_transaction() as connection:
        connection.execute("ALTER TABLE run_dispatch_bind_writes DROP COLUMN response_data")
    reopened = SQLiteControlPlane(adapter._database_path, clock.now)
    reopened.initialize()
    assert reopened.bind_run_dispatch(bind) == started


def test_rejeita_bind_de_dispatch_finalizado_sem_replay(adapter, clock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    adapter.finish_run_dispatch(
        FinishRunDispatch(
            tenant_id="354130",
            run_id="run-a",
            dispatch_id=dispatch.dispatch_id,
            outcome=DispatchOutcome.SUCCEEDED,
            finished_at=clock.now(),
        )
    )
    bind = BindRunDispatch(
        tenant_id="354130",
        run_id="run-a",
        dispatch_id=dispatch.dispatch_id,
        execution_ref="exec-a",
        now=clock.now(),
        lease_seconds=30,
    )
    with pytest.raises(Conflict, match="dispatch_terminal"):
        adapter.bind_run_dispatch(bind)


def test_reexecuta_finish_apos_nova_geracao(adapter, clock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    finish = FinishRunDispatch(
        tenant_id="354130",
        run_id="run-a",
        dispatch_id=dispatch.dispatch_id,
        outcome=DispatchOutcome.SUCCEEDED,
        finished_at=clock.now(),
    )
    finished = adapter.finish_run_dispatch(finish)
    replacement = _reserve(adapter, clock)
    assert replacement.dispatch_id != dispatch.dispatch_id
    assert adapter.finish_run_dispatch(finish) == finished


def test_migra_resposta_de_finish(adapter, clock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    finish = FinishRunDispatch(
        tenant_id="354130",
        run_id="run-a",
        dispatch_id=dispatch.dispatch_id,
        outcome=DispatchOutcome.SUCCEEDED,
        finished_at=clock.now(),
    )
    finished = adapter.finish_run_dispatch(finish)
    with adapter.write_transaction() as connection:
        connection.execute("ALTER TABLE run_dispatch_terminal_writes DROP COLUMN response_data")
    reopened = SQLiteControlPlane(adapter._database_path, clock.now)
    reopened.initialize()
    assert reopened.finish_run_dispatch(finish) == finished


def test_nao_reconstroi_respostas_de_dispatch_substituido(adapter, clock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    bind = BindRunDispatch(
        tenant_id="354130",
        run_id="run-a",
        dispatch_id=dispatch.dispatch_id,
        execution_ref="exec-a",
        now=clock.now(),
        lease_seconds=30,
    )
    adapter.bind_run_dispatch(bind)
    adapter.finish_run_dispatch(
        FinishRunDispatch(
            tenant_id="354130",
            run_id="run-a",
            dispatch_id=dispatch.dispatch_id,
            outcome=DispatchOutcome.SUCCEEDED,
            finished_at=clock.now(),
        )
    )
    _reserve(adapter, clock)
    with adapter.write_transaction() as connection:
        connection.executescript(
            "ALTER TABLE run_dispatch_bind_writes DROP COLUMN response_data;"
            "ALTER TABLE run_dispatch_terminal_writes DROP COLUMN response_data;"
        )
    reopened = SQLiteControlPlane(adapter._database_path, clock.now)
    reopened.initialize()
    with reopened.read_connection() as connection:
        responses = connection.execute(
            "SELECT response_data FROM run_dispatch_bind_writes UNION ALL "
            "SELECT response_data FROM run_dispatch_terminal_writes"
        ).fetchall()
    assert responses == [(None,), (None,)]
