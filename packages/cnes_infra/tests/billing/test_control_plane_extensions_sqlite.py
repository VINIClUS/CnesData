"""Extensões de billing do control plane SQLite: run sem medição e vinculação."""

import sqlite3
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from cnes_domain.billing.commands import ConsumeReservationCommand, ReleaseReservationCommand
from cnes_domain.billing.errors import (
    BillingDisabledError,
    IdempotencyConflict,
    PermanentBillingError,
)
from cnes_domain.billing.models import ReservationStatus
from cnes_domain.control_plane.entities import Run
from cnes_domain.control_plane.enums import RunState
from cnes_domain.ports.control_plane import ControlPlanePort
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.billing.billing_factories import NOW
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    DEPENDENCIES,
    HASH_B,
    TENANT,
    make_reserve_command,
)
from packages.cnes_infra.tests.billing.test_control_plane_extensions import (
    OTHER_DISPATCH,
    authorized,
    binding,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock

TABLES = (
    "runs", "run_dependencies", "run_billing_states", "idempotency_records", "outbox_events",
)


class RecordingConnection:
    def __init__(self, inner: sqlite3.Connection, script: "Script") -> None:
        self.inner = inner
        self.script = script

    def execute(self, statement: str, *args: Any) -> Any:
        self.script.tick(statement)
        return self.inner.execute(statement, *args)

    def executemany(self, statement: str, *args: Any) -> Any:
        self.script.tick(statement)
        return self.inner.executemany(statement, *args)

    def commit(self) -> None:
        self.script.tick("COMMIT")
        self.inner.commit()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


class Script:
    def __init__(self, fail_at: int | None = None) -> None:
        self.fail_at = fail_at
        self.statements: list[str] = []
        self.armed = False

    def tick(self, statement: str) -> None:
        if statement.startswith("BEGIN"):
            self.armed = True
            return
        if not self.armed:
            return
        self.statements.append(statement)
        if self.fail_at == len(self.statements):
            raise sqlite3.OperationalError("injected")


def instrument(adapter: SQLiteControlPlane, script: Script, monkeypatch: Any) -> None:
    connect = adapter._connect
    monkeypatch.setattr(adapter, "_connect", lambda: RecordingConnection(connect(), script))


def row_counts(path: Path) -> dict[str, int]:
    connection = sqlite3.connect(path)
    try:
        return {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
            for table in TABLES
        }
    finally:
        connection.close()


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "control.sqlite3"


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(NOW)


@pytest.fixture
def adapter(database: Path, clock: MutableClock) -> Iterator[SQLiteControlPlane]:
    plane = SQLiteControlPlane(database, clock.now)
    plane.initialize()
    return plane


def test_cria_run_sem_medicao_com_todas_as_linhas(adapter, database) -> None:
    run = adapter.create_unmetered_run(authorized())

    assert run.state is RunState.WAITING_INPUTS
    assert run.missing_sources == ("CNES/LFCES",)
    assert adapter.get_run(TENANT, "run-01") == run
    assert row_counts(database) == dict.fromkeys(TABLES, 1) | {"run_dependencies": 2}


def test_grava_companion_nao_vinculado_com_autorizacao(adapter) -> None:
    command = authorized()

    adapter.create_unmetered_run(command)

    state = adapter.get_run_billing_state(TENANT, "run-01")
    assert state.authorization == command.authorization
    assert (state.execution_generation, state.fencing_token) == (0, 0)
    assert state.execution_dispatch_id is None


def test_grava_evento_de_run_autorizado(adapter) -> None:
    adapter.create_unmetered_run(authorized())

    (event,) = adapter.pending_outbox(10)

    assert event.event_type == "run.authorized"
    assert event.aggregate_id == "run-01"
    assert event.payload["entitlement_version"] == 1


def test_replay_com_mesmo_hash_devolve_o_mesmo_run_sem_duplicar(adapter, database) -> None:
    first = adapter.create_unmetered_run(authorized())

    assert adapter.create_unmetered_run(authorized()) == first
    assert row_counts(database) == dict.fromkeys(TABLES, 1) | {"run_dependencies": 2}


def test_rejeita_replay_com_outro_hash(adapter) -> None:
    adapter.create_unmetered_run(authorized())

    with pytest.raises(IdempotencyConflict, match="key=req-01"):
        adapter.create_unmetered_run(authorized(request_hash=HASH_B))


def test_run_existente_sob_outra_chave_e_conflito_sem_gravar_nada(adapter, database) -> None:
    adapter.put_run(
        Run(
            tenant_id=TENANT, run_id="run-01", competencia="2026-08", dataset_name="cnes_vinculos",
            state=RunState.WAITING_INPUTS, dependencies=DEPENDENCIES, missing_sources=(),
            created_at=NOW,
        )
    )

    with pytest.raises(PermanentBillingError) as error:
        adapter.create_unmetered_run(authorized())

    assert error.value.code == "run_conflict"
    counts = row_counts(database)
    assert (counts["run_billing_states"], counts["idempotency_records"]) == (0, 0)
    assert counts["outbox_events"] == 0


def test_replay_sem_run_persistido_falha_com_run_ausente(adapter, database) -> None:
    adapter.create_unmetered_run(authorized())
    connection = sqlite3.connect(database)
    connection.execute("DELETE FROM run_dependencies")
    connection.execute("DELETE FROM runs")
    connection.commit()
    connection.close()

    with pytest.raises(PermanentBillingError) as error:
        adapter.create_unmetered_run(authorized())

    assert error.value.code == "run_missing_after_replay"


def test_idempotencia_expirada_e_sobrescrita(adapter, clock, database) -> None:
    adapter.create_unmetered_run(authorized())
    clock.advance(timedelta(days=2))

    run = adapter.create_unmetered_run(authorized(run_id="run-02", request_hash=HASH_B))

    assert run.run_id == "run-02"
    assert row_counts(database)["idempotency_records"] == 1
    assert adapter.get_run_billing_state(TENANT, "run-02") is not None


def test_falha_apos_cada_statement_desfaz_a_criacao_inteira(
    adapter, database, monkeypatch
) -> None:
    measure = Script()
    instrument(adapter, measure, monkeypatch)
    adapter.create_unmetered_run(authorized(run_id="run-probe", idempotency_key="probe"))
    total = len(measure.statements)
    monkeypatch.undo()
    assert total >= 6
    baseline = row_counts(database)

    for position in range(1, total + 1):
        instrument(adapter, Script(fail_at=position), monkeypatch)
        with pytest.raises(sqlite3.OperationalError, match="injected"):
            adapter.create_unmetered_run(authorized())
        monkeypatch.undo()
        assert row_counts(database) == baseline, position

    assert adapter.create_unmetered_run(authorized()).run_id == "run-01"


def test_primeira_vinculacao_persiste_a_execucao(adapter) -> None:
    adapter.create_unmetered_run(authorized())

    state = adapter.bind_run_execution(binding())

    assert state.execution_ref == "exec-1"
    assert adapter.get_run_billing_state(TENANT, "run-01") == state


def test_vinculacao_idempotente_nao_grava(adapter, monkeypatch) -> None:
    adapter.create_unmetered_run(authorized())
    first = adapter.bind_run_execution(binding())
    script = Script()
    instrument(adapter, script, monkeypatch)

    assert adapter.bind_run_execution(binding()) == first
    assert not any(statement.startswith("UPDATE") for statement in script.statements)


def test_vinculacao_com_referencia_diferente_e_conflito(adapter) -> None:
    adapter.create_unmetered_run(authorized())
    adapter.bind_run_execution(binding())

    with pytest.raises(PermanentBillingError) as error:
        adapter.bind_run_execution(binding(execution_ref="exec-2"))

    assert error.value.code == "run_execution_conflict"


def test_vinculacao_com_anterior_obsoleto_e_rejeitada(adapter) -> None:
    adapter.create_unmetered_run(authorized())
    adapter.bind_run_execution(binding())

    with pytest.raises(PermanentBillingError) as error:
        adapter.bind_run_execution(binding(dispatch_id=OTHER_DISPATCH, generation=2))

    assert error.value.code == "run_execution_stale"


def test_vinculacao_sem_companion_falha(adapter) -> None:
    with pytest.raises(PermanentBillingError) as error:
        adapter.bind_run_execution(binding())

    assert error.value.code == "run_billing_state_missing"


def test_estado_de_billing_ausente_devolve_none(adapter) -> None:
    assert adapter.get_run_billing_state(TENANT, "run-01") is None


def test_migracao_da_tabela_de_companion_e_idempotente(adapter, database) -> None:
    adapter.create_unmetered_run(authorized())

    adapter.initialize()
    adapter.initialize()

    assert adapter.get_run_billing_state(TENANT, "run-01") is not None


def test_migra_banco_antigo_sem_a_tabela_de_companion(adapter, database) -> None:
    connection = sqlite3.connect(database)
    connection.execute("DROP TABLE run_billing_states")
    connection.commit()
    connection.close()

    adapter.initialize()

    assert adapter.create_unmetered_run(authorized()).run_id == "run-01"


def test_reserva_com_criacao_nao_existe_no_modo_desabilitado(adapter) -> None:
    with pytest.raises(BillingDisabledError, match="operation=reserve_and_create_run"):
        adapter.reserve_and_create_run(make_reserve_command())


def test_consumo_e_liberacao_devolvem_reservas_desabilitadas(adapter) -> None:
    consumed = adapter.consume_reservation(ConsumeReservationCommand(ACCOUNT, "res-1", 0, NOW))
    released = adapter.release_reservation(
        ReleaseReservationCommand(ACCOUNT, "res-1", NOW, "run_failed")
    )

    assert consumed.status is ReservationStatus.CONSUMED
    assert released.status is ReservationStatus.RELEASED


def test_sqlite_control_plane_cumpre_a_porta(adapter) -> None:
    assert isinstance(adapter, ControlPlanePort)
