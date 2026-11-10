"""Claims persistidos recusam replays obsoletos e vínculos misturados."""

from datetime import timedelta

import pytest

from cnes_domain.control_plane.commands import (
    BindRunDispatch,
    CancelReservedRunDispatch,
    ClaimRunDispatchStart,
    FinishRunDispatch,
    ReserveRunDispatch,
)
from cnes_domain.control_plane.dispatch_start import DispatchStartClaim
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.control_plane.sqlite_claims import write_dispatch_record
from packages.cnes_infra.tests.contracts.clock import MutableClock, _prepare_unit
from packages.cnes_infra.tests.control_plane.test_dynamodb_promotion_snapshot import (
    NOW,
    seed,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_promotion_snapshot import (
    ddb as dynamodb_fixture,
)

ddb = dynamodb_fixture


@pytest.fixture(params=["sqlite", "dynamodb"])
def started_store(request, tmp_path):
    if request.param == "dynamodb":
        adapter, _ = request.getfixturevalue("ddb")
    else:
        adapter = SQLiteControlPlane(tmp_path / "claims.sqlite", lambda: NOW)
        adapter.initialize()
    return adapter, seed(adapter) if request.param == "dynamodb" else _sqlite_seed(adapter)


def _sqlite_seed(adapter):
    dispatch = _prepare_unit(adapter, MutableClock(NOW))
    claim = DispatchStartClaim(
        dispatch_id=dispatch.dispatch_id,
        generation=dispatch.generation,
        permit_token=1,
        fence_version=0,
        request_sha256="a" * 64,
    )
    command = ClaimRunDispatchStart(
        tenant_id=dispatch.tenant_id,
        run_id=dispatch.run_id,
        dispatch_id=dispatch.dispatch_id,
        claim=claim,
        now=NOW,
        lease_seconds=300,
    )
    return dispatch, None, None, claim, command


def _bind(dispatch, claim):
    return BindRunDispatch(
        tenant_id=dispatch.tenant_id,
        run_id=dispatch.run_id,
        dispatch_id=dispatch.dispatch_id,
        execution_ref="exec-a",
        claim=claim,
        now=NOW,
        lease_seconds=300,
    )


def _cancel(command):
    return CancelReservedRunDispatch(
        tenant_id=command.tenant_id,
        run_id=command.run_id,
        dispatch_id=command.dispatch_id,
        claim=command.claim,
        canceled_at=NOW,
    )


@pytest.mark.parametrize("change", ["missing", "identity", "generation", "expired"])
def test_claim_obsoleto_nao_modifica_dispatch(started_store, change):
    adapter, (dispatch, _, _, claim, command) = started_store
    updates = {
        "missing": {"run_id": "missing"},
        "identity": {
            "dispatch_id": "0" * 16,
            "claim": claim.model_copy(update={"dispatch_id": "0" * 16}),
        },
        "generation": {"claim": claim.model_copy(update={"generation": 2})},
        "expired": {"now": dispatch.lease_until},
    }
    assert adapter.claim_run_dispatch_start(command.model_copy(update=updates[change])) is None
    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == dispatch


@pytest.mark.parametrize("state", ["starting", "started", "terminal"])
def test_dispatch_ja_reivindicado_nao_aceita_novo_claim_ou_cancelamento(started_store, state):
    adapter, (dispatch, _, _, claim, command) = started_store
    current = adapter.claim_run_dispatch_start(command)
    if state == "started":
        current = adapter.bind_run_dispatch(_bind(dispatch, claim))
    if state == "terminal":
        current = adapter.finish_run_dispatch(
            FinishRunDispatch(
                tenant_id=dispatch.tenant_id,
                run_id=dispatch.run_id,
                dispatch_id=dispatch.dispatch_id,
                claim=claim,
                outcome=DispatchOutcome.SUCCEEDED,
                finished_at=NOW,
            )
        )
    assert adapter.claim_run_dispatch_start(command) is None
    assert adapter.cancel_reserved_run_dispatch(_cancel(command)) is None
    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == current


@pytest.mark.parametrize("change", ["missing", "identity", "generation"])
def test_cancelamento_obsoleto_preserva_reserva(started_store, change):
    adapter, (dispatch, _, _, claim, command) = started_store
    updates = {
        "missing": {"run_id": "missing"},
        "identity": {
            "dispatch_id": "0" * 16,
            "claim": claim.model_copy(update={"dispatch_id": "0" * 16}),
        },
        "generation": {"claim": claim.model_copy(update={"generation": 2})},
    }
    assert (
        adapter.cancel_reserved_run_dispatch(_cancel(command).model_copy(update=updates[change]))
        is None
    )
    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == dispatch


@pytest.mark.parametrize("state", ["reserved", "starting", "started"])
def test_bind_com_claim_divergente_preserva_dispatch(started_store, state):
    adapter, (dispatch, _, _, claim, command) = started_store
    current = dispatch
    if state != "reserved":
        current = adapter.claim_run_dispatch_start(command)
    if state == "started":
        current = adapter.bind_run_dispatch(_bind(dispatch, claim))
    wrong = claim.model_copy(update={"permit_token": 2})
    with pytest.raises(Conflict, match=r"dispatch_bind(?:ing)?_conflict"):
        adapter.bind_run_dispatch(_bind(dispatch, wrong))
    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == current


@pytest.mark.parametrize("state", ["starting", "started"])
def test_finish_sem_claim_nao_libera_dispatch_reivindicado(started_store, state):
    adapter, (dispatch, _, _, claim, command) = started_store
    current = adapter.claim_run_dispatch_start(command)
    if state == "started":
        current = adapter.bind_run_dispatch(_bind(dispatch, claim))
    with pytest.raises(Conflict, match="dispatch_binding_conflict"):
        adapter.finish_run_dispatch(
            FinishRunDispatch(
                tenant_id=dispatch.tenant_id,
                run_id=dispatch.run_id,
                dispatch_id=dispatch.dispatch_id,
                outcome=DispatchOutcome.CANCELED,
                finished_at=NOW + timedelta(hours=1),
            )
        )
    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == current


def test_claim_persistido_sobrevive_ao_lease_e_bind_e_idempotente(started_store):
    adapter, (dispatch, _, _, claim, command) = started_store
    claimed = adapter.claim_run_dispatch_start(command)
    late = NOW + timedelta(hours=1)
    replay = adapter.reserve_run_dispatch(
        ReserveRunDispatch(
            tenant_id=dispatch.tenant_id,
            run_id=dispatch.run_id,
            wave_id=dispatch.wave_id,
            unit_ids=dispatch.unit_ids,
            now=late,
            lease_seconds=300,
        )
    )
    assert replay == claimed
    assert adapter.get_active_run_dispatch(dispatch.tenant_id, dispatch.run_id) == claimed
    bind = _bind(dispatch, claim).model_copy(update={"now": late})
    started = adapter.bind_run_dispatch(bind)
    assert started.state is DispatchState.STARTED
    assert adapter.bind_run_dispatch(bind) == started


def test_sqlite_claim_impede_nova_wave_mesmo_apos_expirar(tmp_path):
    adapter = SQLiteControlPlane(tmp_path / "claims.sqlite", lambda: NOW)
    adapter.initialize()
    dispatch, _, _, _, command = _sqlite_seed(adapter)
    claimed = adapter.claim_run_dispatch_start(command)
    with pytest.raises(Conflict, match="dispatch_live"):
        adapter.reserve_run_dispatch(
            ReserveRunDispatch(
                tenant_id=dispatch.tenant_id,
                run_id=dispatch.run_id,
                wave_id="b" * 16,
                unit_ids=dispatch.unit_ids,
                now=NOW + timedelta(hours=1),
                lease_seconds=300,
            )
        )
    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == claimed


@pytest.mark.parametrize("divergent", [False, True])
def test_sqlite_bind_legado_sem_resposta_preserva_vinculo_persistido(tmp_path, divergent):
    adapter = SQLiteControlPlane(tmp_path / "legacy.sqlite", lambda: NOW)
    adapter.initialize()
    dispatch, _, _, claim, command = _sqlite_seed(adapter)
    adapter.claim_run_dispatch_start(command)
    bind = _bind(dispatch, claim)
    started = adapter.bind_run_dispatch(bind)
    with adapter.write_transaction() as connection:
        connection.execute("ALTER TABLE run_dispatch_bind_writes DROP COLUMN response_data")
        connection.execute("ALTER TABLE run_dispatch_bind_writes ADD COLUMN response_data TEXT")
        if divergent:
            write_dispatch_record(
                connection, started.model_copy(update={"execution_ref": "exec-b"})
            )
    if divergent:
        with pytest.raises(Conflict, match="dispatch_binding_conflict"):
            adapter.bind_run_dispatch(bind)
    else:
        assert adapter.bind_run_dispatch(bind) == started
    persisted = adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id)
    assert persisted is not None
    assert persisted.execution_ref == ("exec-b" if divergent else "exec-a")


def test_sqlite_starting_corrompido_sem_claim_falha_fechado(tmp_path):
    from pydantic import ValidationError

    adapter = SQLiteControlPlane(tmp_path / "legacy.sqlite", lambda: NOW)
    adapter.initialize()
    dispatch, _, _, _, _ = _sqlite_seed(adapter)
    starting = dispatch.model_copy(update={"state": DispatchState.STARTING})
    with adapter.write_transaction() as connection:
        write_dispatch_record(connection, starting)
    with pytest.raises(ValidationError, match="start_claim_required"):
        adapter.reserve_run_dispatch(
            ReserveRunDispatch(
                tenant_id=dispatch.tenant_id,
                run_id=dispatch.run_id,
                wave_id=dispatch.wave_id,
                unit_ids=dispatch.unit_ids,
                now=NOW + timedelta(hours=1),
                lease_seconds=300,
            )
        )
    with adapter.read_connection() as connection:
        assert connection.execute("SELECT state FROM run_dispatches").fetchone() == ("STARTING",)
