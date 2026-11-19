"""Claims persistidos recusam replays obsoletos e vínculos misturados."""

from datetime import timedelta

import pytest

from cnes_domain.control_plane.commands import (
    BindRunDispatch,
    CancelReservedRunDispatch,
    ClaimRunDispatchStart,
    FailRunUnit,
    FinishRunDispatch,
    ReserveRunDispatch,
)
from cnes_domain.control_plane.dispatch_start import DispatchStartClaim
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState, RunUnitState
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.control_plane.sqlite_claims import write_dispatch_record
from packages.cnes_infra.tests.contracts.clock import (
    MutableClock,
    _claim_unit_command,
    _commit_command,
    _event,
    _prepare_unit,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import _TABLE_NAME
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


def _settle_unit(adapter, dispatch, claimed, failure):
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    event = _event("starting-unit-finished")
    if failure:
        failed = FailRunUnit(
            tenant_id=command.tenant_id, run_id=command.run_id, unit_id=command.unit_id,
            dispatch_id=command.dispatch_id, owner=command.owner,
            fencing_token=command.fencing_token, error_code="processing_failed", retryable=False,
        )
        result = adapter.fail_run_unit(failed, event)
        assert result.error_code == "processing_failed"
    else:
        result = adapter.commit_run_unit(command, event)
        assert result.output_manifests == command.output_manifests
    assert adapter.read_outbox_page(None, 100).events.count(event) == 1
    return result


@pytest.mark.parametrize("failure", [False, True], ids=["commit", "fail"])
def test_unidade_starting_persiste_antes_do_ack_sem_alterar_dispatch(started_store, failure):
    adapter, (dispatch, _, _, _, command) = started_store
    starting = adapter.claim_run_dispatch_start(command)
    claimed = adapter.claim_run_unit(_claim_unit_command(
        dispatch.dispatch_id, "worker-a", MutableClock(NOW),
    ))
    assert claimed is not None
    result = _settle_unit(adapter, dispatch, claimed, failure)
    assert result.state is (RunUnitState.FAILED_FINAL if failure else RunUnitState.SUCCEEDED)
    assert result.lease_owner is result.lease_until is None
    assert result.dispatch_id == dispatch.dispatch_id
    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == starting


@pytest.mark.parametrize(
    ("update", "code"),
    [
        ({"owner": "intruder"}, "owner_mismatch"),
        ({"fencing_token": 999}, "fence_mismatch"),
        ({"dispatch_id": "0" * 16}, "dispatch_mismatch"),
    ],
)
def test_sqlite_starting_recusa_fence_divergente_sem_efeito(tmp_path, update, code):
    adapter = SQLiteControlPlane(tmp_path / "fence.sqlite", lambda: NOW)
    adapter.initialize()
    dispatch, _, _, _, command = _sqlite_seed(adapter)
    starting = adapter.claim_run_dispatch_start(command)
    claimed = adapter.claim_run_unit(_claim_unit_command(
        dispatch.dispatch_id, "worker-a", MutableClock(NOW),
    ))
    assert claimed is not None
    commit = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    before = adapter.list_run_units(dispatch.tenant_id, dispatch.run_id)
    events = adapter.read_outbox_page(None, 100).events
    with pytest.raises((FenceRejected, LeaseLost), match=code):
        adapter.commit_run_unit(commit.model_copy(update=update), _event("rejected"))
    assert adapter.list_run_units(dispatch.tenant_id, dispatch.run_id) == before
    assert adapter.read_outbox_page(None, 100).events == events
    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == starting


@pytest.fixture(params=["sqlite", "dynamodb"])
def timed_store(request, tmp_path):
    clock = MutableClock(NOW)
    if request.param == "dynamodb":
        _, client = request.getfixturevalue("ddb")
        adapter = DynamoDBControlPlane(client, _TABLE_NAME, clock.now)
        initial = seed(adapter)
    else:
        adapter = SQLiteControlPlane(tmp_path / "clock.sqlite", clock.now)
        adapter.initialize()
        initial = _sqlite_seed(adapter)
    dispatch, _, _, claim, command = initial
    assert adapter.claim_run_dispatch_start(command) is not None
    bound = adapter.bind_run_dispatch(_bind(dispatch, claim))
    unit_command = _claim_unit_command(dispatch.dispatch_id, "worker-a", clock)
    claimed = adapter.claim_run_unit(unit_command.model_copy(update={"lease_seconds": 600}))
    assert claimed is not None
    return adapter, clock, bound, claimed


@pytest.mark.parametrize("failure", [False, True], ids=["commit", "fail"])
def test_claim_duravel_preserva_settlement_com_lease_da_unidade_vigente(timed_store, failure):
    adapter, clock, dispatch, claimed = timed_store
    clock.advance(timedelta(seconds=300))
    assert dispatch.lease_until == clock.now()
    assert claimed.lease_until > clock.now()
    result = _settle_unit(adapter, dispatch, claimed, failure)
    assert result.state is (RunUnitState.FAILED_FINAL if failure else RunUnitState.SUCCEEDED)
    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == dispatch


def test_claim_duravel_nao_dispensa_lease_da_unidade(timed_store):
    adapter, clock, dispatch, claimed = timed_store
    clock.advance(timedelta(seconds=600))
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    before = adapter.list_run_units(dispatch.tenant_id, dispatch.run_id)
    events = adapter.read_outbox_page(None, 100).events
    with pytest.raises(LeaseLost, match="lease_expired"):
        adapter.commit_run_unit(command, _event("unit-expired"))
    assert adapter.list_run_units(dispatch.tenant_id, dispatch.run_id) == before
    assert adapter.read_outbox_page(None, 100).events == events


def test_claim_duravel_nao_autoriza_nova_admissao_apos_expirar_dispatch(timed_store):
    adapter, clock, dispatch, _ = timed_store
    clock.advance(timedelta(seconds=600))
    before = adapter.list_run_units(dispatch.tenant_id, dispatch.run_id)
    command = _claim_unit_command(dispatch.dispatch_id, "new-worker", clock)
    assert adapter.claim_run_unit(command) is None
    assert adapter.list_run_units(dispatch.tenant_id, dispatch.run_id) == before


def test_sqlite_starting_finalizado_nao_aceita_commit_nem_evento(tmp_path):
    adapter = SQLiteControlPlane(tmp_path / "terminal.sqlite", lambda: NOW)
    adapter.initialize()
    dispatch, _, _, claim, command = _sqlite_seed(adapter)
    adapter.claim_run_dispatch_start(command)
    unit = adapter.claim_run_unit(_claim_unit_command(
        dispatch.dispatch_id, "worker-a", MutableClock(NOW),
    ))
    assert unit is not None
    finished = adapter.finish_run_dispatch(FinishRunDispatch(
        tenant_id=dispatch.tenant_id, run_id=dispatch.run_id, dispatch_id=dispatch.dispatch_id,
        outcome=DispatchOutcome.CANCELED, finished_at=NOW, claim=claim,
    ))
    commit = _commit_command(dispatch.dispatch_id, "worker-a", unit.fencing_token)
    before = adapter.list_run_units(dispatch.tenant_id, dispatch.run_id)
    events = adapter.read_outbox_page(None, 100).events
    with pytest.raises(LeaseLost, match="dispatch_inactive"):
        adapter.commit_run_unit(commit, _event("terminal-rejected"))
    assert adapter.list_run_units(dispatch.tenant_id, dispatch.run_id) == before
    assert adapter.read_outbox_page(None, 100).events == events
    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == finished
