"""Cancelamento exige a mesma reserva de dispatch e semáforo no commit."""

import pytest

from cnes_domain.control_plane.enums import DispatchState, RunState
from cnes_domain.control_plane.environment_gate import ReleaseUnitPermit
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.dynamodb_environment_gate import semaphore_key
from cnes_infra.control_plane.dynamodb_keys import item_key, unit_key
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import _TABLE_NAME
from packages.cnes_infra.tests.control_plane.test_dynamodb_promotion_snapshot import (
    NOW,
    seed,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_promotion_snapshot import (
    ddb as dynamodb_fixture,
)
from packages.cnes_infra.tests.control_plane.test_environment_gate import _raise
from packages.cnes_infra.tests.control_plane.test_sqlite_dispatch_start import _bind, _cancel

ddb = dynamodb_fixture


def _reserve_semaphore(client, claim):
    client.client.update_item(
        TableName=_TABLE_NAME,
        Key=item_key(*semaphore_key("prod")),
        UpdateExpression="SET generation = :generation, request_sha256 = :request, "
        "start_state = :state",
        ExpressionAttributeValues={
            ":generation": {"N": str(claim.generation)},
            ":request": {"S": claim.request_sha256},
            ":state": {"S": "RESERVED"},
        },
    )


@pytest.mark.parametrize("operation", ["claim", "cancel"])
def test_sem_semaforo_nao_reivindica_nem_cancela_dispatch(ddb, operation):
    adapter, client = ddb
    dispatch, _, _, _, command = seed(adapter)
    client.client.delete_item(TableName=_TABLE_NAME, Key=item_key(*semaphore_key("prod")))
    result = (
        adapter.claim_run_dispatch_start(command)
        if operation == "claim"
        else adapter.cancel_reserved_run_dispatch(_cancel(command))
    )
    assert result is None
    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == dispatch


def test_cancelamento_exige_binding_completo_do_semaforo(ddb):
    adapter, _ = ddb
    dispatch, _, permit, _, command = seed(adapter)
    assert adapter.cancel_reserved_run_dispatch(_cancel(command)) is None
    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == dispatch
    assert adapter.observe_environment_gate("prod").permit == permit


def test_cancelamento_persiste_prova_e_libera_somente_a_reserva_correspondente(ddb):
    adapter, client = ddb
    dispatch, _, permit, claim, command = seed(adapter)
    _reserve_semaphore(client, claim)
    canceled = adapter.cancel_reserved_run_dispatch(_cancel(command))
    assert canceled is not None
    assert canceled.state is DispatchState.TERMINAL
    assert canceled.canceled_before_send
    assert canceled.start_claim == claim
    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == canceled
    assert adapter.claim_run_dispatch_start(command) is None
    proof = b'{"resolution":"CANCEL_BEFORE_SEND","reason":"cancelado"}\n'
    assert adapter.release_unit_permit(
        ReleaseUnitPermit(
            "prod",
            permit.token,
            NOW,
            NOW,
            claim.generation,
            "CANCEL_BEFORE_SEND",
            proof,
        )
    )
    state = adapter.observe_environment_gate("prod")
    assert state.permit is None
    assert state.settlement == proof


def test_cancelamento_perdedor_do_cas_preserva_reserva(ddb):
    adapter, client = ddb
    dispatch, _, _, claim, command = seed(adapter)
    _reserve_semaphore(client, claim)
    client.inject(
        "transact_write_items",
        _raise(
            "TransactionCanceledException",
            "ConditionalCheckFailed",
            "None",
        ),
    )
    assert adapter.cancel_reserved_run_dispatch(_cancel(command)) is None
    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == dispatch
    assert adapter.observe_environment_gate("prod").permit is not None
    assert adapter.observe_environment_gate("prod").settlement is None


def test_bind_tardio_recusa_unidade_ausente(ddb):
    adapter, client = ddb
    dispatch, _, _, claim, command = seed(adapter)
    assert adapter.claim_run_dispatch_start(command) is not None
    run = adapter.get_run(dispatch.tenant_id, dispatch.run_id)
    assert run is not None
    adapter.put_run(run.model_copy(update={"state": RunState.PUBLISHED}))
    client.client.delete_item(
        TableName=_TABLE_NAME,
        Key=item_key(*unit_key(dispatch.tenant_id, dispatch.run_id, dispatch.unit_ids[0])),
    )

    with pytest.raises(Conflict, match="run_not_processing"):
        adapter.bind_run_dispatch(_bind(dispatch, claim))
