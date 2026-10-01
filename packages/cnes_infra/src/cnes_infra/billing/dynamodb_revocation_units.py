"""Cancelamento em lotes de unidades e liquidação de Runs revogados."""

from dataclasses import dataclass, replace
from typing import Any

from cnes_domain.billing.commands import ReleaseReservationCommand
from cnes_domain.billing.errors import PermanentBillingError, RetryableBillingError
from cnes_domain.billing.execution import RunBillingState
from cnes_domain.billing.ports import ClockPort
from cnes_domain.billing.revocation import (
    REVOKED_REASON_CODE,
    CancelRunUnitsCommand,
    CancelRunUnitsResult,
)
from cnes_domain.control_plane.commands import FinalizeRunCancellation, FinishRunDispatch
from cnes_domain.control_plane.entities import OutboxEvent, Run, RunDispatch, RunUnit
from cnes_domain.control_plane.enums import (
    DispatchOutcome,
    DispatchState,
    RunState,
    RunUnitState,
)
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_domain.control_plane.transitions import transition_run_unit
from cnes_infra.billing.dynamodb_items import get_item, transact
from cnes_infra.billing.dynamodb_quota_items import (
    decode_run_billing_state,
    encode_run_billing_state,
)
from cnes_infra.billing.keys import Key, run_billing_key
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_codec import (
    Item,
    check_action,
    decode_model,
    payload,
    put_action,
    query_partition,
)
from cnes_infra.control_plane.dynamodb_keys import dispatch_key, run_entity_key, run_partition

FINALIZE_UNIT_LIMIT = 99
MAX_BATCH_UNITS = 98
_NONTERMINAL_UNITS = frozenset(
    {RunUnitState.PENDING, RunUnitState.LEASED, RunUnitState.FAILED_RETRYABLE}
)
_CONTENDED = "run_cancellation_contended"


@dataclass(frozen=True, slots=True)
class RunContext:
    companion_item: Item
    state: RunBillingState
    run_item: Item
    run: Run


@dataclass(frozen=True, slots=True)
class _StoredUnit:
    item: Item
    unit: RunUnit


def required_item(client: Any, table: str, key: Key, code: str) -> Item:
    """Lê fortemente o item pela chave base.

    Raises: PermanentBillingError: item ausente, com o código informado.
    """
    item = get_item(client, table, key, True)
    if item is None:
        raise PermanentBillingError(code)
    return item


def load_context(client: Any, table: str, tenant_id: str, run_id: str) -> RunContext:
    """Lê fortemente o companion e o Run (itens brutos e decodificados)."""
    companion = required_item(
        client, table, run_billing_key(tenant_id, run_id), "run_billing_state_missing"
    )
    stored = required_item(client, table, run_entity_key(tenant_id, run_id), "run_missing")
    return RunContext(
        companion, decode_run_billing_state(companion), stored, decode_model(stored, Run)
    )


def _canceled_event(command: CancelRunUnitsCommand, state: RunBillingState) -> OutboxEvent:
    tenant_id, run_id = command.tenant_id, command.run_id
    return OutboxEvent(
        tenant_id=tenant_id,
        event_id=f"run.canceled:{tenant_id}:{run_id}",
        event_type="run.canceled",
        aggregate_id=run_id,
        payload={
            "billing_account_id": state.billing_account_id,
            "fencing_token": state.fencing_token,
            "reason_code": REVOKED_REASON_CODE,
        },
        created_at=command.canceled_at,
        delivered_at=None,
    )


def _cancel_action(table: str, stored: _StoredUnit, run: Run) -> dict[str, Any]:
    canceled = transition_run_unit(stored.unit, RunUnitState.CANCELED, run).model_copy(
        update={"lease_owner": None, "lease_until": None}
    )
    item = {**stored.item, "payload": {"S": canceled.model_dump_json()}}
    return put_action(table, item, payload(stored.item))


class RunCancellation:
    """Cancela as unidades de um Run fenceado e liquida reserva e dispatch."""

    def __init__(
        self, client: Any, table_name: str, plane: DynamoDBControlPlane, clock: ClockPort
    ) -> None:
        self._client = client
        self._table = table_name
        self._plane = plane
        self._clock = clock

    def cancel(self, command: CancelRunUnitsCommand) -> CancelRunUnitsResult:
        """Cancela um lote de unidades ou finaliza o Run quando couber em uma transação."""
        context = load_context(self._client, self._table, command.tenant_id, command.run_id)
        fence_matches = (
            context.state.cancel_requested
            and context.state.fencing_token == command.expected_run_fencing_token
        )
        if not fence_matches:
            raise RetryableBillingError("run_fence_changed")
        if context.run.state is RunState.CANCELED:
            self._settle(context.state, command)
            return CancelRunUnitsResult((), None, True)
        if context.run.state is not RunState.CANCEL_REQUESTED:
            raise PermanentBillingError("run_not_cancel_requested")
        pending = self._pending_units(command)
        if len(pending) < FINALIZE_UNIT_LIMIT:
            return self._finalize(command, context, pending)
        return self._cancel_batch(command, context, pending)

    def _pending_units(self, command: CancelRunUnitsCommand) -> list[_StoredUnit]:
        partition = run_partition(command.tenant_id, command.run_id)
        items = query_partition(self._client, self._table, partition, "UNIT#")
        units = (_StoredUnit(item, decode_model(item, RunUnit)) for item in items)
        pending = (stored for stored in units if stored.unit.state in _NONTERMINAL_UNITS)
        return sorted(pending, key=lambda stored: stored.unit.unit_id)

    def _finalize(
        self, command: CancelRunUnitsCommand, context: RunContext, pending: list[_StoredUnit]
    ) -> CancelRunUnitsResult:
        self._settle(context.state, command)
        finalize = FinalizeRunCancellation(
            tenant_id=command.tenant_id, run_id=command.run_id,
            expected_state=RunState.CANCEL_REQUESTED, canceled_at=command.canceled_at,
        )
        try:
            self._plane.finalize_run_cancellation(finalize, _canceled_event(command, context.state))
        except Conflict as error:
            raise RetryableBillingError(_CONTENDED) from error
        return CancelRunUnitsResult(tuple(stored.unit.unit_id for stored in pending), None, True)

    def _cancel_batch(
        self, command: CancelRunUnitsCommand, context: RunContext, pending: list[_StoredUnit]
    ) -> CancelRunUnitsResult:
        after = [
            stored for stored in pending
            if command.cursor is None or stored.unit.unit_id > command.cursor
        ]
        batch = (after or pending)[: min(command.limit, MAX_BATCH_UNITS)]
        actions = (
            check_action(self._table, context.companion_item),
            check_action(self._table, context.run_item),
            *(_cancel_action(self._table, stored, context.run) for stored in batch),
        )
        if not transact(self._client, actions):
            raise RetryableBillingError(_CONTENDED)
        ids = tuple(stored.unit.unit_id for stored in batch)
        return CancelRunUnitsResult(ids, ids[-1], False)

    def _settle(self, state: RunBillingState, command: CancelRunUnitsCommand) -> None:
        self._release(state, command)
        dispatch = self._cancel_dispatch(command)
        self._mirror_dispatch(command, dispatch)

    def _release(self, state: RunBillingState, command: CancelRunUnitsCommand) -> None:
        reservation_id = state.authorization.budget_reservation_id
        if reservation_id is not None:
            self._plane.release_reservation(
                ReleaseReservationCommand(
                    state.billing_account_id, reservation_id, command.canceled_at,
                    REVOKED_REASON_CODE,
                )
            )

    def _cancel_dispatch(self, command: CancelRunUnitsCommand) -> RunDispatch | None:
        key = dispatch_key(command.tenant_id, command.run_id)
        item = get_item(self._client, self._table, key, True)
        if item is None:
            return None
        dispatch = decode_model(item, RunDispatch)
        if dispatch.state is DispatchState.TERMINAL:
            return dispatch
        finish = FinishRunDispatch(
            tenant_id=command.tenant_id, run_id=command.run_id, dispatch_id=dispatch.dispatch_id,
            outcome=DispatchOutcome.CANCELED, finished_at=command.canceled_at,
        )
        try:
            return self._plane.finish_run_dispatch(finish)
        except Conflict as error:
            if error.code != ErrorCode.DISPATCH_EXPIRED:
                raise RetryableBillingError(_CONTENDED) from error
            return dispatch

    def _mirror_dispatch(
        self, command: CancelRunUnitsCommand, dispatch: RunDispatch | None
    ) -> None:
        if dispatch is None or dispatch.state is not DispatchState.TERMINAL:
            return
        key = run_billing_key(command.tenant_id, command.run_id)
        item = required_item(self._client, self._table, key, "run_billing_state_missing")
        state = decode_run_billing_state(item)
        pending_mirror = (
            state.execution_dispatch_id == dispatch.dispatch_id
            and state.execution_status is not DispatchState.TERMINAL
        )
        if not pending_mirror:
            return
        mirrored = replace(
            state,
            execution_status=DispatchState.TERMINAL,
            execution_terminal_outcome=dispatch.terminal_outcome,
            updated_at=self._clock(),
        )
        action = put_action(self._table, encode_run_billing_state(mirrored), payload(item))
        if not transact(self._client, (action,)):
            raise RetryableBillingError(_CONTENDED)
