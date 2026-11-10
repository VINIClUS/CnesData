"""Transações e validação do ciclo de dispatch no DynamoDB."""

from __future__ import annotations

from datetime import timedelta
from hashlib import sha256
from typing import TYPE_CHECKING, Any

from cnes_domain.control_plane.entities import Run, RunDispatch, RunUnit
from cnes_domain.control_plane.enums import DispatchState, RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost, NotFound
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_infra.control_plane.dynamodb_codec import (
    Item,
    check_action,
    decode_model,
    encode_model,
    payload,
    put_action,
)
from cnes_infra.control_plane.dynamodb_host import DynamoDBHost
from cnes_infra.control_plane.dynamodb_keys import (
    dispatch_key,
    key_component,
    run_entity_key,
    unit_key,
)

if TYPE_CHECKING:
    from cnes_domain.control_plane.commands import (
        BindRunDispatch,
        CancelReservedRunDispatch,
        ClaimRunDispatchStart,
        FinishRunDispatch,
        ReserveRunDispatch,
    )


class DynamoDBDispatch(DynamoDBHost):
    """Reserva, vincula e finaliza gerações de dispatch."""

    def _dispatch_item(self, dispatch: RunDispatch) -> Item:
        attributes = {
            "gsi5pk": (
                f"RUN_ITEMS#{key_component(dispatch.tenant_id)}#{key_component(dispatch.run_id)}"
            ),
            "gsi5sk": "DISPATCH#ACTIVE",
        }
        key = dispatch_key(dispatch.tenant_id, dispatch.run_id)
        return encode_model(dispatch, "RUNDISPATCH", key, attributes)

    @staticmethod
    def _validate_dispatch_lease(dispatch: RunDispatch, dispatch_id: str, now: Any) -> None:
        if dispatch.dispatch_id != dispatch_id:
            raise FenceRejected(ErrorCode.DISPATCH_FENCE_REJECTED)
        if dispatch.state not in {
            DispatchState.RESERVED,
            DispatchState.STARTING,
            DispatchState.STARTED,
        }:
            raise LeaseLost(ErrorCode.DISPATCH_TERMINAL)
        if dispatch.lease_until <= now and dispatch.start_claim is None:
            raise LeaseLost(ErrorCode.DISPATCH_EXPIRED)

    def _late_bind_units(
        self, run: Run, dispatch: RunDispatch, command: BindRunDispatch
    ) -> tuple[Item, ...] | None:
        late_states = {
            RunState.PUBLISHED,
            RunState.PUBLISHED_DEGRADED,
            RunState.FAILED,
            RunState.CANCELED,
        }
        completed_states = {
            RunUnitState.SUCCEEDED,
            RunUnitState.SUCCEEDED_DEGRADED,
            RunUnitState.FAILED_FINAL,
        }
        if run.state not in late_states or dispatch.start_claim != command.claim:
            return None
        items: list[Item] = []
        for unit_id in dispatch.unit_ids:
            unit_item = self._get_item(unit_key(run.tenant_id, run.run_id, unit_id))
            if unit_item is None:
                return None
            unit = decode_model(unit_item, RunUnit)
            if unit.dispatch_id != dispatch.dispatch_id or unit.state not in completed_states:
                return None
            items.append(unit_item)
        return tuple(items) if items else None

    def reserve_run_dispatch(self, command: ReserveRunDispatch) -> RunDispatch:
        """Reserva uma geração de dispatch do run."""
        run_item = self._get_item(run_entity_key(command.tenant_id, command.run_id))
        if run_item is None or decode_model(run_item, Run).state is not RunState.PROCESSING:
            raise Conflict(ErrorCode.RUN_NOT_PROCESSING)
        current_item = self._get_item(dispatch_key(command.tenant_id, command.run_id))
        current = decode_model(current_item, RunDispatch) if current_item else None
        replay = self._dispatch_replay(current, command)
        if replay is not None:
            return replay
        prior_unit_items = ()
        if current is not None:
            prior_unit_items = self._replacement_unit_items(current, command.now)
        unit_items = self._dispatch_unit_items(command)
        generation = 1 if current is None else current.generation + 1
        raw_id = f"{command.tenant_id}\x1f{command.run_id}\x1f{command.wave_id}\x1f{generation}"
        dispatch = RunDispatch(
            tenant_id=command.tenant_id,
            run_id=command.run_id,
            wave_id=command.wave_id,
            dispatch_id=sha256(raw_id.encode()).hexdigest()[:16],
            generation=generation,
            unit_ids=command.unit_ids,
            state=DispatchState.RESERVED,
            lease_until=command.now + timedelta(seconds=command.lease_seconds),
        )
        expected = payload(current_item) if current_item is not None else None
        checked_items = {
            (item["pk"]["S"], item["sk"]["S"]): item for item in (*prior_unit_items, *unit_items)
        }
        actions = [check_action(self._table_name, run_item)]
        actions.extend(check_action(self._table_name, item) for item in checked_items.values())
        actions.append(put_action(self._table_name, self._dispatch_item(dispatch), expected))
        self._transact(tuple(actions))
        return dispatch

    @staticmethod
    def _dispatch_replay(
        current: RunDispatch | None, command: ReserveRunDispatch
    ) -> RunDispatch | None:
        if current is None:
            return None
        if current.state is not DispatchState.TERMINAL and current.start_claim is not None:
            return current
        if current.state is DispatchState.TERMINAL or current.lease_until <= command.now:
            return None
        if current.wave_id != command.wave_id or current.unit_ids != command.unit_ids:
            raise Conflict(ErrorCode.ACTIVE_DISPATCH_CONFLICT)
        return current

    def _replacement_unit_items(self, dispatch: RunDispatch, now: Any) -> tuple[Item, ...]:
        items: list[Item] = []
        for unit_id in dispatch.unit_ids:
            item = self._get_item(unit_key(dispatch.tenant_id, dispatch.run_id, unit_id))
            if item is None:
                raise Conflict(ErrorCode.DISPATCH_UNIT_MISSING)
            unit = decode_model(item, RunUnit)
            live = (
                unit.state is RunUnitState.LEASED
                and unit.lease_until is not None
                and unit.lease_until > now
            )
            if live:
                raise Conflict(ErrorCode.DISPATCH_UNIT_UNAVAILABLE)
            items.append(item)
        return tuple(items)

    def _dispatch_unit_items(self, command: ReserveRunDispatch) -> tuple[Item, ...]:
        items: list[Item] = []
        for unit_id in command.unit_ids:
            item = self._get_item(unit_key(command.tenant_id, command.run_id, unit_id))
            if item is None:
                raise Conflict(ErrorCode.DISPATCH_UNIT_MISSING)
            unit = decode_model(item, RunUnit)
            if not self._unit_is_claimable(unit, command.now):
                raise Conflict(ErrorCode.DISPATCH_UNIT_UNAVAILABLE)
            items.append(item)
        return tuple(items)

    def bind_run_dispatch(self, command: BindRunDispatch) -> RunDispatch:
        """Vincula o dispatch a uma execução externa."""
        run_item = self._get_item(run_entity_key(command.tenant_id, command.run_id))
        if run_item is None:
            raise Conflict(ErrorCode.RUN_NOT_PROCESSING)
        run = decode_model(run_item, Run)
        if run.state is not RunState.PROCESSING and run.state not in {
            RunState.PUBLISHED,
            RunState.PUBLISHED_DEGRADED,
            RunState.FAILED,
            RunState.CANCELED,
        }:
            raise Conflict(ErrorCode.RUN_NOT_PROCESSING)
        item, dispatch = self._required_dispatch(command.tenant_id, command.run_id)
        self._validate_dispatch_lease(dispatch, command.dispatch_id, command.now)
        if dispatch.state in {DispatchState.STARTING, DispatchState.STARTED}:
            if (
                dispatch.execution_ref == command.execution_ref
                and dispatch.start_claim == command.claim
            ):
                return dispatch
            if dispatch.state is DispatchState.STARTING and command.claim == dispatch.start_claim:
                pass
            else:
                raise Conflict(ErrorCode.DISPATCH_BINDING_CONFLICT)
        if dispatch.start_claim != command.claim:
            raise Conflict(ErrorCode.DISPATCH_BINDING_CONFLICT)
        late_units = self._late_bind_units(run, dispatch, command)
        if run.state is not RunState.PROCESSING and late_units is None:
            raise Conflict(ErrorCode.RUN_NOT_PROCESSING)
        updated = dispatch.model_copy(
            update={
                "state": DispatchState.STARTED,
                "execution_ref": command.execution_ref,
                "lease_until": command.now + timedelta(seconds=command.lease_seconds),
            }
        )
        checked = (run_item, *(() if late_units is None else late_units))
        actions = [check_action(self._table_name, checked_item) for checked_item in checked]
        actions.append(put_action(self._table_name, self._dispatch_item(updated), payload(item)))
        self._transact(tuple(actions))
        return updated

    def claim_run_dispatch_start(self, command: ClaimRunDispatchStart) -> RunDispatch | None:
        from cnes_infra.control_plane.dynamodb_dispatch_start import claim_start

        return claim_start(self, command)

    def cancel_reserved_run_dispatch(
        self, command: CancelReservedRunDispatch
    ) -> RunDispatch | None:
        from cnes_infra.control_plane.dynamodb_dispatch_start import cancel_reserved

        return cancel_reserved(self, command)

    def finish_run_dispatch(self, command: FinishRunDispatch) -> RunDispatch:
        """Finaliza um dispatch ativo."""
        item, dispatch = self._required_dispatch(command.tenant_id, command.run_id)
        if dispatch.dispatch_id != command.dispatch_id:
            raise Conflict(ErrorCode.DISPATCH_ID_CONFLICT)
        if dispatch.state is DispatchState.TERMINAL:
            if dispatch.terminal_outcome is command.outcome:
                return dispatch
            raise Conflict(ErrorCode.DISPATCH_OUTCOME_CONFLICT)
        if dispatch.lease_until <= command.finished_at and dispatch.start_claim is None:
            raise Conflict(ErrorCode.DISPATCH_EXPIRED)
        if dispatch.state is DispatchState.STARTING and command.claim != dispatch.start_claim:
            raise Conflict(ErrorCode.DISPATCH_BINDING_CONFLICT)
        if command.claim != dispatch.start_claim:
            raise Conflict(ErrorCode.DISPATCH_BINDING_CONFLICT)
        updated = dispatch.model_copy(
            update={
                "state": DispatchState.TERMINAL,
                "terminal_outcome": command.outcome,
            }
        )
        self._transact((put_action(self._table_name, self._dispatch_item(updated), payload(item)),))
        return updated

    def _required_dispatch(self, tenant_id: str, run_id: str) -> tuple[Item, RunDispatch]:
        item = self._get_item(dispatch_key(tenant_id, run_id))
        if item is None:
            raise NotFound(ErrorCode.DISPATCH_MISSING)
        return item, decode_model(item, RunDispatch)

    def get_run_dispatch(self, tenant_id: str, run_id: str) -> RunDispatch | None:
        """Lê a geração persistida, inclusive depois da liquidação."""
        item = self._get_item(dispatch_key(tenant_id, run_id))
        return decode_model(item, RunDispatch) if item is not None else None

    def get_active_run_dispatch(self, tenant_id: str, run_id: str) -> RunDispatch | None:
        """Retorna o dispatch ativo do run."""
        item = self._get_item(dispatch_key(tenant_id, run_id))
        if item is None:
            return None
        dispatch = decode_model(item, RunDispatch)
        active = (
            dispatch.start_claim is not None and dispatch.state is not DispatchState.TERMINAL
        ) or (dispatch.state is not DispatchState.TERMINAL and dispatch.lease_until > self._clock())
        return dispatch if active else None
