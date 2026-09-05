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
from cnes_infra.control_plane.dynamodb_keys import (
    dispatch_key,
    key_component,
    run_entity_key,
    unit_key,
)

if TYPE_CHECKING:
    from cnes_domain.control_plane.commands import (
        BindRunDispatch,
        FinishRunDispatch,
        ReserveRunDispatch,
    )


class DynamoDBDispatch:
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
        if dispatch.state not in {DispatchState.RESERVED, DispatchState.STARTED}:
            raise LeaseLost(ErrorCode.DISPATCH_TERMINAL)
        if dispatch.lease_until <= now:
            raise LeaseLost(ErrorCode.DISPATCH_EXPIRED)

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
        if current.state is DispatchState.TERMINAL or current.lease_until <= command.now:
            return None
        if current.wave_id != command.wave_id or current.unit_ids != command.unit_ids:
            raise Conflict(ErrorCode.ACTIVE_DISPATCH_CONFLICT)
        return current

    def _replacement_unit_items(self, dispatch: RunDispatch, now: Any) -> tuple[Item, ...]:
        items = []
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
        items = []
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
        if run_item is None or decode_model(run_item, Run).state is not RunState.PROCESSING:
            raise Conflict(ErrorCode.RUN_NOT_PROCESSING)
        item, dispatch = self._required_dispatch(command.tenant_id, command.run_id)
        self._validate_dispatch_lease(dispatch, command.dispatch_id, command.now)
        if dispatch.state is DispatchState.STARTED:
            if dispatch.execution_ref == command.execution_ref:
                return dispatch
            raise Conflict(ErrorCode.DISPATCH_BINDING_CONFLICT)
        updated = dispatch.model_copy(
            update={
                "state": DispatchState.STARTED,
                "execution_ref": command.execution_ref,
                "lease_until": command.now + timedelta(seconds=command.lease_seconds),
            }
        )
        self._transact(
            (
                check_action(self._table_name, run_item),
                put_action(self._table_name, self._dispatch_item(updated), payload(item)),
            )
        )
        return updated

    def finish_run_dispatch(self, command: FinishRunDispatch) -> RunDispatch:
        """Finaliza um dispatch ativo."""
        item, dispatch = self._required_dispatch(command.tenant_id, command.run_id)
        if dispatch.dispatch_id != command.dispatch_id:
            raise Conflict(ErrorCode.DISPATCH_ID_CONFLICT)
        if dispatch.state is DispatchState.TERMINAL:
            if dispatch.terminal_outcome is command.outcome:
                return dispatch
            raise Conflict(ErrorCode.DISPATCH_OUTCOME_CONFLICT)
        if dispatch.lease_until <= command.finished_at:
            raise Conflict(ErrorCode.DISPATCH_EXPIRED)
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

    def get_active_run_dispatch(self, tenant_id: str, run_id: str) -> RunDispatch | None:
        """Retorna o dispatch ativo do run."""
        item = self._get_item(dispatch_key(tenant_id, run_id))
        if item is None:
            return None
        dispatch = decode_model(item, RunDispatch)
        active = (
            dispatch.state is not DispatchState.TERMINAL and dispatch.lease_until > self._clock()
        )
        return dispatch if active else None
