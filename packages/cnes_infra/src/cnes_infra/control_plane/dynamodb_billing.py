"""Operações de billing do control plane DynamoDB: run sem medição, vinculação e claim."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from cnes_domain.billing.errors import PermanentBillingError, RetryableBillingError
from cnes_domain.billing.execution_policy import apply_execution_binding
from cnes_domain.control_plane.entities import OutboxEvent, RunDispatch
from cnes_domain.profiles import BillingMode
from cnes_infra.control_plane.dynamodb_codec import (
    Action,
    Item,
    check_action,
    decode_model,
    payload,
    put_action,
)
from cnes_infra.control_plane.dynamodb_keys import idempotency_key
from cnes_infra.control_plane.dynamodb_run_codec import run_dependency_actions, run_item

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.billing.commands import (
        AuthorizedRunCommand,
        ConsumeReservationCommand,
        ReleaseReservationCommand,
        ReserveRunCommand,
    )
    from cnes_domain.billing.execution import RunBillingState, RunExecutionBindingCommand
    from cnes_domain.billing.models import QuotaReservation, RunAuthorization
    from cnes_domain.control_plane.entities import IdempotencyRecord, Run
    from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations

RUN_FIXED_ACTIONS = 4
_BIND_ATTEMPTS = 2
EVENT_TYPE = "run.authorized"


@dataclass(frozen=True, slots=True)
class AuthorizedRunRecords:
    run: Run
    state: RunBillingState
    idempotency: IdempotencyRecord
    event: OutboxEvent


def authorized_run_records(command: AuthorizedRunCommand, now: datetime) -> AuthorizedRunRecords:
    """Monta Run canônico, companion, idempotência e evento de um run autorizado."""
    from cnes_infra.billing.dynamodb_items import deterministic_id
    from cnes_infra.billing.dynamodb_quota import (
        _canonical_run,
        _idempotency_record,
        _run_billing_state,
    )
    from cnes_infra.billing.dynamodb_quota_items import RUN_SCOPE

    request, authorization = command.request, command.authorization
    run = _canonical_run(request, now)
    identity = (request.tenant_id, RUN_SCOPE, request.idempotency_key)
    event = OutboxEvent(
        tenant_id=request.tenant_id,
        event_id=deterministic_id(EVENT_TYPE, request.tenant_id, request.run_id),
        event_type=EVENT_TYPE,
        aggregate_id=request.run_id,
        payload={
            "billing_account_id": request.billing_account_id,
            "plan_version_id": authorization.plan_version_id,
            "entitlement_version": authorization.entitlement_version,
            "dataset_name": request.dataset_name,
        },
        created_at=now,
        delivered_at=None,
    )
    return AuthorizedRunRecords(
        run=run,
        state=_run_billing_state(request, authorization, now),
        idempotency=_idempotency_record(identity, request.request_hash, run.run_id, now),
        event=event,
    )


# billing.keys imports control_plane.dynamodb_keys: billing imports stay lazy (cycle).
class DynamoBillingMixin:
    def _quota(self) -> DynamoQuotaReservations:
        from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations

        return DynamoQuotaReservations(self._client, self._table_name, self._clock)

    def reserve_and_create_run(self, command: ReserveRunCommand) -> RunAuthorization:
        """Reserva quota e cria o Run e o companion atomicamente."""
        return self._quota().reserve_and_create_run(command)

    def consume_reservation(self, command: ConsumeReservationCommand) -> QuotaReservation:
        """Consome a reserva de quota do run."""
        return self._quota().consume(command)

    def release_reservation(self, command: ReleaseReservationCommand) -> QuotaReservation:
        """Libera a reserva de quota do run."""
        return self._quota().release(command)

    def create_unmetered_run(self, command: AuthorizedRunCommand) -> Run:
        """Cria Run e companion sem reserva em uma transação.

        Args: command: pedido e autorização já decididas.
        Returns: Run persistido (o mesmo em replays).
        Raises: IdempotencyConflict, PermanentBillingError, RetryableBillingError.
        """
        from cnes_infra.billing.dynamodb_items import transact
        from cnes_infra.billing.dynamodb_quota_items import (
            RUN_SCOPE,
            ReplayQuery,
            any_present,
            collision_keys,
            read_replay,
        )

        request, now = command.request, self._clock()
        identity = (request.tenant_id, RUN_SCOPE, request.idempotency_key)
        query = ReplayQuery(identity, request.request_hash, now)
        replay = read_replay(self._client, self._table_name, query)
        if replay.stored is not None:
            return self._replayed_run(replay.stored, identity)
        records = authorized_run_records(command, now)
        actions = self._unmetered_actions(records, command, replay.expired)
        if transact(self._client, actions):
            return records.run
        replay = read_replay(self._client, self._table_name, query)
        if replay.stored is not None:
            return self._replayed_run(replay.stored, identity)
        collisions = collision_keys(actions, idempotency_key(*identity))
        if any_present(self._client, self._table_name, collisions):
            raise PermanentBillingError("run_conflict")
        raise RetryableBillingError("run_creation_contended")

    def _unmetered_actions(
        self, records: AuthorizedRunRecords, command: AuthorizedRunCommand, expired: Item | None
    ) -> tuple[Action, ...]:
        from cnes_infra.billing.dynamodb_items import outbox_item, put_new
        from cnes_infra.billing.dynamodb_quota_items import (
            encode_run_billing_state,
            idempotency_put,
        )

        table = self._table_name
        return (
            put_new(table, run_item(records.run)),
            *run_dependency_actions(table, records.run, RUN_FIXED_ACTIONS),
            put_new(table, encode_run_billing_state(records.state)),
            idempotency_put(table, records.idempotency, command.authorization, expired),
            put_new(table, outbox_item(records.event)),
        )

    def _replayed_run(self, stored: Item, identity: tuple[str, str, str]) -> Run:
        from cnes_infra.billing.dynamodb_items import decode_idempotency_record

        record = decode_idempotency_record(stored, identity)
        run = self.get_run(record.tenant_id, record.resource_id)
        if run is None:
            raise PermanentBillingError("run_missing_after_replay")
        return run

    def _billing_item(self, tenant_id: str, run_id: str) -> Item | None:
        from cnes_infra.billing.dynamodb_items import get_item
        from cnes_infra.billing.keys import run_billing_key

        key = run_billing_key(tenant_id, run_id)
        return get_item(self._client, self._table_name, key, True)

    def get_run_billing_state(self, tenant_id: str, run_id: str) -> RunBillingState | None:
        """Lê o companion de billing do run."""
        from cnes_infra.billing.dynamodb_quota_items import decode_run_billing_state

        item = self._billing_item(tenant_id, run_id)
        return None if item is None else decode_run_billing_state(item)

    def bind_run_execution(self, command: RunExecutionBindingCommand) -> RunBillingState:
        """Vincula a execução ao companion por compare-and-set.

        Raises: PermanentBillingError, RetryableBillingError: run_execution_contended.
        """
        from cnes_infra.billing.dynamodb_items import transact
        from cnes_infra.billing.dynamodb_quota_items import (
            decode_run_billing_state,
            encode_run_billing_state,
        )

        for _ in range(_BIND_ATTEMPTS):
            item = self._billing_item(command.tenant_id, command.run_id)
            state = None if item is None else decode_run_billing_state(item)
            updated = apply_execution_binding(state, command)
            if updated is state:
                return state
            encoded = encode_run_billing_state(updated)
            if transact(self._client, (put_action(self._table_name, encoded, payload(item)),)):
                return updated
        raise RetryableBillingError("run_execution_contended")

    def _claim_billing_checks(self, dispatch_item: Item) -> tuple[Action, ...] | None:
        if self._billing.mode is not BillingMode.STRIPE:
            return ()
        from cnes_infra.billing.dynamodb_quota_items import decode_run_billing_state
        from cnes_infra.billing.keys import run_billing_key

        dispatch = decode_model(dispatch_item, RunDispatch)
        item = self._get_item(run_billing_key(dispatch.tenant_id, dispatch.run_id))
        if item is None:
            return None if self._billing.enforced else ()
        state = decode_run_billing_state(item)
        bound = (
            state.execution_dispatch_id == dispatch.dispatch_id
            and state.execution_ref is not None
            and state.execution_ref == dispatch.execution_ref
            and state.cancel_requested is False
        )
        return (check_action(self._table_name, item),) if bound else None
