"""Operações de billing do control plane SQLite: run sem medição e vinculação."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from cnes_domain.billing.errors import (
    BillingDisabledError,
    IdempotencyConflict,
    PermanentBillingError,
)
from cnes_domain.billing.execution_policy import apply_execution_binding
from cnes_domain.control_plane.entities import IdempotencyRecord
from cnes_infra.billing.disabled import DisabledQuotaReservations
from cnes_infra.control_plane.dynamodb_billing import authorized_run_records
from cnes_infra.control_plane.sqlite_schema import deserialize_model, serialize_model

if TYPE_CHECKING:
    import sqlite3
    from datetime import datetime

    from cnes_domain.billing.commands import (
        AuthorizedRunCommand,
        ConsumeReservationCommand,
        ReleaseReservationCommand,
        ReserveRunCommand,
    )
    from cnes_domain.billing.execution import RunBillingState, RunExecutionBindingCommand
    from cnes_domain.billing.models import QuotaReservation, RunAuthorization
    from cnes_domain.control_plane.entities import Run


_SELECT_STATE = "SELECT data FROM run_billing_states WHERE tenant_id = ? AND run_id = ?"


def _select_state(
    connection: sqlite3.Connection, tenant_id: str, run_id: str
) -> RunBillingState | None:
    from cnes_infra.billing.dynamodb_quota_items import decode_run_billing_state

    row = connection.execute(_SELECT_STATE, (tenant_id, run_id)).fetchone()
    return None if row is None else decode_run_billing_state(json.loads(row[0]))


def _dump_state(state: RunBillingState) -> str:
    from cnes_infra.billing.dynamodb_quota_items import encode_run_billing_state

    return json.dumps(encode_run_billing_state(state))


class SQLiteBillingMixin:
    def reserve_and_create_run(self, command: ReserveRunCommand) -> RunAuthorization:
        """Indisponível: o SQLite opera apenas com billing desabilitado."""
        raise BillingDisabledError("billing_mode=disabled operation=reserve_and_create_run")

    def consume_reservation(self, command: ConsumeReservationCommand) -> QuotaReservation:
        """Devolve a reserva local sem medição como consumida."""
        return DisabledQuotaReservations(self._clock).consume(command)

    def release_reservation(self, command: ReleaseReservationCommand) -> QuotaReservation:
        """Devolve a reserva local sem medição como liberada."""
        return DisabledQuotaReservations(self._clock).release(command)

    def create_unmetered_run(self, command: AuthorizedRunCommand) -> Run:
        """Cria Run, companion, idempotência e evento em uma transação.

        Args: command: pedido e autorização já decididas.
        Returns: Run persistido (o mesmo em replays).
        Raises: IdempotencyConflict, PermanentBillingError.
        """
        request, now = command.request, self._clock()
        records = authorized_run_records(command, now)
        with self.write_transaction() as connection:
            replayed = self._replayed_unmetered_run(connection, command, now)
            if replayed is not None:
                return replayed
            if self.get_run_record(connection, request.tenant_id, request.run_id) is not None:
                raise PermanentBillingError("run_conflict")
            self.put_run_record(connection, records.run)
            connection.execute(
                "INSERT INTO run_billing_states (tenant_id, run_id, data) VALUES (?, ?, ?)",
                (
                    request.tenant_id,
                    request.run_id,
                    _dump_state(records.state),
                ),
            )
            record = records.idempotency
            connection.execute(
                "INSERT INTO idempotency_records (tenant_id, scope, key, data) "
                "VALUES (?, ?, ?, ?) ON CONFLICT (tenant_id, scope, key) "
                "DO UPDATE SET data = excluded.data",
                (record.tenant_id, record.scope, record.key, serialize_model(record)),
            )
            self.put_outbox_event(connection, records.event, request.tenant_id)
            return records.run

    def _replayed_unmetered_run(
        self, connection: sqlite3.Connection, command: AuthorizedRunCommand, now: datetime
    ) -> Run | None:
        from cnes_infra.billing.dynamodb_quota_items import RUN_SCOPE

        request = command.request
        row = connection.execute(
            "SELECT data FROM idempotency_records WHERE tenant_id = ? AND scope = ? AND key = ?",
            (request.tenant_id, RUN_SCOPE, request.idempotency_key),
        ).fetchone()
        current = None if row is None else deserialize_model(row[0], IdempotencyRecord)
        if current is None or current.expires_at <= now:
            return None
        if current.request_hash != request.request_hash:
            raise IdempotencyConflict(f"key={request.idempotency_key}")
        run = self.get_run_record(connection, request.tenant_id, request.run_id)
        if run is None:
            raise PermanentBillingError("run_missing_after_replay")
        return run

    def get_run_billing_state(self, tenant_id: str, run_id: str) -> RunBillingState | None:
        """Lê o companion de billing do run."""
        with self.read_connection() as connection:
            return _select_state(connection, tenant_id, run_id)

    def bind_run_execution(self, command: RunExecutionBindingCommand) -> RunBillingState:
        """Vincula a execução ao companion dentro de uma transação.

        Raises: PermanentBillingError: Estado ausente, divergente ou obsoleto.
        """
        with self.write_transaction() as connection:
            state = _select_state(connection, command.tenant_id, command.run_id)
            updated = apply_execution_binding(state, command)
            if updated is not state:
                connection.execute(
                    "UPDATE run_billing_states SET data = ? WHERE tenant_id = ? AND run_id = ?",
                    (
                        _dump_state(updated),
                        command.tenant_id,
                        command.run_id,
                    ),
                )
            return updated
