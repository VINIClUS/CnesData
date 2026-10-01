"""Falha atômica de Run em PUBLISHING cuja publicação foi negada pela revogação."""

from typing import Any

from cnes_domain.billing.errors import RetryableBillingError
from cnes_domain.billing.ports import ClockPort
from cnes_domain.billing.revocation import FailDeniedPublicationCommand
from cnes_domain.control_plane.entities import OutboxEvent
from cnes_domain.control_plane.enums import RunState
from cnes_domain.control_plane.transitions import transition_run
from cnes_infra.billing.dynamodb_items import outbox_item, put_new, transact
from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations
from cnes_infra.billing.dynamodb_revocation_units import RunContext, load_context
from cnes_infra.control_plane.dynamodb_codec import Action, check_action, payload, put_action
from cnes_infra.control_plane.dynamodb_run_codec import run_item

_STALE = "run_revocation_stale"


def _require_event(command: FailDeniedPublicationCommand, event: OutboxEvent) -> None:
    matches = (
        event.tenant_id == command.tenant_id
        and event.aggregate_id == command.run_id
        and event.delivered_at is None
    )
    if not matches:
        raise ValueError("reason=publication_event_mismatch")


def _eligible(context: RunContext, command: FailDeniedPublicationCommand) -> bool:
    return (
        context.run.state is RunState.PUBLISHING
        and not context.state.cancel_requested
        and context.state.fencing_token == command.expected_fencing_token
    )


class PublicationDenial:
    """Move o Run PUBLISHING para FAILED e libera a reserva em uma transação."""

    def __init__(self, client: Any, table_name: str, clock: ClockPort) -> None:
        self._client = client
        self._table = table_name
        self._quota = DynamoQuotaReservations(client, table_name, clock)

    def fail(self, command: FailDeniedPublicationCommand, event: OutboxEvent) -> bool:
        """Falha o Run com publicação negada.

        Args: Comando com fence esperado e o evento de outbox.
        Returns: True se gravou; False se nada havia a fazer ou outro vencedor atuou.
        Raises: ValueError, PermanentBillingError, RetryableBillingError.
        """
        _require_event(command, event)
        context = self._load(command)
        if not _eligible(context, command):
            return False
        if transact(self._client, self._actions(context, command, event)):
            return True
        return self._lost_race(command)

    def _load(self, command: FailDeniedPublicationCommand) -> RunContext:
        return load_context(self._client, self._table, command.tenant_id, command.run_id)

    def _actions(
        self, context: RunContext, command: FailDeniedPublicationCommand, event: OutboxEvent
    ) -> tuple[Action, ...]:
        failed = transition_run(context.run, RunState.FAILED)
        return (
            put_action(self._table, run_item(failed), payload(context.run_item)),
            check_action(self._table, context.companion_item),
            put_new(self._table, outbox_item(event)),
            *self._release(context, command),
        )

    def _release(
        self, context: RunContext, command: FailDeniedPublicationCommand
    ) -> tuple[Action, ...]:
        reservation_id = context.state.authorization.budget_reservation_id
        if reservation_id is None:
            return ()
        return self._quota.release_reserved_actions(
            context.state.billing_account_id, reservation_id, command.failed_at,
            command.reason_code,
        )

    def _lost_race(self, command: FailDeniedPublicationCommand) -> bool:
        if self._load(command).run.state is RunState.PUBLISHING:
            raise RetryableBillingError(_STALE)
        return False
