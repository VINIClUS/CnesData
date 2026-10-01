"""Fences de billing em commit/fail de unidade e na publicação do control plane DynamoDB."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cnes_domain.billing.execution import PublicationGuard
from cnes_domain.billing.publication import (
    require_publication_companion,
    require_publication_snapshot,
    unit_companion_allows,
)
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_domain.control_plane.errors import FenceRejected
from cnes_domain.profiles import BillingMode
from cnes_infra.control_plane.dynamodb_codec import Action, check_action

if TYPE_CHECKING:
    from cnes_domain.billing.execution import RunBillingState
    from cnes_domain.control_plane.commands import CommitRunUnit, FailRunUnit, PublishDataset


def _decode_companion(item: dict | None) -> RunBillingState | None:
    from cnes_infra.billing.dynamodb_quota_items import decode_run_billing_state

    return None if item is None else decode_run_billing_state(item)


class DynamoBillingFencesMixin:
    def _unit_billing_checks(self, command: CommitRunUnit | FailRunUnit) -> list[Action]:
        item = self._billing_item(command.tenant_id, command.run_id)
        state = _decode_companion(item)
        if not unit_companion_allows(state, command.dispatch_id, self._billing.execution_mode):
            raise FenceRejected(ErrorCode.DISPATCH_FENCE_REJECTED)
        return [] if item is None else [check_action(self._table_name, item)]

    def _publication_billing_actions(self, command: PublishDataset) -> tuple[Action, ...]:
        guards, state = self._publication_guards(command)
        if state is None or self._billing.execution_mode is not BillingMode.STRIPE:
            return guards
        reservation_id = state.authorization.budget_reservation_id
        if reservation_id is None:
            return guards
        settlement = self._quota().consume_reserved_actions(
            state.billing_account_id, reservation_id, self._clock()
        )
        return (*guards, *settlement)

    def _publication_guards(
        self, command: PublishDataset
    ) -> tuple[tuple[Action, ...], RunBillingState | None]:
        version, permit = command.version, command.publication_permit
        item = self._billing_item(version.tenant_id, version.run_id)
        state = _decode_companion(item)
        require_publication_companion(state, permit, self._billing.execution_mode)
        if item is None:
            return (), None
        guards = [check_action(self._table_name, item)]
        if isinstance(permit.binding_context, PublicationGuard):
            guards.append(self._snapshot_check(permit.binding_context))
        return tuple(guards), state

    def _snapshot_check(self, guard: PublicationGuard) -> Action:
        from cnes_infra.billing.dynamodb_items import decode_snapshot, get_item
        from cnes_infra.billing.dynamodb_quota_items import SnapshotExpectation, snapshot_check
        from cnes_infra.billing.keys import entitlement_snapshot_key

        account, now = guard.billing_account_id, self._clock()
        item = get_item(self._client, self._table_name, entitlement_snapshot_key(account), True)
        snapshot = None if item is None else decode_snapshot(item, account)
        require_publication_snapshot(snapshot, guard, now)
        expected = SnapshotExpectation(account, guard.expected_entitlement_version, None)
        return snapshot_check(self._table_name, expected, now)
