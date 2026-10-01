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
from cnes_infra.control_plane.dynamodb_keys import item_key

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
        version, permit = command.version, command.publication_permit
        mode = self._billing.execution_mode
        item = self._billing_item(version.tenant_id, version.run_id)
        state = _decode_companion(item)
        require_publication_companion(state, permit, mode)
        if item is None:
            return ()
        actions = [check_action(self._table_name, item)]
        if isinstance(permit.binding_context, PublicationGuard):
            actions.append(self._snapshot_check(permit.binding_context))
        reservation_id = state.authorization.budget_reservation_id
        if mode is BillingMode.STRIPE and reservation_id is not None:
            actions.extend(
                self._quota().consume_reserved_actions(
                    state.billing_account_id, reservation_id, self._clock()
                )
            )
        return tuple(actions)

    def _snapshot_check(self, guard: PublicationGuard) -> Action:
        from cnes_infra.billing.dynamodb_items import decode_snapshot, get_item
        from cnes_infra.billing.keys import entitlement_snapshot_key

        key = entitlement_snapshot_key(guard.billing_account_id)
        item = get_item(self._client, self._table_name, key, True)
        snapshot = None if item is None else decode_snapshot(item, guard.billing_account_id)
        require_publication_snapshot(snapshot, guard)
        return {
            "ConditionCheck": {
                "TableName": self._table_name,
                "Key": item_key(*key),
                "ConditionExpression": "entitlement_version = :version",
                "ExpressionAttributeValues": {
                    ":version": {"N": str(guard.expected_entitlement_version)}
                },
            }
        }
