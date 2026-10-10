"""Contrato estático compartilhado pelos mixins do control plane DynamoDB."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime
    from typing import Any

    from pydantic import BaseModel

    from cnes_domain.billing.execution import RunBillingState
    from cnes_domain.control_plane.commands import (
        ClaimRunUnit,
        CommitRunUnit,
        FailRunUnit,
        PublishDataset,
    )
    from cnes_domain.control_plane.entities import (
        Job,
        OutboxEvent,
        RawManifestRecord,
        Run,
        RunDispatch,
        RunUnit,
    )
    from cnes_domain.control_plane.queries import LatestSucceededJobQuery
    from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations
    from cnes_infra.billing.settings import BillingSettings
    from cnes_infra.control_plane.dynamodb_billing import ClaimDeferred
    from cnes_infra.control_plane.dynamodb_codec import Action, Item

    class DynamoDBHost:
        _client: Any
        _table_name: str
        _clock: Callable[[], datetime]
        _billing: BillingSettings

        def _get_item(self, key: tuple[str, str]) -> Item | None: ...
        def _get_model[T: BaseModel](
            self, key: tuple[str, str], model_type: type[T]
        ) -> T | None: ...
        def _transact(
            self, actions: tuple[Action, ...], client_request_token: str | None = None
        ) -> None: ...
        def _job_item(self, job: Job) -> Item: ...
        def _run_item(self, run: Run) -> Item: ...
        def _unit_item(self, unit: RunUnit) -> Item: ...
        def _raw_actions(self, record: RawManifestRecord) -> tuple[Action, ...]: ...
        def _latest_job_action(
            self, job: Job, expected_head_manifest_id: str | None = None,
            resync_marker_present: bool | None = None,
        ) -> Action: ...
        def _raw_resync_marker_present(self, manifest: Any) -> bool | None: ...
        def _accepted_resync_actions(
            self, manifest: Any, marker_present: bool | None
        ) -> tuple[Action, ...]: ...
        @staticmethod
        def _job_is_claimable(job: Job, now: datetime) -> bool: ...
        @staticmethod
        def _unit_is_claimable(unit: RunUnit, now: Any) -> bool: ...
        @staticmethod
        def _validate_dispatch_lease(
            dispatch: RunDispatch, dispatch_id: str, now: Any
        ) -> None: ...
        def _event_action(self, tenant_id: str, event: OutboxEvent) -> Action: ...
        @staticmethod
        def _event_replay_matches(current: OutboxEvent | None, event: OutboxEvent) -> bool: ...
        def _get_outbox_event(self, event_id: str) -> OutboxEvent | None: ...
        def _billing_item(self, tenant_id: str, run_id: str) -> Item | None: ...
        def _quota(self) -> DynamoQuotaReservations: ...
        def _claim_billing_checks(
            self, dispatch_item: Item
        ) -> list[Action] | ClaimDeferred | None: ...
        def _claim_run_unit_once(
            self, command: ClaimRunUnit
        ) -> RunUnit | ClaimDeferred | None: ...
        def _unit_billing_checks(
            self, command: CommitRunUnit | FailRunUnit
        ) -> list[Action]: ...
        def _publication_guards(
            self, command: PublishDataset
        ) -> tuple[tuple[Action, ...], RunBillingState | None]: ...
        def _publication_billing_actions(
            self, command: PublishDataset
        ) -> tuple[Action, ...]: ...
        def get_job(self, tenant_id: str, job_id: str) -> Job | None: ...
        def get_run(self, tenant_id: str, run_id: str) -> Run | None: ...
        def query_latest_succeeded_job(
            self, query: LatestSucceededJobQuery
        ) -> Job | None: ...
else:
    DynamoDBHost = object
