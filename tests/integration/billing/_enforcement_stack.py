"""Stack de enforcement: publicação com policy composta e revogação imediata reais."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest

from cnes_domain.billing.models import QuotaReservation
from cnes_domain.billing.publication import BillingPublicationPolicy, PublicationPolicyDependencies
from cnes_domain.billing.revocation import (
    ImmediateRevocationCommand,
    ImmediateRevocationService,
    RevocationDependencies,
    RevocationResult,
    RevocationSettings,
)
from cnes_domain.control_plane.commands import (
    CommitRunUnit,
    FailRunUnit,
    PublicationPermit,
    PublishDataset,
    TransitionRun,
)
from cnes_domain.control_plane.entities import (
    DatasetPointer,
    DatasetVersion,
    ManifestRef,
    OutboxEvent,
    Run,
)
from cnes_domain.control_plane.enums import RunState
from cnes_domain.ports.processing import CancelRunExecution
from cnes_infra.billing.audit_outbox import DynamoBillingAudit
from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection
from cnes_infra.billing.dynamodb_quota_items import decode_reservation
from cnes_infra.billing.dynamodb_revocation import DynamoRevocationStore
from cnes_infra.billing.keys import reservation_key
from cnes_infra.billing.wiring import BillingGateResources, build_entitlement_gate
from data_processor.orchestration.publisher import (
    DatasetPublisher,
    PublicationPolicy,
    PublishRequest,
    PublishResult,
)
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT
from packages.cnes_infra.tests.billing.revocation_support import (
    get_raw,
    lookup_period,
    usage_counters,
)
from tests.integration.billing._execution_stack import (
    DEPLOYMENT_LIMIT,
    RUN_ID,
    TENANT,
    Case,
    Stack,
    active_dispatch,
    claim_command,
    complete_wave,
    create_processing_run,
    resume,
)

SQLITE_DISABLED = Case("sqlite-disabled", dynamo=False, stripe=False)
DYNAMO_DISABLED = Case("dynamodb-disabled", dynamo=True, stripe=False)
DYNAMO_STRIPE = Case("dynamodb-stripe", dynamo=True, stripe=True)
MATRIX = [
    pytest.param(SQLITE_DISABLED, id=SQLITE_DISABLED.name),
    pytest.param(DYNAMO_DISABLED, id=DYNAMO_DISABLED.name),
    pytest.param(DYNAMO_STRIPE, id=DYNAMO_STRIPE.name),
]
WAVE_COUNT = 3
REVOCATION_REASON = "security_incident"


@dataclass(frozen=True, slots=True)
class RawView:
    client: Any
    table: str


@dataclass
class RevokingPolicy:
    inner: PublicationPolicy
    action: Callable[[], Any]
    seen: list[PublicationPermit] = field(default_factory=list)

    def __call__(self, run: Run) -> PublicationPermit:
        permit = self.inner(run)
        self.seen.append(permit)
        self.action()
        return permit


class FailingExecutor:
    def __init__(self) -> None:
        self.requests: list[CancelRunExecution] = []

    def cancel(self, request: CancelRunExecution) -> None:
        self.requests.append(request)
        raise RuntimeError("executor_down")


def _event(event_type: str, aggregate_id: str = RUN_ID) -> OutboxEvent:
    return OutboxEvent(
        tenant_id=TENANT, event_id=f"{event_type}:{aggregate_id}", event_type=event_type,
        aggregate_id=aggregate_id, payload={}, created_at=NOW, delivered_at=None,
    )


def drive_to_publishing(stack: Stack) -> Run:
    create_processing_run(stack)
    for _ in range(WAVE_COUNT):
        resume(stack)
        complete_wave(stack)
    run = stack.plane.get_run(TENANT, RUN_ID)
    return stack.plane.transition_run(TransitionRun(
        tenant_id=TENANT, run_id=RUN_ID, expected_state=RunState.PROCESSING,
        new_state=RunState.PUBLISHING, missing_sources=run.missing_sources,
    ), _event("run.publishing"))


def composed_policy(stack: Stack) -> BillingPublicationPolicy:
    resources = BillingGateResources(stack.clock.now, DEPLOYMENT_LIMIT, stack.client, TABLE_NAME)
    gate = build_entitlement_gate(stack.case.settings, resources)
    return BillingPublicationPolicy(PublicationPolicyDependencies(
        control_plane=stack.plane, gate=gate, clock=stack.clock.now,
        mode=stack.case.settings.execution_mode,
    ))


def pointer_of(stack: Stack) -> DatasetPointer | None:
    run = stack.plane.get_run(TENANT, RUN_ID)
    return stack.plane.get_dataset_pointer(TENANT, run.dataset_name)


def publish(stack: Stack, policy: PublicationPolicy) -> PublishResult:
    run = stack.plane.get_run(TENANT, RUN_ID)
    pointer = pointer_of(stack)
    request = PublishRequest(
        run=run, units=stack.plane.list_run_units(TENANT, RUN_ID),
        expected_version_id=None if pointer is None else pointer.version_id,
        now=stack.clock.now(),
    )
    return DatasetPublisher(stack.store, stack.plane, policy).publish(request)


def revoke(stack: Stack, executor: Any = None) -> RevocationResult:
    client, clock = stack.client, stack.clock.now
    dependencies = RevocationDependencies(
        projection=DynamoEntitlementProjection(client, TABLE_NAME, clock),
        store=DynamoRevocationStore(client, TABLE_NAME, clock),
        executor=executor or stack.executor,
        audit=DynamoBillingAudit(client, TABLE_NAME),
        clock=clock,
    )
    command = ImmediateRevocationCommand(ACCOUNT, "admin-1", REVOCATION_REASON, clock())
    return ImmediateRevocationService(dependencies, RevocationSettings()).revoke(command)


def revoker(stack: Stack, executor: Any = None) -> Callable[[], RevocationResult]:
    return lambda: revoke(stack, executor)


def reservation_of(stack: Stack) -> QuotaReservation:
    raw = RawView(stack.client, TABLE_NAME)
    reservation_id = stack.plane.get_run_billing_state(
        TENANT, RUN_ID
    ).authorization.budget_reservation_id
    key = reservation_key(ACCOUNT, lookup_period(raw, RUN_ID), reservation_id)
    return decode_reservation(get_raw(raw, key))[0]


def consumed_runs(stack: Stack) -> int:
    return usage_counters(RawView(stack.client, TABLE_NAME))["consumed_runs"]


def first_wave_claim(stack: Stack) -> tuple[Any, Any]:
    resume(stack)
    dispatch = active_dispatch(stack)
    unit = stack.plane.claim_run_unit(claim_command(stack, dispatch, dispatch.unit_ids[0]))
    return dispatch, unit


def commit_command(dispatch: Any, unit: Any) -> CommitRunUnit:
    manifest = ManifestRef(
        manifest_id="out-1", manifest_key=f"raw/{TENANT}/CNES/2026-01/out-1/manifest.json",
    )
    return CommitRunUnit(
        tenant_id=TENANT, run_id=RUN_ID, unit_id=unit.unit_id, dispatch_id=dispatch.dispatch_id,
        owner="worker-a", fencing_token=unit.fencing_token, output_manifests=(manifest,),
    )


def fail_command(dispatch: Any, unit: Any) -> FailRunUnit:
    return FailRunUnit(
        tenant_id=TENANT, run_id=RUN_ID, unit_id=unit.unit_id, dispatch_id=dispatch.dispatch_id,
        owner="worker-a", fencing_token=unit.fencing_token, error_code="processing_error",
        retryable=True,
    )


def commit_event() -> OutboxEvent:
    return _event("run_unit.completed")


def fail_event() -> OutboxEvent:
    return _event("run_unit.failed")


def direct_publish_command(stack: Stack, permit: PublicationPermit) -> PublishDataset:
    run = stack.plane.get_run(TENANT, RUN_ID)
    version = DatasetVersion(
        tenant_id=TENANT, dataset_name=run.dataset_name, version_id=RUN_ID, run_id=RUN_ID,
        run_manifest_key=f"reconciliation/{TENANT}/{run.competencia}/{RUN_ID}/run-manifest.json",
        created_at=NOW,
    )
    return PublishDataset(
        version=version, pointer_name="current", expected_version_id=None,
        final_state=RunState.PUBLISHED, missing_sources=(), publication_permit=permit,
        event=_event("reconciliation.published"),
    )
