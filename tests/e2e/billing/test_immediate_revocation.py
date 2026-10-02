"""E2E da revogação administrativa imediata que a Stripe não consegue desfazer."""

from typing import Any

import pytest

from cnes_domain.billing.errors import EntitlementDenied, PublishDenied
from cnes_domain.billing.models import (
    AccessLevel,
    BillingEnforcementMode,
    EntitlementAction,
    EntitlementSnapshot,
    RunAuthorization,
    SubscriptionStatus,
)
from cnes_domain.billing.revocation import (
    ImmediateRevocationCommand,
    ImmediateRevocationService,
    RevocationDependencies,
    RevocationResult,
    RevocationSettings,
)
from cnes_domain.control_plane.commands import CommitRunUnit
from cnes_domain.control_plane.entities import ManifestRef, RunDispatch, RunUnit
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost
from cnes_domain.ports.processing import CancelRunExecution
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.settings import BillingSettings
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT, TENANT
from packages.cnes_infra.tests.billing.revocation_support import (
    RUN_ID,
    claim_unit,
    event_of,
    make_unit,
    move_to_processing,
    put_units,
    start_wave,
)
from packages.cnes_infra.tests.billing.test_dynamodb_publication_fence import (
    guard,
    publish_command,
)

pytestmark = [pytest.mark.stripe]

ADMIN_ID = "admin-1"
REASON_CODE = "fraud_confirmed"
DATASET = "cnes_vinculos"
STALE_COMMIT_ERRORS = (FenceRejected, LeaseLost, Conflict)
STRIPE_ENFORCING = BillingSettings(BillingMode.STRIPE, BillingEnforcementMode.ENFORCE, 0)


class FenceObservingExecutor:
    def __init__(self, store: Any) -> None:
        self._store = store
        self.observed: list[tuple[int, bool]] = []

    def cancel(self, request: CancelRunExecution) -> None:
        state = self._store.get_run_billing_state(request.tenant_id, request.run_id)
        self.observed.append((state.fencing_token, state.cancel_requested))


def _is_active(snapshot: EntitlementSnapshot) -> bool:
    return snapshot.subscription_status is SubscriptionStatus.ACTIVE


def _is_revoked(snapshot: EntitlementSnapshot) -> bool:
    return snapshot.subscription_status is SubscriptionStatus.ADMIN_REVOKED


def _claim_first_unit(runtime: Any) -> tuple[RunDispatch, RunUnit]:
    env = runtime.env
    move_to_processing(env)
    put_units(env, (make_unit("unit-a"), make_unit("unit-b")))
    dispatch = start_wave(env, ("unit-a",), None)
    return dispatch, claim_unit(env, dispatch, "unit-a")


def _revoke(runtime: Any, executor: FenceObservingExecutor) -> RevocationResult:
    dependencies = RevocationDependencies(
        projection=runtime.projection,
        store=runtime.env.store,
        executor=executor,
        audit=runtime.audit,
        clock=runtime.clock.now,
    )
    service = ImmediateRevocationService(dependencies, RevocationSettings())
    command = ImmediateRevocationCommand(ACCOUNT, ADMIN_ID, REASON_CODE, runtime.clock.now())
    return service.revoke(command)


def _stale_commit(dispatch: RunDispatch, unit: RunUnit) -> CommitRunUnit:
    output = ManifestRef(
        manifest_id="out-unit-a",
        manifest_key=f"raw/{TENANT}/CNES/2026-08/out-unit-a/manifest.json",
    )
    return CommitRunUnit(
        tenant_id=TENANT, run_id=RUN_ID, unit_id=unit.unit_id, dispatch_id=dispatch.dispatch_id,
        owner="worker-a", fencing_token=unit.fencing_token, output_manifests=(output,),
    )


def _assert_commit_rejected(runtime: Any, dispatch: RunDispatch, unit: RunUnit) -> None:
    event = event_of("unit.completed.unit-a")
    with pytest.raises(STALE_COMMIT_ERRORS):
        runtime.env.plane.commit_run_unit(_stale_commit(dispatch, unit), event)


def _assert_publish_rejected(runtime: Any, auth: RunAuthorization, stale: int) -> None:
    plane = DynamoDBControlPlane(
        runtime.client, runtime.table, runtime.clock.now, billing=STRIPE_ENFORCING
    )
    permit = guard(
        expected_entitlement_version=auth.entitlement_version,
        expected_run_fencing_token=stale,
        checked_at=runtime.clock.now(),
    )
    with pytest.raises((PublishDenied, Conflict)):
        plane.publish_dataset(publish_command(permit, token=stale))
    assert plane.get_dataset_pointer(TENANT, DATASET) is None


def _assert_blocked(runtime: Any) -> None:
    decision = runtime.decide(EntitlementAction.CREATE_RUN)
    assert decision.allowed is False
    assert decision.access_level is AccessLevel.BLOCKED
    assert decision.reason == "admin_revoked"
    with pytest.raises(EntitlementDenied, match="reason=admin_revoked"):
        runtime.create_run("run-02")


def _assert_revocation_audit(runtime: Any, revoked_version: int) -> None:
    revoked = runtime.outbox_events("entitlement.revoked")
    assert len(revoked) == 1
    assert revoked[0].payload["reason_code"] == REASON_CODE
    assert revoked[0].payload["attributes"]["entitlement_version"] == revoked_version
    canceled = runtime.outbox_events("run.cancel_requested")
    assert [event.aggregate_id for event in canceled] == [RUN_ID]


def _assert_stripe_cannot_undo(runtime: Any, revoked_version: int) -> None:
    runtime.cancel_now()
    runtime.await_inbox_event("customer.subscription.deleted")
    snapshot = runtime.snapshot()
    assert snapshot.subscription_status is SubscriptionStatus.ADMIN_REVOKED
    assert snapshot.entitlement_version >= revoked_version


def test_admin_revocation_invalida_fence_imediatamente(clock_runtime: Any) -> None:
    runtime = clock_runtime
    runtime.subscribe()
    runtime.await_snapshot(_is_active, "active")
    auth = runtime.create_run(RUN_ID)
    dispatch, unit = _claim_first_unit(runtime)
    stale = runtime.env.store.get_run_billing_state(TENANT, RUN_ID).fencing_token
    before = runtime.snapshot()
    executor = FenceObservingExecutor(runtime.env.store)

    result = _revoke(runtime, executor)

    assert result.fenced_run_ids == (RUN_ID,)
    assert result.cancel_failures == ()
    assert executor.observed == [(stale + 1, True)]
    revoked = runtime.snapshot()
    assert _is_revoked(revoked)
    assert revoked.entitlement_version == before.entitlement_version + 1
    assert revoked.entitlement_version == result.entitlement_version
    _assert_blocked(runtime)
    _assert_commit_rejected(runtime, dispatch, unit)
    _assert_publish_rejected(runtime, auth, stale)
    _assert_revocation_audit(runtime, revoked.entitlement_version)
    _assert_stripe_cannot_undo(runtime, revoked.entitlement_version)
