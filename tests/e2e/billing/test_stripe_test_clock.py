"""E2E do ciclo de vida de assinatura Stripe com Test Clock sobre DynamoDB Local."""

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

import pytest

from cnes_domain.billing.errors import EntitlementDenied
from cnes_domain.billing.models import (
    AccessLevel,
    EntitlementAction,
    EntitlementDecision,
    EntitlementSnapshot,
    SubscriptionStatus,
)

pytestmark = [pytest.mark.stripe]

GRACE_DAYS = 7
RENEWAL_MARGIN = timedelta(hours=2)
TRIAL_DAYS = 14
PLAN_VERSION_ID = "plan_e2e"
SECOND = timedelta(seconds=1)
CREATE_RUN = EntitlementAction.CREATE_RUN

Predicate = Callable[[EntitlementSnapshot], bool]


def _is(status: SubscriptionStatus) -> Predicate:
    return lambda snapshot: snapshot.subscription_status is status


def _active_from(period_start: datetime) -> Predicate:
    return lambda snapshot: (
        snapshot.subscription_status is SubscriptionStatus.ACTIVE
        and snapshot.period_start == period_start
    )


def _scheduled_cancel(snapshot: EntitlementSnapshot) -> bool:
    return snapshot.cancel_at_period_end and (
        snapshot.subscription_status is SubscriptionStatus.ACTIVE
    )


def _versions(runtime: Any, event_type: str) -> list[int]:
    return sorted(item["entitlement_version"] for item in _attributes(runtime, event_type))


def _attributes(runtime: Any, event_type: str) -> list[dict[str, Any]]:
    return [event.payload["attributes"] for event in runtime.outbox_events(event_type)]


def _status_changes(runtime: Any, to_status: str) -> list[dict[str, Any]]:
    attributes = _attributes(runtime, "subscription.status_changed")
    return [item for item in attributes if item["subscription_status"] == to_status]


def _status_change(runtime: Any, to_status: str) -> dict[str, Any]:
    changes = _status_changes(runtime, to_status)
    assert len(changes) == 1
    return changes[0]


def _assert_one_change_per_version(runtime: Any, version: int) -> None:
    assert _versions(runtime, "entitlement.changed") == list(range(1, version + 1))


def _assert_decision(
    decision: EntitlementDecision, level: AccessLevel, allowed: bool, reason: str | None = None
) -> None:
    assert decision.allowed is allowed
    assert decision.access_level is level
    if reason is not None:
        assert decision.reason == reason


def _assert_denied(runtime: Any, action: EntitlementAction, reason: str) -> None:
    _assert_decision(runtime.decide(action), AccessLevel.READ_ONLY, False, reason)


def _assert_trial_snapshot(runtime: Any, trial: EntitlementSnapshot) -> None:
    assert trial.plan_version_id == PLAN_VERSION_ID
    assert trial.period_end - trial.period_start == timedelta(days=TRIAL_DAYS)
    assert (trial.period_start, trial.period_end) == runtime.period()
    assert trial.grace_until is None


def _assert_trial_access(runtime: Any, trial: EntitlementSnapshot) -> None:
    decision = runtime.decide(CREATE_RUN)
    _assert_decision(decision, AccessLevel.FULL, True)
    assert decision.entitlement_version == trial.entitlement_version
    authorization = runtime.create_run("run-trial")
    assert authorization.plan_version_id == PLAN_VERSION_ID
    assert authorization.entitlement_version == trial.entitlement_version
    assert runtime.run_period("run-trial") == trial.period_start


def _assert_trial_audit(runtime: Any, trial: EntitlementSnapshot) -> None:
    _assert_one_change_per_version(runtime, trial.entitlement_version)
    started = _status_change(runtime, "trialing")
    assert started["previous_status"] is None


def _assert_trial_converts(runtime: Any, trial: EntitlementSnapshot) -> None:
    runtime.advance_to(trial.period_end + RENEWAL_MARGIN)
    active = runtime.await_snapshot(_active_from(trial.period_end), "trial_to_active")
    assert active.entitlement_version > trial.entitlement_version
    converted = _status_change(runtime, "active")
    assert converted["previous_status"] == "trialing"


def test_trial_concede_plano_de_trial(clock_runtime: Any) -> None:
    runtime = clock_runtime
    runtime.subscribe(trial_days=TRIAL_DAYS)
    trial = runtime.await_snapshot(_is(SubscriptionStatus.TRIALING), "trialing")

    _assert_trial_snapshot(runtime, trial)
    _assert_trial_access(runtime, trial)
    _assert_trial_audit(runtime, trial)
    _assert_trial_converts(runtime, trial)


def _first_active(runtime: Any) -> EntitlementSnapshot:
    runtime.subscribe()
    return runtime.await_snapshot(_is(SubscriptionStatus.ACTIVE), "active")


def _assert_renewal_isolated(
    runtime: Any, first: EntitlementSnapshot, first_usage: dict[str, int]
) -> EntitlementSnapshot:
    runtime.advance_to(first.period_end + RENEWAL_MARGIN)
    renewed = runtime.await_snapshot(_active_from(first.period_end), "renewed")
    assert renewed.entitlement_version > first.entitlement_version
    assert runtime.usage(first.period_start) == first_usage
    return renewed


def _assert_new_period_usage(
    runtime: Any, renewed: EntitlementSnapshot, first_usage: dict[str, int]
) -> None:
    authorization = runtime.create_run("run-02")
    assert runtime.run_period("run-02") == renewed.period_start
    assert runtime.usage(renewed.period_start) == first_usage
    replay = runtime.create_run("run-02")
    assert replay.budget_reservation_id == authorization.budget_reservation_id
    assert runtime.usage(renewed.period_start) == first_usage


def _assert_status_never_left_active(runtime: Any, first: EntitlementSnapshot) -> None:
    versions = _versions(runtime, "subscription.status_changed")
    assert all(version <= first.entitlement_version for version in versions)


def test_renewal_move_periodo_sem_duplicar_quota(clock_runtime: Any) -> None:
    runtime = clock_runtime
    first = _first_active(runtime)
    runtime.create_run("run-01")
    first_usage = runtime.usage(first.period_start)

    renewed = _assert_renewal_isolated(runtime, first, first_usage)
    _assert_new_period_usage(runtime, renewed, first_usage)

    _assert_one_change_per_version(runtime, renewed.entitlement_version)
    _assert_status_never_left_active(runtime, first)


def _assert_past_due_snapshot(past_due: EntitlementSnapshot, first: EntitlementSnapshot) -> None:
    assert past_due.period_start == first.period_end
    assert past_due.grace_until == past_due.period_start + timedelta(days=GRACE_DAYS)


def _assert_grace_access(runtime: Any) -> None:
    _assert_decision(runtime.decide(CREATE_RUN), AccessLevel.FULL, True)
    runtime.create_run("run-grace")
    entered = _status_change(runtime, "past_due")
    assert entered["previous_status"] == "active"


def _assert_grace_expired(runtime: Any, past_due: EntitlementSnapshot) -> None:
    assert past_due.grace_until is not None
    runtime.set_app_time(past_due.grace_until + SECOND)
    _assert_denied(runtime, CREATE_RUN, "grace_expired")
    with pytest.raises(EntitlementDenied, match="reason=grace_expired"):
        runtime.create_run("run-late")
    _assert_denied(runtime, EntitlementAction.PUBLISH_RUN, "grace_expired")


def test_payment_failure_aplica_grace_e_depois_read_only(clock_runtime: Any) -> None:
    runtime = clock_runtime
    first = _first_active(runtime)
    runtime.fail_future_payments()
    runtime.advance_to(first.period_end + RENEWAL_MARGIN)
    past_due = runtime.await_snapshot(_is(SubscriptionStatus.PAST_DUE), "past_due")

    _assert_past_due_snapshot(past_due, first)
    _assert_grace_access(runtime)
    _assert_grace_expired(runtime, past_due)


def _assert_access_until_period_end(runtime: Any, scheduled: EntitlementSnapshot) -> None:
    runtime.set_app_time(scheduled.period_end - SECOND)
    _assert_decision(runtime.decide(CREATE_RUN), AccessLevel.FULL, True)
    runtime.create_run("run-before-end")
    runtime.set_app_time(scheduled.period_end + SECOND)
    _assert_denied(runtime, CREATE_RUN, "period_ended")


def _assert_canceled_after_period_end(runtime: Any, scheduled: EntitlementSnapshot) -> None:
    runtime.advance_to(scheduled.period_end + RENEWAL_MARGIN)
    runtime.await_snapshot(_is(SubscriptionStatus.CANCELED), "canceled")
    _assert_denied(runtime, CREATE_RUN, "status_canceled")
    assert _status_change(runtime, "canceled")["previous_status"] == "active"


def test_cancel_at_period_end_preserva_acesso_ate_period_end(clock_runtime: Any) -> None:
    runtime = clock_runtime
    first = _first_active(runtime)
    runtime.cancel_at_period_end()
    scheduled = runtime.await_snapshot(_scheduled_cancel, "cancel_scheduled")

    assert scheduled.entitlement_version > first.entitlement_version
    _assert_access_until_period_end(runtime, scheduled)
    _assert_canceled_after_period_end(runtime, scheduled)
