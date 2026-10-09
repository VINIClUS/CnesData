"""Ambiente e fakes compartilhados pelos testes do worker de billing."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol, cast
from unittest.mock import Mock

from billing_worker.worker import BillingWorker, WorkerJobs
from cnes_domain.billing.inbox import (
    ReconciliationResult,
    RecoveryRequest,
    RecoveryResult,
    ReservationRecoveryResult,
)
from cnes_domain.billing.models import BillingMetric
from cnes_infra.billing.revocation_sweep import RevocationSweepResult

REGION = "sa-east-1"
TABLE = "control-plane"
SECRET_KEY = "sk_test_SEGREDO"  # noqa: S105
WEBHOOK_SECRET = "whsec_SEGREDO"  # noqa: S105
ORIGIN = "https://app.example.com"
STATE_MACHINE = "arn:aws:states:sa-east-1:1:stateMachine:processor"
NOW = datetime(2026, 10, 1, 12, tzinfo=UTC)
STRIPE_ENV = {
    "PROFILE": "aws",
    "BILLING_MODE": "stripe",
    "AWS_REGION": REGION,
    "AWS_CONTROL_PLANE_TABLE": TABLE,
    "DYNAMODB_ENDPOINT_URL": "  ",
    "STRIPE_SECRET_KEY_SECRET_ARN": "arn:aws:secretsmanager:sa-east-1:1:secret:sk",
    "STRIPE_WEBHOOK_SECRET_SECRET_ARN": "arn:aws:secretsmanager:sa-east-1:1:secret:wh",
    "BILLING_RETURN_ORIGINS": ORIGIN,
    "BILLING_SUCCESS_URL": f"{ORIGIN}/ok",
    "BILLING_CANCEL_URL": f"{ORIGIN}/cancel",
    "BILLING_PORTAL_RETURN_URL": f"{ORIGIN}/portal",
}
RESULT = RecoveryResult(3, 1, 2, 0, "evt_9")
RECONCILED = ReconciliationResult(4, 1, 1, 0, None)
SWEPT = RevocationSweepResult(2, 1, 3, 1, 0, None)
RELEASED = ReservationRecoveryResult(5, 2, None)


class Metrics:
    def __init__(self) -> None:
        self.emitted: list[BillingMetric] = []

    def emit(self, metric: BillingMetric) -> None:
        self.emitted.append(metric)

    def values(self, name: str) -> list[float]:
        return [metric.value for metric in self.emitted if metric.name == name]


class MockJobs(Protocol):
    recovery: Mock
    request: RecoveryRequest
    reconciler: Mock
    revocations: Mock
    reservations: Mock
    metrics: Metrics
    clock: Callable[[], datetime]


def env(**overrides: str) -> dict[str, str]:
    return {**STRIPE_ENV, **overrides}


def without(*keys: str) -> dict[str, str]:
    return {k: v for k, v in STRIPE_ENV.items() if k not in keys}


def make_jobs(**overrides: object) -> WorkerJobs:
    recovery = Mock()
    recovery.drain_inbox.return_value = RESULT
    recovery.run.return_value = RESULT
    reconciler = Mock()
    reconciler.run.return_value = RECONCILED
    revocations = Mock()
    revocations.run.return_value = SWEPT
    reservations = Mock()
    reservations.reconcile_expired_reservations.return_value = RELEASED
    values: dict[str, Any] = {
        "recovery": recovery,
        "request": RecoveryRequest(72, 100),
        "reconciler": reconciler,
        "revocations": revocations,
        "reservations": reservations,
        "metrics": Metrics(),
        "clock": lambda: NOW,
    }
    values.update(overrides)
    return WorkerJobs(**values)


def make_worker(**overrides: object) -> tuple[BillingWorker, MockJobs]:
    jobs = make_jobs(**overrides)
    return BillingWorker(jobs), cast("MockJobs", jobs)
