"""Métricas de billing em CloudWatch EMF emitidas pelo logging JSON."""

import logging
import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Any

from cnes_domain.billing.models import BillingMetric, SubscriptionStatus
from cnes_domain.billing.ports import BillingMetricsPort

BILLING_METRICS_NAMESPACE = "CnesData/Billing"
ALLOWED_DIMENSIONS = frozenset({"Environment", "EventType", "Reason", "SubscriptionStatus"})

_ENVIRONMENT = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MILLISECOND = timedelta(milliseconds=1)
_VALUE_PATTERNS = {
    "EventType": re.compile(r"^[a-z][a-z0-9_.]{0,127}$"),
    "Reason": re.compile(r"^[a-z][a-z0-9_]{0,63}$"),
}
_STATUSES = frozenset(status.value for status in SubscriptionStatus)


class BillingMetricName(StrEnum):
    WEBHOOK_LATENCY_MS = "WebhookLatencyMs"
    WEBHOOK_FAILURES = "WebhookFailures"
    WEBHOOK_DUPLICATES = "WebhookDuplicates"
    RECOVERY_BACKLOG = "RecoveryBacklog"
    RECONCILIATION_DRIFT = "ReconciliationDrift"
    ENTITLEMENT_CHECKS_DENIED = "EntitlementChecksDenied"
    QUOTA_RESERVATIONS_ACTIVE = "QuotaReservationsActive"
    QUOTA_RESERVATIONS_EXPIRED = "QuotaReservationsExpired"
    RUNS_CANCELED_BY_REVOCATION = "RunsCanceledByRevocation"
    ENTITLEMENT_SNAPSHOT_AGE_SECONDS = "EntitlementSnapshotAgeSeconds"
    AUDIT_OUTBOX_FAILURES = "AuditOutboxFailures"


_NAMES = MappingProxyType({name.value: name for name in BillingMetricName})
_UNIT_OVERRIDES = {
    BillingMetricName.WEBHOOK_LATENCY_MS: "Milliseconds",
    BillingMetricName.ENTITLEMENT_SNAPSHOT_AGE_SECONDS: "Seconds",
}
METRIC_UNITS: Mapping[BillingMetricName, str] = MappingProxyType(
    dict.fromkeys(BillingMetricName, "Count") | _UNIT_OVERRIDES
)


def billing_metric(
    name: BillingMetricName,
    value: float,
    occurred_at: datetime,
    dimensions: Mapping[str, str] | None = None,
) -> BillingMetric:
    """Cria uma métrica de billing com a unidade do catálogo.

    Args: Nome, valor finito, instante UTC e dimensões opcionais.
    Returns: BillingMetric pronta para ``emit``.
    """
    return BillingMetric(
        name=name.value,
        value=value,
        unit=METRIC_UNITS[name],
        dimensions=dict(dimensions or {}),
        occurred_at=occurred_at,
    )


def _dimension_value_valid(key: str, value: str) -> bool:
    if key == "SubscriptionStatus":
        return value in _STATUSES
    return _VALUE_PATTERNS[key].match(value) is not None


def _rejection_reason(metric: BillingMetric) -> str | None:
    known = _NAMES.get(metric.name)
    if known is None:
        return "metric_unknown"
    if metric.unit != METRIC_UNITS[known]:
        return "unit_mismatch"
    if not set(metric.dimensions) <= ALLOWED_DIMENSIONS - {"Environment"}:
        return "dimension_not_allowed"
    if not all(_dimension_value_valid(k, v) for k, v in metric.dimensions.items()):
        return "dimension_value_invalid"
    return None


class CloudWatchBillingMetrics:
    """Sink de métricas que emite documentos EMF pelo logging JSON."""

    def __init__(self, environment: str, logger: logging.Logger | None = None) -> None:
        if _ENVIRONMENT.match(environment) is None:
            raise ValueError("reason=environment_invalid")
        self._environment = environment
        self._logger = logger or logging.getLogger("cnes_infra.billing.metrics")

    def emit(self, metric: BillingMetric) -> None:
        """Emite a métrica como EMF; métricas inválidas são descartadas com aviso."""
        reason = _rejection_reason(metric)
        if reason is not None:
            self._logger.warning("billing_metric_rejected reason=%s", reason)
            return
        self._logger.info("billing_metric", extra=self._document(metric))

    def _document(self, metric: BillingMetric) -> dict[str, Any]:
        dimensions = {"Environment": self._environment, **metric.dimensions}
        directive = {
            "Namespace": BILLING_METRICS_NAMESPACE,
            "Dimensions": [sorted(dimensions)],
            "Metrics": [{"Name": metric.name, "Unit": metric.unit}],
        }
        timestamp = (metric.occurred_at - _EPOCH) // _MILLISECOND
        return {
            "_aws": {"Timestamp": timestamp, "CloudWatchMetrics": [directive]},
            **dimensions,
            metric.name: metric.value,
        }


class DiscardBillingMetrics:
    """Sink de métricas que descarta todas as emissões."""

    def emit(self, metric: BillingMetric) -> None:
        """Args: metric: Métrica ignorada."""


def build_billing_metrics(environment: str | None) -> BillingMetricsPort:
    """Args: environment: Ambiente EMF ou None.
    Returns: Sink CloudWatch quando há ambiente; senão descarte.
    """
    if environment is None:
        return DiscardBillingMetrics()
    return CloudWatchBillingMetrics(environment)
