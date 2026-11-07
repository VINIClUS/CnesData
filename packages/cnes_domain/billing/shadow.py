"""Observador de shadow que audita as negações hipotéticas dos gates de API."""

import hashlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Protocol

from cnes_domain.billing.errors import BillingDependencyError, BillingError
from cnes_domain.billing.models import (
    AccessLevel,
    BillingAccountTenantLink,
    BillingAuditEvent,
    BillingMetric,
    CapacityKind,
    EntitlementAction,
    EntitlementDecision,
    ReadConsistency,
)
from cnes_domain.billing.policy import EntitlementPolicy
from cnes_domain.billing.ports import (
    BillingAuditPort,
    BillingMetricsPort,
    ClockPort,
    EntitlementProjectionPort,
)
from cnes_domain.profiles import BillingMode

logger = logging.getLogger(__name__)

SHADOW_DENIED_EVENT = "entitlement.shadow_denied"
SHADOW_OBSERVER_ACTOR = "system:shadow_observer"
SHADOW_DENIALS_METRIC = "ShadowEntitlementDenials"
SHADOW_FAILURES_METRIC = "ShadowObserverFailures"
BILLING_ACCOUNT_MISSING = "billing_account_missing"
_UNEXPECTED = "unexpected"
_HOUR_BUCKET = "%Y%m%d%H"
_CAPACITY_KINDS = MappingProxyType({
    EntitlementAction.REGISTER_AGENT: CapacityKind.AGENT,
    EntitlementAction.TENANT_CREATION: CapacityKind.TENANT,
})

RetentionAnchor = Callable[[], datetime | None]


class TenantAccountReader(Protocol):
    def get_tenant_account(
        self, tenant_id: str, consistency: ReadConsistency,
    ) -> BillingAccountTenantLink | None: ...


class ShadowCatalogReader(TenantAccountReader, Protocol):
    def get_tenant_link(
        self, billing_account_id: str, tenant_id: str, consistency: ReadConsistency,
    ) -> BillingAccountTenantLink | None: ...


class CapacityCounterReader(Protocol):
    def get_capacity_count(self, billing_account_id: str, kind: CapacityKind) -> int | None: ...


def linked_billing_account(reader: TenantAccountReader, tenant_id: str) -> str | None:
    """Args: reader: Catálogo de links; tenant_id: Tenant autenticado.
    Returns: Conta do link reverso lido com consistência forte; None se ausente ou divergente.
    """
    link = reader.get_tenant_account(tenant_id, ReadConsistency.STRONG)
    if link is None or link.tenant_id != tenant_id:
        return None
    return link.billing_account_id


def shadow_bucket_id(
    tenant_id: str, action: EntitlementAction, reason: str, at: datetime,
) -> str:
    """Returns: Id estável de 32 hex por tenant, ação, motivo e hora UTC."""
    parts = [tenant_id, action.value, reason, at.strftime(_HOUR_BUCKET)]
    encoded = json.dumps(parts, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()[:32]


@dataclass(frozen=True, slots=True)
class ShadowObservation:
    action: EntitlementAction
    tenant_id: str
    billing_account_id: str | None = None
    retention_anchor: RetentionAnchor | None = None


class ShadowObserver(Protocol):
    def observe(self, observation: ShadowObservation) -> None: ...


class NullShadowObserver:
    def observe(self, observation: ShadowObservation) -> None:
        """Args: observation: Ignorada fora de stripe+shadow."""
        del observation


NULL_SHADOW_OBSERVER = NullShadowObserver()


@dataclass(frozen=True, slots=True)
class ShadowDenial:
    reason: str
    billing_account_id: str | None = None
    limit: int | None = None
    used: int | None = None


@dataclass(frozen=True, slots=True)
class ShadowObserverDependencies:
    accounts: ShadowCatalogReader
    projection: EntitlementProjectionPort
    capacity: CapacityCounterReader
    audit: BillingAuditPort
    clock: ClockPort
    metrics: BillingMetricsPort | None = None


@dataclass(frozen=True, slots=True)
class _Allowed:
    observation: ShadowObservation
    account: str
    decision: EntitlementDecision
    now: datetime


def _failure_code(error: Exception) -> str:
    return error.code if isinstance(error, BillingError) else _UNEXPECTED


def _attributes(
    observation: ShadowObservation, denial: ShadowDenial,
) -> dict[str, str | int | bool | None]:
    attributes: dict[str, str | int | bool | None] = {
        "action": observation.action.value,
        "reason": denial.reason,
        "tenant_id": observation.tenant_id,
    }
    optional = {
        "billing_account_id": denial.billing_account_id,
        "limit": denial.limit,
        "used": denial.used,
    }
    attributes |= {name: value for name, value in optional.items() if value is not None}
    return attributes


class ShadowEntitlementObserver:
    """Avalia o que o enforce decidiria e audita a negação sem nunca bloquear nem levantar."""

    def __init__(self, dependencies: ShadowObserverDependencies) -> None:
        self._deps = dependencies
        self._policy = EntitlementPolicy(BillingMode.STRIPE)

    def observe(self, observation: ShadowObservation) -> None:
        """Args: observation: Ação já permitida pela decisão real.
        Falhas de dependência viram log e métrica; nunca auditoria de negação.
        """
        try:
            now = self._deps.clock()
            denial = self._evaluate(observation, now)
            if denial is not None:
                self._record(observation, denial, now)
        except Exception as error:
            self._failed(observation.action, error)

    def _evaluate(self, observation: ShadowObservation, now: datetime) -> ShadowDenial | None:
        account = observation.billing_account_id or linked_billing_account(
            self._deps.accounts, observation.tenant_id,
        )
        if account is None:
            return ShadowDenial(BILLING_ACCOUNT_MISSING)
        snapshot = self._deps.projection.get_snapshot(account, ReadConsistency.STRONG)
        if snapshot is None:
            return ShadowDenial("snapshot_missing", account)
        if snapshot.billing_account_id != account:
            return ShadowDenial("snapshot_account_mismatch", account)
        decision = self._policy.evaluate(snapshot, observation.action, now)
        if not decision.allowed:
            return ShadowDenial(decision.reason, account)
        allowed = _Allowed(observation, account, decision, now)
        return self._capacity(allowed) or self._retention(allowed)

    def _capacity(self, allowed: _Allowed) -> ShadowDenial | None:
        kind = _CAPACITY_KINDS.get(allowed.observation.action)
        if kind is None or self._replayed_tenant(allowed):
            return None
        limit = allowed.decision.quota_limit
        used = self._deps.capacity.get_capacity_count(allowed.account, kind)
        if used is None:
            return ShadowDenial("capacity_not_seeded", allowed.account, limit)
        if limit is None or used < limit:
            return None
        return ShadowDenial(f"max_{kind.value}s_exceeded", allowed.account, limit, used)

    def _replayed_tenant(self, allowed: _Allowed) -> bool:
        observation = allowed.observation
        if observation.action is not EntitlementAction.TENANT_CREATION:
            return False
        link = self._deps.accounts.get_tenant_link(
            allowed.account, observation.tenant_id, ReadConsistency.STRONG,
        )
        return link is not None

    def _retention(self, allowed: _Allowed) -> ShadowDenial | None:
        anchor = allowed.observation.retention_anchor
        retention_days = allowed.decision.quota_limit
        read_only = allowed.decision.access_level is AccessLevel.READ_ONLY
        if anchor is None or not read_only or retention_days is None:
            return None
        created_at = anchor()
        if created_at is None:
            raise BillingDependencyError("serving_version_unavailable")
        if created_at >= allowed.now - timedelta(days=retention_days):
            return None
        return ShadowDenial("retention_expired", allowed.account, retention_days)

    def _record(self, observation: ShadowObservation, denial: ShadowDenial, now: datetime) -> None:
        action = observation.action
        logger.warning("billing_shadow_denied action=%s reason=%s", action.value, denial.reason)
        bucket = shadow_bucket_id(observation.tenant_id, action, denial.reason, now)
        self._deps.audit.append(BillingAuditEvent(
            event_id=f"{SHADOW_DENIED_EVENT}:{bucket}",
            event_type=SHADOW_DENIED_EVENT,
            aggregate_id=denial.billing_account_id or observation.tenant_id,
            actor_id=SHADOW_OBSERVER_ACTOR,
            reason_code=denial.reason,
            occurred_at=now,
            attributes=_attributes(observation, denial),
        ))
        self._emit(action, SHADOW_DENIALS_METRIC, {"Reason": denial.reason})

    def _failed(self, action: EntitlementAction, error: Exception) -> None:
        code = _failure_code(error)
        logger.warning("billing_shadow_observer_failed action=%s code=%s", action.value, code)
        self._emit(action, SHADOW_FAILURES_METRIC, {})

    def _emit(self, action: EntitlementAction, name: str, dimensions: dict[str, str]) -> None:
        metrics = self._deps.metrics
        if metrics is None:
            return
        try:
            metrics.emit(BillingMetric(name, 1.0, "Count", dimensions, self._deps.clock()))
        except Exception:
            logger.warning("billing_shadow_metric_failed action=%s", action.value)
