"""Subscription entitlement policy as a status/action decision table."""

from datetime import datetime
from types import MappingProxyType

from cnes_domain.billing.errors import EntitlementDenied
from cnes_domain.billing.models import (
    AccessLevel,
    EntitlementAction,
    EntitlementDecision,
    EntitlementSnapshot,
    SubscriptionStatus,
)
from cnes_domain.profiles import BillingMode

WILDCARD_FEATURE = "*"
CRITICAL_ACTIONS = frozenset({
    EntitlementAction.CREATE_RUN,
    EntitlementAction.REGISTER_AGENT,
    EntitlementAction.ANALYTICS_QUERY,
    EntitlementAction.TENANT_CREATION,
    EntitlementAction.PUBLISH_RUN,
})

_STATUS_ACCESS = MappingProxyType({
    SubscriptionStatus.TRIALING: AccessLevel.FULL,
    SubscriptionStatus.ACTIVE: AccessLevel.FULL,
    SubscriptionStatus.PAST_DUE: AccessLevel.FULL,
    SubscriptionStatus.INCOMPLETE: AccessLevel.BLOCKED,
    SubscriptionStatus.INCOMPLETE_EXPIRED: AccessLevel.BLOCKED,
    SubscriptionStatus.UNPAID: AccessLevel.READ_ONLY,
    SubscriptionStatus.PAUSED: AccessLevel.READ_ONLY,
    SubscriptionStatus.CANCELED: AccessLevel.READ_ONLY,
    SubscriptionStatus.ADMIN_REVOKED: AccessLevel.BLOCKED,
})
_LEVEL_ACTIONS = MappingProxyType({
    AccessLevel.FULL: frozenset(EntitlementAction),
    AccessLevel.READ_ONLY: frozenset({EntitlementAction.SERVING_ACCESS}),
    AccessLevel.BLOCKED: frozenset[EntitlementAction](),
})
_REQUIRED_FEATURE = MappingProxyType({
    EntitlementAction.ANALYTICS_QUERY: "analytics_query",
    EntitlementAction.SERVING_ACCESS: "serving_history",
})
_QUOTA_FIELD = MappingProxyType({
    EntitlementAction.CREATE_RUN: "max_runs_per_period",
    EntitlementAction.REGISTER_AGENT: "max_agents",
    EntitlementAction.TENANT_CREATION: "max_tenants",
    EntitlementAction.ANALYTICS_QUERY: "athena_scan_budget_bytes",
    EntitlementAction.SERVING_ACCESS: "retention_days",
})
_QUOTA_GATED = frozenset({
    EntitlementAction.CREATE_RUN,
    EntitlementAction.REGISTER_AGENT,
    EntitlementAction.ANALYTICS_QUERY,
    EntitlementAction.TENANT_CREATION,
})


def require_allowed(decision: EntitlementDecision) -> EntitlementDecision:
    """Args: decision: Decisão de entitlement avaliada.
    Returns: A própria decisão quando permitida.
    Raises: EntitlementDenied: Quando a decisão nega a ação.
    """
    if not decision.allowed:
        raise EntitlementDenied(f"reason={decision.reason} action={decision.action.value}")
    return decision


def _denied(
    snapshot: EntitlementSnapshot, action: EntitlementAction, level: AccessLevel, reason: str,
) -> EntitlementDecision:
    return EntitlementDecision(
        action=action,
        allowed=False,
        access_level=level,
        reason=reason,
        entitlement_version=snapshot.entitlement_version,
        quota_limit=None,
    )


def _effective_access(snapshot: EntitlementSnapshot, now: datetime) -> tuple[AccessLevel, str]:
    status = snapshot.subscription_status
    level, reason = _STATUS_ACCESS[status], f"status_{status.value}"
    if level is not AccessLevel.FULL:
        return level, reason
    grace = snapshot.grace_until
    if status is SubscriptionStatus.PAST_DUE and (grace is None or now > grace):
        return AccessLevel.READ_ONLY, "grace_expired"
    if snapshot.cancel_at_period_end and now > snapshot.period_end:
        return AccessLevel.READ_ONLY, "period_ended"
    return level, reason


def _has_feature(snapshot: EntitlementSnapshot, feature: str, mode: BillingMode) -> bool:
    if feature in snapshot.features:
        return True
    return mode is BillingMode.DISABLED and WILDCARD_FEATURE in snapshot.features


def _quota_limit(snapshot: EntitlementSnapshot, action: EntitlementAction) -> int | None:
    field = _QUOTA_FIELD.get(action)
    return None if field is None else getattr(snapshot.quotas, field)


class EntitlementPolicy:
    def __init__(self, mode: BillingMode = BillingMode.STRIPE) -> None:
        self._mode = mode

    def evaluate(
        self, snapshot: EntitlementSnapshot, action: EntitlementAction, now: datetime,
    ) -> EntitlementDecision:
        """Args: snapshot: Entitlement vigente; action: Ação pedida; now: Instante UTC.
        Returns: Decisão com nível de acesso, motivo e limite de cota.
        """
        status = snapshot.subscription_status
        if status is SubscriptionStatus.ADMIN_REVOKED:
            return _denied(snapshot, action, AccessLevel.BLOCKED, "admin_revoked")
        if action in CRITICAL_ACTIONS and now > snapshot.valid_until:
            return _denied(snapshot, action, AccessLevel.BLOCKED, "snapshot_expired")
        level, reason = _effective_access(snapshot, now)
        if action not in _LEVEL_ACTIONS[level]:
            return _denied(snapshot, action, level, reason)
        feature = _REQUIRED_FEATURE.get(action)
        if feature is not None and not _has_feature(snapshot, feature, self._mode):
            return _denied(snapshot, action, level, "feature_missing")
        limit = _quota_limit(snapshot, action)
        if action in _QUOTA_GATED and limit == 0:
            return _denied(snapshot, action, level, "quota_not_granted")
        return EntitlementDecision(
            action=action,
            allowed=True,
            access_level=level,
            reason="allowed",
            entitlement_version=snapshot.entitlement_version,
            quota_limit=limit,
        )
