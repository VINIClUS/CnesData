"""Mapeamento puro do estado atual da Stripe para snapshot de entitlement."""

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any

from cnes_domain.billing.commands import StripeBillingState
from cnes_domain.billing.models import EntitlementSnapshot, PlanVersion, SubscriptionStatus

STRIPE_SNAPSHOT_VALIDITY_MARGIN_HOURS = 72
COMPARED_FIELDS = (
    "subscription_status",
    "stripe_subscription_id",
    "plan_version_id",
    "features",
    "quotas",
    "period_start",
    "period_end",
    "cancel_at_period_end",
    "grace_until",
)


@dataclass(frozen=True, slots=True)
class SnapshotMappingInput:
    billing_account_id: str
    state: StripeBillingState
    plan: PlanVersion
    current: EntitlementSnapshot | None
    now: datetime


def mapped_status(inputs: SnapshotMappingInput) -> SubscriptionStatus:
    """Status do snapshot: ADMIN_REVOKED atual é sticky, senão o da Stripe.

    Args: Entrada de mapeamento.
    Returns: Status de assinatura resultante.
    """
    current = inputs.current
    if current is not None and current.subscription_status is SubscriptionStatus.ADMIN_REVOKED:
        return SubscriptionStatus.ADMIN_REVOKED
    return inputs.state.subscription_status


def _grace_until(inputs: SnapshotMappingInput, status: SubscriptionStatus) -> datetime | None:
    if status is not SubscriptionStatus.PAST_DUE:
        return None
    current = inputs.current
    if (
        current is not None
        and current.subscription_status is SubscriptionStatus.PAST_DUE
        and current.stripe_subscription_id == inputs.state.stripe_subscription_id
        and current.grace_until is not None
    ):
        return max(current.grace_until, inputs.state.period_start)
    return inputs.state.period_start + timedelta(days=inputs.plan.grace_period_days)


def _valid_until(inputs: SnapshotMappingInput, grace_until: datetime | None) -> datetime:
    period_end = inputs.state.period_end
    latest = max(inputs.now, period_end, grace_until or period_end)
    return latest + timedelta(hours=STRIPE_SNAPSHOT_VALIDITY_MARGIN_HOURS)


def map_snapshot(
    inputs: SnapshotMappingInput, version: int, source_event_id: str
) -> EntitlementSnapshot:
    """Mapeia o estado da Stripe para o snapshot de entitlement.

    Args: Entrada de mapeamento, versão do entitlement e id do evento de origem.
    Returns: Snapshot com grace e validade calculados.
    """
    state = inputs.state
    status = mapped_status(inputs)
    grace_until = _grace_until(inputs, status)
    return EntitlementSnapshot(
        billing_account_id=inputs.billing_account_id,
        stripe_subscription_id=state.stripe_subscription_id,
        subscription_status=status,
        cancel_at_period_end=state.cancel_at_period_end,
        plan_version_id=inputs.plan.plan_version_id,
        features=state.active_features,
        quotas=inputs.plan.quotas,
        period_start=state.period_start,
        period_end=state.period_end,
        grace_until=grace_until,
        valid_until=_valid_until(inputs, grace_until),
        entitlement_version=version,
        updated_at=inputs.now,
        source_event_id=source_event_id,
    )


def canonical_fields(snapshot: EntitlementSnapshot) -> dict[str, Any]:
    """Args: Snapshot de entitlement.
    Returns: Campos comparados em forma canônica serializável.
    """
    grace = snapshot.grace_until
    return {
        "subscription_status": snapshot.subscription_status.value,
        "stripe_subscription_id": snapshot.stripe_subscription_id,
        "plan_version_id": snapshot.plan_version_id,
        "features": sorted(snapshot.features),
        "quotas": asdict(snapshot.quotas),
        "period_start": snapshot.period_start.isoformat(),
        "period_end": snapshot.period_end.isoformat(),
        "cancel_at_period_end": snapshot.cancel_at_period_end,
        "grace_until": None if grace is None else grace.isoformat(),
    }


def changed_fields(current: EntitlementSnapshot, desired: EntitlementSnapshot) -> tuple[str, ...]:
    """Args: Snapshot atual e snapshot desejado.
    Returns: Nomes dos campos comparados cujo valor canônico difere.
    """
    before, after = canonical_fields(current), canonical_fields(desired)
    return tuple(name for name in COMPARED_FIELDS if before[name] != after[name])
