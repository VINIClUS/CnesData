"""Enforcers de perda de acesso: porta, modo shadow e seleção por modo."""

import logging
from collections.abc import Callable
from typing import Protocol, runtime_checkable

from cnes_domain.billing.models import (
    BillingAuditEvent,
    BillingEnforcementMode,
    EntitlementSnapshot,
)
from cnes_domain.billing.ports import BillingAuditPort, ClockPort
from cnes_domain.billing.revocation import RevocationResult
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.dynamodb_items import deterministic_id
from cnes_infra.billing.settings import BillingSettings

__all__ = [
    "SHADOW_ACCESS_LOSS_EVENT",
    "AccessLossEnforcerPort",
    "ShadowAccessLossEnforcer",
    "select_access_loss_enforcer",
]

SHADOW_ACCESS_LOSS_EVENT = "entitlement.shadow_access_loss"
_SHADOW_REASON = "shadow_access_loss"

logger = logging.getLogger(__name__)


@runtime_checkable
class AccessLossEnforcerPort(Protocol):
    def enforce_access_loss(
        self, snapshot: EntitlementSnapshot, actor_id: str
    ) -> RevocationResult:
        raise NotImplementedError

    def resume_pending(self, billing_account_id: str, actor_id: str) -> RevocationResult | None:
        raise NotImplementedError


class ShadowAccessLossEnforcer:
    """Registra a perda de acesso em auditoria sem fencing de runs."""

    def __init__(self, audit: BillingAuditPort, clock: ClockPort) -> None:
        self._audit = audit
        self._clock = clock

    def enforce_access_loss(
        self, snapshot: EntitlementSnapshot, actor_id: str
    ) -> RevocationResult:
        """Audita a perda de acesso sem revogar runs.

        Args: Snapshot que perdeu acesso e ator da decisão.
        Returns: Resultado sem runs fenced.
        """
        account_id = snapshot.billing_account_id
        version = snapshot.entitlement_version
        self._audit.append(BillingAuditEvent(
            event_id=deterministic_id(SHADOW_ACCESS_LOSS_EVENT, account_id, str(version)),
            event_type=SHADOW_ACCESS_LOSS_EVENT,
            aggregate_id=account_id,
            actor_id=actor_id,
            reason_code=_SHADOW_REASON,
            occurred_at=self._clock(),
            attributes={
                "entitlement_version": version,
                "subscription_status": snapshot.subscription_status.value,
            },
        ))
        logger.info(
            "billing_shadow_access_loss billing_account_id=%s entitlement_version=%d",
            account_id, version,
        )
        return RevocationResult(version, (), ())

    def resume_pending(self, billing_account_id: str, actor_id: str) -> RevocationResult | None:
        """Args: Conta e ator.
        Returns: Sempre None, pois o modo shadow não mantém revogações pendentes.
        """
        return None


def select_access_loss_enforcer(
    settings: BillingSettings,
    enforced: Callable[[], AccessLossEnforcerPort],
    shadow: Callable[[], AccessLossEnforcerPort],
) -> AccessLossEnforcerPort | None:
    """Args: Settings de billing e fábricas dos enforcers enforce e shadow.
    Returns: Enforcer do modo ativo, ou None quando o enforcement está desligado.
    """
    if settings.mode is BillingMode.DISABLED:
        return None
    if settings.enforcement_mode is BillingEnforcementMode.OFF:
        return None
    return enforced() if settings.enforced else shadow()
