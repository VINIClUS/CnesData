"""Gate de entitlement do serving aplicado após a autorização de membership."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Protocol
from uuid import uuid4

from central_api.services.billing_gates import BillingAccountMissing
from central_api.services.serving_access import ServingUnavailable
from cnes_domain.billing.commands import GateRequest
from cnes_domain.billing.errors import BillingError, EntitlementDenied
from cnes_domain.billing.models import AccessLevel, BillingAuditEvent

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from central_api.services.billing_gates import ApiBillingGates
    from cnes_domain.billing.models import EntitlementDecision
    from cnes_domain.control_plane.entities import DatasetVersion
    from cnes_domain.ports.serving import ServingAccessPort, ServingGrant, ServingRequest

logger = logging.getLogger(__name__)

_SERVING_ALLOW_CACHED = False
_REASON = re.compile(r"reason=([a-z][a-z0-9_]*)")
_DEFAULT_REASON = "entitlement_denied"
_BLOCKED = "blocked"
SERVING_DENIED_EVENT = "serving.denied"


class DatasetVersionReader(Protocol):
    def get_dataset_version(
        self, tenant_id: str, dataset_name: str, version_id: str
    ) -> DatasetVersion | None: ...


@dataclass(frozen=True, slots=True)
class _Denial:
    reason: str
    level: str
    code: str | None = None


def _unavailable(code: str) -> ServingUnavailable:
    logger.warning("serving_entitlement_unavailable code=%s", code)
    return ServingUnavailable(code)


def _domain_reason(error: EntitlementDenied) -> str:
    found = _REASON.search(str(error))
    return found.group(1) if found else _DEFAULT_REASON


class EntitledServingAccess:
    def __init__(
        self,
        inner: ServingAccessPort,
        gates: ApiBillingGates,
        versions: DatasetVersionReader,
        clock: Callable[[], datetime],
    ) -> None:
        self._inner = inner
        self._gates = gates
        self._versions = versions
        self._clock = clock

    def authorize(self, request: ServingRequest) -> ServingGrant:
        """Autoriza membership e depois entitlement, sem iniciar compute nem apagar dados.

        Args: request: Identidade e dataset solicitados.
        Returns: Grant do acesso interno quando o entitlement permite.
        Raises: ServingUnavailable: Membership, entitlement, retenção ou dependência.
        """
        grant = self._inner.authorize(request)
        account, decision = self._decide(request)
        if decision.access_level is AccessLevel.READ_ONLY and decision.quota_limit is not None:
            self._require_retention(request, (account, grant), decision.quota_limit)
        return grant

    def _decide(self, request: ServingRequest) -> tuple[str, EntitlementDecision]:
        account = request.tenant_id
        try:
            account = self._gates.accounts.resolve(request.tenant_id)
            decision = self._gates.gate.authorize_serving_access(
                GateRequest(account, request.tenant_id), allow_cached=_SERVING_ALLOW_CACHED,
            )
        except BillingAccountMissing as error:
            raise self._deny(request, account, _Denial(error.code, _BLOCKED)) from error
        except EntitlementDenied as error:
            denial = _Denial(_domain_reason(error), _BLOCKED, "entitlement_denied")
            raise self._deny(request, account, denial) from error
        except BillingError as error:
            raise _unavailable("entitlement_unavailable") from error
        return account, decision

    def _deny(
        self, request: ServingRequest, aggregate_id: str, denial: _Denial,
    ) -> ServingUnavailable:
        logger.warning(
            "serving_denied reason=%s access_level=%s", denial.reason, denial.level,
        )
        if self._gates.audit is not None:
            self._gates.audit.append(BillingAuditEvent(
                event_id=f"{SERVING_DENIED_EVENT}:{uuid4().hex}",
                event_type=SERVING_DENIED_EVENT,
                aggregate_id=aggregate_id,
                actor_id=request.user_id,
                reason_code=denial.reason,
                occurred_at=self._clock(),
                attributes={
                    "tenant_id": request.tenant_id,
                    "dataset_name": request.dataset_name,
                    "access_level": denial.level,
                },
            ))
        return ServingUnavailable(denial.code or denial.reason)

    def _require_retention(
        self, request: ServingRequest, resolved: tuple[str, ServingGrant], retention_days: int
    ) -> None:
        account, grant = resolved
        version = self._versions.get_dataset_version(
            grant.tenant_id, request.dataset_name, grant.version_id
        )
        if version is None:
            raise _unavailable("serving_version_unavailable")
        if version.created_at < self._clock() - timedelta(days=retention_days):
            denial = _Denial("retention_expired", AccessLevel.READ_ONLY.value)
            raise self._deny(request, account, denial)
