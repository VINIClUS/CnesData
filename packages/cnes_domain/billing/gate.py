"""Entitlement gate routing critical operations through strong snapshot reads."""

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Protocol, runtime_checkable

from cnes_domain.billing.commands import (
    AnalyticsRequest,
    CreateRunRequest,
    GateRequest,
    PublishGateRequest,
    ReserveRunCommand,
)
from cnes_domain.billing.errors import EntitlementDenied
from cnes_domain.billing.models import (
    AnalyticsAuthorization,
    BillingMetric,
    EntitlementAction,
    EntitlementDecision,
    EntitlementSnapshot,
    ReadConsistency,
    RunAuthorization,
)
from cnes_domain.billing.policy import EntitlementPolicy, require_allowed
from cnes_domain.billing.ports import (
    BillingMetricsPort,
    ClockPort,
    EntitlementProjectionPort,
    QuotaReservationPort,
)
from cnes_domain.billing.validation import require_positive

_REASON = re.compile(r"reason=([a-z][a-z0-9_]*)")
_FALLBACK_REASON = "entitlement_denied"


def _denial_reason(error: EntitlementDenied) -> str:
    match = _REASON.search(str(error))
    return match.group(1) if match else _FALLBACK_REASON


def _counts_denials(method: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(method)
    def wrapper(self: "EntitlementGate", *args: Any, **kwargs: Any) -> Any:
        try:
            return method(self, *args, **kwargs)
        except EntitlementDenied as error:
            self._count_denial(error)  # pyright: ignore[reportPrivateUsage]
            raise

    return wrapper


@runtime_checkable
class EntitlementCacheReader(Protocol):
    def get_latest(self, billing_account_id: str) -> EntitlementSnapshot | None: ...


def _require_account(snapshot: EntitlementSnapshot, billing_account_id: str) -> EntitlementSnapshot:
    if snapshot.billing_account_id != billing_account_id:
        raise EntitlementDenied("reason=snapshot_account_mismatch")
    return snapshot


@dataclass(frozen=True, slots=True)
class RunReservationSettings:
    deployment_max_concurrency: int
    reservation_id_factory: Callable[[], str]
    reservation_ttl: timedelta

    def __post_init__(self) -> None:
        require_positive(self.deployment_max_concurrency, "deployment_max_concurrency")
        if self.reservation_ttl <= timedelta(0):
            raise ValueError("reason=reservation_ttl_not_positive")


@dataclass(frozen=True, slots=True)
class EntitlementGateDependencies:
    projection: EntitlementProjectionPort
    quotas: QuotaReservationPort
    clock: ClockPort
    run_settings: RunReservationSettings
    policy: EntitlementPolicy = field(default_factory=EntitlementPolicy)
    cache: EntitlementCacheReader | None = None
    metrics: BillingMetricsPort | None = None


class EntitlementGate:
    def __init__(self, dependencies: EntitlementGateDependencies) -> None:
        self._projection = dependencies.projection
        self._quotas = dependencies.quotas
        self._clock = dependencies.clock
        self._settings = dependencies.run_settings
        self._policy = dependencies.policy
        self._cache = dependencies.cache
        self._metrics = dependencies.metrics

    def _count_denial(self, error: EntitlementDenied) -> None:
        if self._metrics is None:
            return
        self._metrics.emit(
            BillingMetric(
                name="EntitlementChecksDenied",
                value=1.0,
                unit="Count",
                dimensions={"Reason": _denial_reason(error)},
                occurred_at=self._clock(),
            )
        )

    @_counts_denials
    def authorize_create_run(self, request: CreateRunRequest) -> RunAuthorization:
        """Args: request: Pedido de criação de run.
        Returns: Autorização com reserva de cota criada pelo port de quotas.
        Raises: EntitlementDenied: Snapshot ausente ou ação negada.
        """
        snapshot = self._critical_snapshot(request.billing_account_id)
        now = self._clock()
        self._authorize(snapshot, EntitlementAction.CREATE_RUN, now)
        settings = self._settings
        command = ReserveRunCommand(
            request=request,
            snapshot=snapshot,
            deployment_max_concurrency=settings.deployment_max_concurrency,
            reservation_id=settings.reservation_id_factory(),
            expires_at=now + settings.reservation_ttl,
        )
        return self._quotas.reserve_and_create_run(command)

    @_counts_denials
    def authorize_register_agent(self, request: GateRequest) -> EntitlementDecision:
        """Args: request: Pedido de registro de agente.
        Returns: Decisão permitida com limite de agentes.
        Raises: EntitlementDenied: Snapshot ausente ou ação negada.
        """
        return self._decide_critical(request, EntitlementAction.REGISTER_AGENT)

    @_counts_denials
    def authorize_analytics_query(self, request: AnalyticsRequest) -> AnalyticsAuthorization:
        """Args: request: Pedido de consulta analítica.
        Returns: Autorização sem reserva, limitada pelo orçamento de scan.
        Raises: EntitlementDenied: Snapshot ausente ou ação negada.
        """
        snapshot = self._critical_snapshot(request.billing_account_id)
        now = self._clock()
        decision = self._authorize(snapshot, EntitlementAction.ANALYTICS_QUERY, now)
        limit = decision.quota_limit
        return AnalyticsAuthorization(
            billing_account_id=request.billing_account_id,
            entitlement_version=snapshot.entitlement_version,
            budget_reservation_id=None,
            max_scan_bytes=limit if limit is not None else request.estimated_scan_bytes,
            authorized_at=now,
        )

    @_counts_denials
    def authorize_serving_access(
        self, request: GateRequest, allow_cached: bool = False,
    ) -> EntitlementDecision:
        """Args: request: Pedido de acesso; allow_cached: Permite snapshot em cache.
        Returns: Decisão permitida, possivelmente somente leitura.
        Raises: EntitlementDenied: Snapshot ausente ou ação negada.
        """
        snapshot = self._serving_snapshot(request.billing_account_id, allow_cached)
        return self._authorize(snapshot, EntitlementAction.SERVING_ACCESS, self._clock())

    @_counts_denials
    def authorize_tenant_creation(self, request: GateRequest) -> EntitlementDecision:
        """Args: request: Pedido de criação de tenant.
        Returns: Decisão permitida com limite de tenants.
        Raises: EntitlementDenied: Snapshot ausente ou ação negada.
        """
        return self._decide_critical(request, EntitlementAction.TENANT_CREATION)

    @_counts_denials
    def authorize_publish_run(self, request: PublishGateRequest) -> EntitlementDecision:
        """Args: request: Pedido de publicação com versão de entitlement esperada.
        Returns: Decisão permitida para publicar o run.
        Raises: EntitlementDenied: Snapshot ausente, regredido ou ação negada.
        """
        snapshot = self._critical_snapshot(request.billing_account_id)
        if snapshot.entitlement_version < request.expected_entitlement_version:
            raise EntitlementDenied("reason=snapshot_version_regressed action=publish_run")
        return self._authorize(snapshot, EntitlementAction.PUBLISH_RUN, self._clock())

    def _critical_snapshot(self, billing_account_id: str) -> EntitlementSnapshot:
        snapshot = self._projection.get_snapshot(billing_account_id, ReadConsistency.STRONG)
        if snapshot is None:
            raise EntitlementDenied("reason=snapshot_missing")
        return _require_account(snapshot, billing_account_id)

    def _serving_snapshot(self, billing_account_id: str, allow_cached: bool) -> EntitlementSnapshot:
        cached = None
        if allow_cached and self._cache is not None:
            cached = self._cache.get_latest(billing_account_id)
        if cached is None:
            return self._critical_snapshot(billing_account_id)
        return _require_account(cached, billing_account_id)

    def _authorize(
        self, snapshot: EntitlementSnapshot, action: EntitlementAction, now: datetime,
    ) -> EntitlementDecision:
        return require_allowed(self._policy.evaluate(snapshot, action, now))

    def _decide_critical(
        self, request: GateRequest, action: EntitlementAction,
    ) -> EntitlementDecision:
        snapshot = self._critical_snapshot(request.billing_account_id)
        return self._authorize(snapshot, action, self._clock())
