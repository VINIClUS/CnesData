"""Composição do gate de entitlement e dos callbacks de execução por modo."""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import uuid4

from cnes_domain.billing.commands import CreateRunRequest
from cnes_domain.billing.errors import BillingError
from cnes_domain.billing.execution_policy import (
    BillingConcurrencyPolicy,
    BillingExecutionDependencies,
    BillingExecutionStarted,
    ExecutionBindingPort,
)
from cnes_domain.billing.gate import (
    EntitlementGate,
    EntitlementGateDependencies,
    RunReservationSettings,
)
from cnes_domain.billing.models import (
    BillingAuditEvent,
    BillingEnforcementMode,
    EntitlementAction,
    ReadConsistency,
    RunAuthorization,
)
from cnes_domain.billing.policy import EntitlementPolicy
from cnes_domain.billing.ports import (
    BillingAuditPort,
    ClockPort,
    EntitlementProjectionPort,
    QuotaReservationPort,
)
from cnes_domain.ports.processing import ExecutionCallbacks, ExecutionPermit, ExecutionStarted
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.disabled import DisabledEntitlementProjection, DisabledQuotaReservations
from cnes_infra.billing.metrics import build_billing_metrics
from cnes_infra.billing.settings import BillingConfigurationError, BillingSettings

logger = logging.getLogger(__name__)

RESERVATION_TTL = timedelta(minutes=15)


@dataclass(frozen=True, slots=True)
class BillingGateResources:
    clock: ClockPort
    deployment_max_concurrency: int
    dynamodb_client: Any | None = None
    table_name: str | None = None


@dataclass(frozen=True, slots=True)
class BillingEnforcement:
    gate: EntitlementGate
    capacity: QuotaReservationPort
    audit: BillingAuditPort | None = None


StartedCallback = Callable[[Any, Any, str, ExecutionPermit], None]


class ChainedExecutionStarted:
    def __init__(self, billing: StartedCallback, downstream: StartedCallback) -> None:
        self.billing = billing
        self.downstream = downstream

    def __call__(self, run: Any, request: Any, execution_ref: str, permit: ExecutionPermit) -> None:
        self.billing(run, request, execution_ref, permit)
        self.downstream(run, request, execution_ref, permit)


class ShadowEntitlementGate(EntitlementGate):
    def __init__(
        self,
        dependencies: EntitlementGateDependencies,
        observed: EntitlementProjectionPort,
        audit: BillingAuditPort | None = None,
    ) -> None:
        super().__init__(dependencies)
        self._observed = observed
        self._audit = audit

    def authorize_create_run(self, request: CreateRunRequest) -> RunAuthorization:
        """Args: request: Pedido de criação de run.
        Returns: Autorização sem medição; o snapshot real é apenas observado.
        """
        reason = self._shadow_reason(request)
        if reason is not None:
            logger.warning(
                "billing_shadow_denied action=create_run reason=%s billing_account_id=%s "
                "tenant_id=%s",
                reason,
                request.billing_account_id,
                request.tenant_id,
            )
            self._audit_denial(request, reason)
        return super().authorize_create_run(request)

    def _audit_denial(self, request: CreateRunRequest, reason: str) -> None:
        if self._audit is None:
            return
        from cnes_infra.billing.dynamodb_items import deterministic_id

        self._audit.append(
            BillingAuditEvent(
                event_id=deterministic_id(
                    "entitlement.shadow_denied",
                    request.billing_account_id,
                    request.tenant_id,
                    request.idempotency_key,
                ),
                event_type="entitlement.shadow_denied",
                aggregate_id=request.billing_account_id,
                actor_id="system:shadow_gate",
                reason_code=reason,
                occurred_at=self._clock(),
                attributes={
                    "action": "create_run",
                    "tenant_id": request.tenant_id,
                    "run_id": request.run_id,
                },
            )
        )

    def _shadow_reason(self, request: CreateRunRequest) -> str | None:
        try:
            snapshot = self._observed.get_snapshot(
                request.billing_account_id, ReadConsistency.STRONG,
            )
        except BillingError:
            return "projection_unavailable"
        if snapshot is None:
            return "snapshot_missing"
        if snapshot.billing_account_id != request.billing_account_id:
            return "snapshot_account_mismatch"
        decision = EntitlementPolicy(BillingMode.STRIPE).evaluate(
            snapshot, EntitlementAction.CREATE_RUN, self._clock(),
        )
        return None if decision.allowed else decision.reason


def _run_settings(resources: BillingGateResources) -> RunReservationSettings:
    return RunReservationSettings(
        resources.deployment_max_concurrency, lambda: str(uuid4()), RESERVATION_TTL,
    )


def _unmetered_dependencies(resources: BillingGateResources) -> EntitlementGateDependencies:
    clock = resources.clock
    return EntitlementGateDependencies(
        DisabledEntitlementProjection(clock),
        DisabledQuotaReservations(clock),
        clock,
        _run_settings(resources),
        EntitlementPolicy(BillingMode.DISABLED),
        None,
    )


def _dynamodb_resources(resources: BillingGateResources) -> tuple[Any, str]:
    if resources.dynamodb_client is None or not resources.table_name:
        raise BillingConfigurationError("billing_dynamodb_required")
    return resources.dynamodb_client, resources.table_name


def _enforced(
    settings: BillingSettings, resources: BillingGateResources, client: Any, table: str,
) -> BillingEnforcement:
    from cnes_infra.billing.audit_outbox import BestEffortBillingAudit, DynamoBillingAudit
    from cnes_infra.billing.cache import LocalEntitlementCache
    from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection
    from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations

    clock = resources.clock
    metrics = build_billing_metrics(settings.metrics_environment)
    audit = BestEffortBillingAudit(DynamoBillingAudit(client, table), metrics, clock)
    ttl = settings.cache_ttl_seconds
    cache = LocalEntitlementCache(ttl, clock) if ttl > 0 else None
    quotas = DynamoQuotaReservations(client, table, clock)
    gate = EntitlementGate(
        EntitlementGateDependencies(
            DynamoEntitlementProjection(client, table, clock),
            quotas,
            clock,
            _run_settings(resources),
            EntitlementPolicy(BillingMode.STRIPE),
            cache,
            metrics,
        )
    )
    return BillingEnforcement(gate, quotas, audit)


def _unmetered(resources: BillingGateResources) -> BillingEnforcement:
    dependencies = _unmetered_dependencies(resources)
    return BillingEnforcement(EntitlementGate(dependencies), dependencies.quotas)


def _shadow(
    settings: BillingSettings, resources: BillingGateResources, client: Any, table: str,
) -> BillingEnforcement:
    from cnes_infra.billing.audit_outbox import BestEffortBillingAudit, DynamoBillingAudit
    from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection

    observed = DynamoEntitlementProjection(client, table, resources.clock)
    metrics = build_billing_metrics(settings.metrics_environment)
    audit = BestEffortBillingAudit(DynamoBillingAudit(client, table), metrics, resources.clock)
    dependencies = _unmetered_dependencies(resources)
    gate = ShadowEntitlementGate(dependencies, observed, audit)
    return BillingEnforcement(gate, dependencies.quotas, audit)


def build_billing_enforcement(
    settings: BillingSettings, resources: BillingGateResources,
) -> BillingEnforcement:
    """Args: settings: Modo de billing; resources: Relógio, limite e DynamoDB.
    Returns: Gate e port de capacidade da mesma composição por modo.
    Raises: BillingConfigurationError: Stripe sem cliente ou tabela DynamoDB.
    """
    unmetered = settings.mode is BillingMode.DISABLED
    if unmetered or settings.enforcement_mode is BillingEnforcementMode.OFF:
        return _unmetered(resources)
    client, table = _dynamodb_resources(resources)
    if settings.enforced:
        return _enforced(settings, resources, client, table)
    return _shadow(settings, resources, client, table)


def build_entitlement_gate(
    settings: BillingSettings, resources: BillingGateResources,
) -> EntitlementGate:
    """Args: settings: Modo de billing; resources: Relógio, limite e DynamoDB.
    Returns: Gate sem medição, em sombra ou com enforcement sobre DynamoDB.
    Raises: BillingConfigurationError: Stripe sem cliente ou tabela DynamoDB.
    """
    return build_billing_enforcement(settings, resources).gate


def _execution_audit(
    settings: BillingSettings, resources: BillingGateResources,
) -> BillingAuditPort | None:
    if settings.mode is not BillingMode.STRIPE:
        return None
    if resources.dynamodb_client is None or not resources.table_name:
        return None
    from cnes_infra.billing.audit_outbox import BestEffortBillingAudit, DynamoBillingAudit

    return BestEffortBillingAudit(
        DynamoBillingAudit(resources.dynamodb_client, resources.table_name),
        build_billing_metrics(settings.metrics_environment),
        resources.clock,
    )


def build_execution_callbacks(
    settings: BillingSettings,
    control_plane: ExecutionBindingPort,
    resources: BillingGateResources,
    execution_started: ExecutionStarted,
) -> ExecutionCallbacks:
    """Args: settings: Modo; control_plane: Port; resources: Relógio e DynamoDB; execution_started.
    Returns: Política de concorrência e callback de início encadeado.
    """
    audit = _execution_audit(settings, resources)
    deps = BillingExecutionDependencies(
        control_plane, resources.clock, settings.execution_mode, audit,
    )
    started = ChainedExecutionStarted(BillingExecutionStarted(deps), execution_started)
    return ExecutionCallbacks(BillingConcurrencyPolicy(deps), started)
