"""Composição do worker de billing por modo de enforcement."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from billing_worker.worker import BillingWorker, WorkerJobs
from cnes_domain.billing.ports import BillingMetricsPort
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.composition import (
    BillingStorage,
    StripeBillingComponents,
    StripeRuntimeSettings,
    build_secret_provider,
    build_stripe_billing,
    build_webhook_recovery,
)
from cnes_infra.billing.enforcement import AccessLossEnforcerPort, select_access_loss_enforcer
from cnes_infra.billing.metrics import build_billing_metrics
from cnes_infra.billing.settings import BillingConfigurationError, BillingSettings

SessionFactory = Callable[[str], Any]


@dataclass(frozen=True, slots=True)
class _Runtime:
    session: Any
    region: str
    storage: BillingStorage
    state_machine_arn: str | None
    secrets: Any


@dataclass(frozen=True, slots=True)
class _Parts:
    settings: StripeRuntimeSettings
    components: StripeBillingComponents
    enforcer: AccessLossEnforcerPort | None
    metrics: BillingMetricsPort


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _required(values: Mapping[str, str], key: str, code: str) -> str:
    value = values.get(key, "").strip()
    if not value:
        raise BillingConfigurationError(code)
    return value


def _state_machine_arn(values: Mapping[str, str], billing: BillingSettings) -> str | None:
    if not billing.enforced:
        return None
    return _required(values, "AWS_STATE_MACHINE_ARN", "state_machine_arn_required")


def _revocation_service(runtime: _Runtime, components: StripeBillingComponents) -> Any:
    from cnes_domain.billing.revocation import (
        ImmediateRevocationService,
        RevocationDependencies,
    )
    from cnes_infra.billing.dynamodb_revocation import DynamoRevocationStore
    from cnes_infra.executor.step_functions import StepFunctionsExecutor

    storage = runtime.storage
    client = runtime.session.client("stepfunctions", region_name=runtime.region)
    return ImmediateRevocationService(
        RevocationDependencies(
            components.projection,
            DynamoRevocationStore(storage.client, storage.table_name, _utc_now),
            StepFunctionsExecutor(client, runtime.state_machine_arn),
            components.audit,
            _utc_now,
        )
    )


def _enforcer(
    billing: BillingSettings, runtime: _Runtime, components: StripeBillingComponents
) -> AccessLossEnforcerPort | None:
    from cnes_infra.billing.enforcement import ShadowAccessLossEnforcer

    return select_access_loss_enforcer(
        billing,
        lambda: _revocation_service(runtime, components),
        lambda: ShadowAccessLossEnforcer(components.audit, _utc_now),
    )


def _recovery(runtime: _Runtime, parts: _Parts) -> Any:
    from cnes_infra.billing.projector import ProjectorDependencies

    c = parts.components
    dependencies = ProjectorDependencies(
        c.inbox, c.catalog, c.gateway, c.projection, _utc_now, parts.enforcer, parts.metrics,
    )
    return build_webhook_recovery(runtime.storage, dependencies)


def _reconciler(runtime: _Runtime, parts: _Parts) -> Any:
    from cnes_infra.billing.reconciliation import BillingReconciler, ReconciliationDependencies
    from cnes_infra.billing.reconciliation_cursor import DynamoReconciliationCursor

    c, storage = parts.components, runtime.storage
    cursor = DynamoReconciliationCursor(storage.client, storage.table_name, _utc_now)
    return BillingReconciler(
        ReconciliationDependencies(
            c.catalog, c.gateway, c.projection, cursor, parts.enforcer, c.audit,
            parts.metrics, _utc_now,
        )
    )


def _revocations(runtime: _Runtime, parts: _Parts) -> Any:
    from cnes_infra.billing.keys import revocation_sweep_cursor_key
    from cnes_infra.billing.reconciliation_cursor import DynamoReconciliationCursor
    from cnes_infra.billing.revocation_sweep import RevocationSweep, RevocationSweepDependencies

    if parts.enforcer is None:
        return None
    storage = runtime.storage
    cursor = DynamoReconciliationCursor(
        storage.client, storage.table_name, _utc_now, revocation_sweep_cursor_key(),
    )
    return RevocationSweep(
        RevocationSweepDependencies(
            parts.components.catalog, cursor, parts.enforcer, parts.metrics, _utc_now,
        )
    )


def _jobs(runtime: _Runtime, parts: _Parts) -> WorkerJobs:
    from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations

    storage = runtime.storage
    return WorkerJobs(
        recovery=_recovery(runtime, parts),
        request=parts.settings.recovery,
        reconciler=_reconciler(runtime, parts),
        revocations=_revocations(runtime, parts),
        reservations=DynamoQuotaReservations(storage.client, storage.table_name, _utc_now),
        metrics=parts.metrics,
        clock=_utc_now,
    )


def _runtime(
    values: Mapping[str, str], billing: BillingSettings, session_factory: SessionFactory
) -> _Runtime:
    region = _required(values, "AWS_REGION", "aws_region_required")
    table = _required(values, "AWS_CONTROL_PLANE_TABLE", "billing_table_required")
    endpoint = values.get("DYNAMODB_ENDPOINT_URL", "").strip() or None
    arn = _state_machine_arn(values, billing)
    session = session_factory(region)
    secrets = build_secret_provider(billing.mode, session)
    if secrets is None:
        raise BillingConfigurationError("secret_provider_required")
    client = session.client("dynamodb", region_name=region, endpoint_url=endpoint)
    return _Runtime(session, region, BillingStorage(client, table), arn, secrets)


def build_worker(
    values: Mapping[str, str], session_factory: SessionFactory,
) -> BillingWorker | None:
    """Args: values: Variáveis de ambiente; session_factory: região para sessão boto3.
    Returns: Worker pronto, ou None quando o billing está desabilitado.
    Raises: BillingConfigurationError: Configuração ausente ou inválida.
    """
    billing = BillingSettings.from_mapping(values)
    if billing.mode is BillingMode.DISABLED:
        return None
    settings = StripeRuntimeSettings.from_mapping(values)
    runtime = _runtime(values, billing, session_factory)
    components = build_stripe_billing(settings, runtime.secrets, runtime.storage, _utc_now)
    enforcer = _enforcer(billing, runtime, components)
    metrics = build_billing_metrics(billing.metrics_environment)
    return BillingWorker(_jobs(runtime, _Parts(settings, components, enforcer, metrics)))
