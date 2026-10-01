"""Instalação do billing na API por modo: dependências disabled e Stripe."""
from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException
from starlette.requests import Request  # noqa: TC002 - resolved at runtime by FastAPI

from central_api.routes import raw_jobs
from central_api.services.agent_admission import AgentAdmission
from cnes_domain.ports.control_plane import ControlPlanePort  # noqa: TC001

if TYPE_CHECKING:
    from collections.abc import Callable

    from boto3.session import Session

    from central_api.composition import RuntimeComponents
    from central_api.services.billing_gates import ApiBillingGates
    from cnes_domain.billing.ports import BillingMetricsPort, SecretProviderPort
    from cnes_domain.billing.revocation import ImmediateRevocationService
    from cnes_infra.billing import StripeBillingComponents

logger = logging.getLogger(__name__)

_BILLING_PATH_PREFIX = "/api/v1/billing/"


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _billing_disabled() -> None:
    raise HTTPException(status_code=404, detail="billing_disabled")


def install_billing(app: object, runtime: RuntimeComponents, session: Session) -> None:
    """Args: app: FastAPI; runtime: Componentes compostos; session: boto3 do runtime.
    Raises: BillingConfigurationError: Stripe sem DynamoDB ou configuração inválida.
    """
    from central_api.routes import billing, stripe_webhook
    from cnes_infra.billing import BillingSettings, build_secret_provider
    from cnes_infra.billing.metrics import build_billing_metrics

    settings = BillingSettings.from_mapping(os.environ)
    provider = build_secret_provider(settings.mode, session)
    app.dependency_overrides[billing.get_billing_mode] = lambda: settings.mode
    _install_agent_admission(app, runtime.billing_gates)
    if provider is None:
        app.dependency_overrides[stripe_webhook.get_stripe_webhook_verifier] = _billing_disabled
        logger.info("billing_composed mode=%s", settings.mode.value)
        return
    _install_stripe_billing(
        app, runtime, provider, build_billing_metrics(settings.metrics_environment),
    )


def _install_agent_admission(app: object, gates: ApiBillingGates | None) -> None:
    if gates is None:
        return

    def _admission(
        control_plane: Annotated[ControlPlanePort, Depends(raw_jobs.get_control_plane)],
    ) -> AgentAdmission:
        return AgentAdmission(control_plane, gates)

    app.dependency_overrides[raw_jobs.get_agent_admission] = _admission


def _billing_control_plane(runtime: RuntimeComponents) -> Callable[[Request], ControlPlanePort]:
    def _resolve(request: Request) -> ControlPlanePort:
        if not request.url.path.startswith(_BILLING_PATH_PREFIX):
            raise HTTPException(status_code=503, detail="control_plane_not_configured")
        return runtime.control_plane

    return _resolve


def _install_stripe_billing(
    app: object,
    runtime: RuntimeComponents,
    provider: SecretProviderPort,
    metrics: BillingMetricsPort,
) -> None:
    from central_api.routes import billing, stripe_webhook
    from cnes_infra.billing import (
        BillingConfigurationError,
        StripeRuntimeSettings,
        build_stripe_billing,
    )
    services = runtime.services
    if services is None or services.billing_storage is None:
        raise BillingConfigurationError("billing_dynamodb_required")
    components = build_stripe_billing(
        StripeRuntimeSettings.from_mapping(os.environ), provider,
        services.billing_storage, _utc_now,
    )
    overrides = app.dependency_overrides
    overrides[billing.get_membership_authorizer] = lambda: services.membership_authorizer
    overrides[billing.get_billing_catalog] = lambda: components.catalog
    overrides[billing.get_stripe_gateway] = lambda: components.gateway
    overrides[billing.get_entitlement_projection] = lambda: components.projection
    overrides[billing.get_billing_audit] = lambda: components.audit
    overrides[billing.get_billing_clock] = lambda: _utc_now
    overrides[raw_jobs.get_control_plane] = _billing_control_plane(runtime)
    overrides[stripe_webhook.get_stripe_webhook_verifier] = lambda: components.verifier
    overrides[stripe_webhook.get_webhook_inbox] = lambda: components.inbox
    overrides[stripe_webhook.get_billing_metrics] = lambda: metrics
    _install_billing_admin(app, runtime, components)
    logger.info("billing_composed mode=stripe")


def _install_billing_admin(
    app: object, runtime: RuntimeComponents, components: StripeBillingComponents,
) -> None:
    from central_api.routes import billing_admin, tenants

    gates = runtime.billing_gates
    revocation = _revocation_service(runtime, components)
    overrides = app.dependency_overrides
    overrides[billing_admin.get_revocation_service] = lambda: revocation
    if gates is not None:
        overrides[tenants.get_tenant_gates] = lambda: gates


def _revocation_service(
    runtime: RuntimeComponents, components: StripeBillingComponents,
) -> ImmediateRevocationService:
    from cnes_domain.billing.revocation import (
        ImmediateRevocationService,
        RevocationDependencies,
    )
    from cnes_infra.billing.dynamodb_revocation import DynamoRevocationStore

    storage = runtime.services.billing_storage
    store = DynamoRevocationStore(storage.client, storage.table_name, _utc_now)
    return ImmediateRevocationService(
        RevocationDependencies(
            components.projection, store, runtime.executor, components.audit, _utc_now,
        )
    )
