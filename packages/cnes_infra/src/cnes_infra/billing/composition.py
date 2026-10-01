"""Composição dos adapters de billing Stripe sem carregar SDKs remotos no import."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, Self

from cnes_domain.billing.inbox import RecoveryRequest
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.settings import BillingConfigurationError
from cnes_infra.billing.stripe_gateway import StripeGateway, StripeGatewayConfig

if TYPE_CHECKING:
    from collections.abc import Mapping

    from cnes_domain.billing.ports import (
        BillingAuditPort,
        BillingCatalogPort,
        ClockPort,
        EntitlementProjectionPort,
        SecretProviderPort,
        StripeGatewayPort,
        WebhookInboxPort,
    )
    from cnes_infra.billing.projector import ProjectorDependencies
    from cnes_infra.billing.recovery import WebhookRecovery
    from cnes_infra.billing.webhook_verifier import StripeWebhookVerifier

__all__ = [
    "BillingStorage",
    "SessionProtocol",
    "StripeBillingComponents",
    "StripeRuntimeSettings",
    "build_secret_provider",
    "build_stripe_billing",
]


class SessionProtocol(Protocol):
    def client(self, service_name: str, *args: Any, **kwargs: Any) -> Any: ...  # pragma: no cover


def build_secret_provider(
    mode: BillingMode, session: SessionProtocol,
) -> SecretProviderPort | None:
    """Args: mode: Modo de billing; session: Sessão boto3 ou equivalente.
    Returns: Provider do Secrets Manager, ou None quando o billing está desabilitado.
    Raises: BillingConfigurationError: Modo desconhecido.
    """
    if mode == BillingMode.DISABLED:
        return None
    if mode == BillingMode.STRIPE:
        from cnes_infra.billing.secrets_manager import SecretsManagerSecretProvider

        return SecretsManagerSecretProvider(session.client("secretsmanager"))
    raise BillingConfigurationError("billing_mode_unknown")


@dataclass(frozen=True, slots=True)
class BillingStorage:
    client: Any
    table_name: str


def _required(values: Mapping[str, str], key: str, code: str) -> str:
    value = values.get(key, "").strip()
    if not value:
        raise BillingConfigurationError(code)
    return value


def _gateway_config(values: Mapping[str, str]) -> StripeGatewayConfig:
    origins = frozenset(
        origin.strip()
        for origin in values.get("BILLING_RETURN_ORIGINS", "").split(",")
        if origin.strip()
    )
    try:
        return StripeGatewayConfig(
            values.get("BILLING_SUCCESS_URL", ""),
            values.get("BILLING_CANCEL_URL", ""),
            values.get("BILLING_PORTAL_RETURN_URL", ""),
            origins,
        )
    except ValueError as error:
        raise BillingConfigurationError("billing_return_urls_invalid") from error


def _recovery_request(values: Mapping[str, str]) -> RecoveryRequest:
    from cnes_infra.billing.recovery import (
        STRIPE_RECOVERY_BATCH_SIZE,
        STRIPE_RECOVERY_LOOKBACK_HOURS,
    )

    try:
        return RecoveryRequest(
            int(values.get("STRIPE_RECOVERY_LOOKBACK_HOURS", STRIPE_RECOVERY_LOOKBACK_HOURS)),
            int(values.get("STRIPE_RECOVERY_BATCH_SIZE", STRIPE_RECOVERY_BATCH_SIZE)),
        )
    except ValueError as error:
        raise BillingConfigurationError("stripe_recovery_settings_invalid") from error


@dataclass(frozen=True, slots=True)
class StripeRuntimeSettings:
    secret_key_arn: str
    webhook_secret_arn: str
    gateway: StripeGatewayConfig
    recovery: RecoveryRequest

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> Self:
        """Args: values: Variáveis de ambiente; apenas ARNs, nunca valores de segredo.
        Returns: Configuração Stripe validada.
        Raises: BillingConfigurationError: ARN ausente, URLs ou recovery inválidos.
        """
        return cls(
            _required(values, "STRIPE_SECRET_KEY_SECRET_ARN", "stripe_secret_key_arn_required"),
            _required(
                values, "STRIPE_WEBHOOK_SECRET_SECRET_ARN", "stripe_webhook_secret_arn_required",
            ),
            _gateway_config(values),
            _recovery_request(values),
        )


@dataclass(frozen=True, slots=True)
class StripeBillingComponents:
    catalog: BillingCatalogPort
    projection: EntitlementProjectionPort
    gateway: StripeGatewayPort
    verifier: StripeWebhookVerifier
    inbox: WebhookInboxPort
    audit: BillingAuditPort
    recovery: WebhookRecovery


def _build_recovery(
    storage: BillingStorage, dependencies: ProjectorDependencies,
) -> WebhookRecovery:
    from cnes_infra.billing.projector import StripeEventProjector
    from cnes_infra.billing.recovery import RecoveryDependencies, WebhookRecovery
    from cnes_infra.billing.recovery_cursor import DynamoRecoveryCursor

    clock = dependencies.clock
    cursor = DynamoRecoveryCursor(storage.client, storage.table_name, clock)
    return WebhookRecovery(RecoveryDependencies(
        dependencies.inbox, StripeEventProjector(dependencies), dependencies.stripe, cursor, clock,
    ))


def build_stripe_billing(
    settings: StripeRuntimeSettings,
    secrets: SecretProviderPort,
    storage: BillingStorage,
    clock: ClockPort,
) -> StripeBillingComponents:
    """Args: settings: ARNs e config; secrets: Provider; storage: DynamoDB; clock: Relógio.
    Returns: Componentes Stripe compartilhando o mesmo storage e relógio.
    Raises: SecretProviderError: Falha ao obter segredos.
    """
    import stripe

    from cnes_infra.billing.audit_outbox import DynamoBillingAudit
    from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
    from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection
    from cnes_infra.billing.projector import ProjectorDependencies
    from cnes_infra.billing.webhook_inbox import WebhookInbox
    from cnes_infra.billing.webhook_verifier import StripeWebhookVerifier

    api_key = secrets.get_secret(settings.secret_key_arn)
    webhook_secret = secrets.get_secret(settings.webhook_secret_arn)
    args = (storage.client, storage.table_name, clock)
    catalog = DynamoBillingCatalog(*args)
    projection = DynamoEntitlementProjection(*args)
    inbox = WebhookInbox(*args)
    gateway = StripeGateway(stripe.StripeClient(api_key), settings.gateway, catalog)
    return StripeBillingComponents(
        catalog=catalog,
        projection=projection,
        gateway=gateway,
        verifier=StripeWebhookVerifier(webhook_secret),
        inbox=inbox,
        audit=DynamoBillingAudit(storage.client, storage.table_name),
        recovery=_build_recovery(
            storage, ProjectorDependencies(inbox, catalog, gateway, projection, clock),
        ),
    )
