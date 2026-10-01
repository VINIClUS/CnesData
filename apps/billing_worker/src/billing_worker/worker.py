"""Composição e execução dos ciclos do worker de billing."""

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, Protocol

from cnes_domain.billing.inbox import RecoveryRequest, RecoveryResult
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.composition import (
    BillingStorage,
    StripeRuntimeSettings,
    build_secret_provider,
    build_stripe_billing,
)
from cnes_infra.billing.settings import BillingConfigurationError, BillingSettings

SessionFactory = Callable[[str], Any]


class RecoveryRunner(Protocol):
    def drain_inbox(self, limit: int) -> RecoveryResult: ...  # pragma: no cover

    def run(self, request: RecoveryRequest) -> RecoveryResult: ...  # pragma: no cover


class BillingWorker:
    def __init__(self, recovery: RecoveryRunner, request: RecoveryRequest) -> None:
        self._recovery = recovery
        self._request = request

    def run_inbox(self, limit: int) -> RecoveryResult:
        """Args: limit: Máximo de eventos vencidos do inbox neste ciclo.
        Returns: Resultado do dreno limitado.
        Raises: BillingError: Falha de dependência ou permanente.
        """
        return self._recovery.drain_inbox(limit)

    def run_recover(self) -> RecoveryResult:
        """Returns: Resultado de uma página de recovery pelo cursor de eventos Stripe.
        Raises: RetryableBillingError: Página não resolvida ou sem progresso.
        """
        return self._recovery.run(self._request)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _required(values: Mapping[str, str], key: str, code: str) -> str:
    value = values.get(key, "").strip()
    if not value:
        raise BillingConfigurationError(code)
    return value


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
    region = _required(values, "AWS_REGION", "aws_region_required")
    table = _required(values, "AWS_CONTROL_PLANE_TABLE", "billing_table_required")
    endpoint = values.get("DYNAMODB_ENDPOINT_URL", "").strip() or None
    stripe_settings = StripeRuntimeSettings.from_mapping(values)
    session = session_factory(region)
    provider = build_secret_provider(billing.mode, session)
    if provider is None:
        raise BillingConfigurationError("secret_provider_required")
    client = session.client("dynamodb", region_name=region, endpoint_url=endpoint)
    components = build_stripe_billing(
        stripe_settings, provider, BillingStorage(client, table), _utc_now,
    )
    return BillingWorker(components.recovery, stripe_settings.recovery)
