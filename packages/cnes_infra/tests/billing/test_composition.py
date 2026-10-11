"""Testes da composição do billing Stripe."""

import logging
import subprocess
import sys
from typing import Any, cast
from unittest.mock import Mock, patch

import pytest

from cnes_domain.billing.inbox import RecoveryRequest
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.audit_outbox import DynamoBillingAudit
from cnes_infra.billing.composition import (
    BillingStorage,
    StripeRuntimeSettings,
    build_secret_provider,
    build_stripe_billing,
    build_webhook_recovery,
)
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection
from cnes_infra.billing.projector import ProjectorDependencies
from cnes_infra.billing.secrets_manager import SecretsManagerSecretProvider
from cnes_infra.billing.settings import BillingConfigurationError
from cnes_infra.billing.stripe_gateway import StripeGateway
from cnes_infra.billing.webhook_inbox import WebhookInbox
from cnes_infra.billing.webhook_verifier import StripeWebhookVerifier

ORIGIN = "https://app.example.test"
KEY_ARN = "arn:aws:secretsmanager:sa-east-1:1:secret:key"
WEBHOOK_ARN = "arn:aws:secretsmanager:sa-east-1:1:secret:hook"
API_KEY = "sk_test_valor_secreto_123"
WEBHOOK_SECRET = "whsec_valor_secreto_456"  # noqa: S105


def _values(**overrides: str) -> dict[str, str]:
    base = {
        "STRIPE_SECRET_KEY_SECRET_ARN": KEY_ARN,
        "STRIPE_WEBHOOK_SECRET_SECRET_ARN": WEBHOOK_ARN,
        "BILLING_SUCCESS_URL": f"{ORIGIN}/ok",
        "BILLING_CANCEL_URL": f"{ORIGIN}/cancel",
        "BILLING_PORTAL_RETURN_URL": f"{ORIGIN}/billing",
        "BILLING_RETURN_ORIGINS": ORIGIN,
    }
    base.update(overrides)
    return base


def _without(key: str) -> dict[str, str]:
    values = _values()
    del values[key]
    return values


def _blank(key: str) -> dict[str, str]:
    return {**_values(), key: "  "}


@pytest.fixture
def session_spy() -> Mock:
    return Mock()


def test_disabled_nao_cria_client_secrets_manager(session_spy) -> None:
    assert build_secret_provider(BillingMode.DISABLED, session_spy) is None
    session_spy.client.assert_not_called()


def test_stripe_cria_provider_com_client_secrets_manager(session_spy) -> None:
    provider = build_secret_provider(BillingMode.STRIPE, session_spy)
    session_spy.client.assert_called_once_with("secretsmanager")
    assert isinstance(provider, SecretsManagerSecretProvider)


def test_modo_desconhecido_rejeita_sem_chamar_sessao(session_spy) -> None:
    with pytest.raises(BillingConfigurationError) as caught:
        build_secret_provider(cast("BillingMode", "other"), session_spy)
    assert caught.value.code == "billing_mode_unknown"
    session_spy.client.assert_not_called()


def test_from_mapping_aplica_defaults_de_recovery() -> None:
    settings = StripeRuntimeSettings.from_mapping(_values())
    assert settings.recovery == RecoveryRequest(72, 100)
    assert settings.secret_key_arn == KEY_ARN
    assert settings.webhook_secret_arn == WEBHOOK_ARN
    assert settings.gateway.allowed_origins == frozenset({ORIGIN})


def test_from_mapping_aceita_valores_explicitos_de_recovery() -> None:
    values = _values(STRIPE_RECOVERY_LOOKBACK_HOURS="24", STRIPE_RECOVERY_BATCH_SIZE="50")
    settings = StripeRuntimeSettings.from_mapping(values)
    assert settings.recovery == RecoveryRequest(24, 50)


def test_from_mapping_normaliza_lista_de_origens() -> None:
    other = "https://outro.example.test"
    values = _values(
        BILLING_RETURN_ORIGINS=f" {ORIGIN} , ,{other},",
        BILLING_CANCEL_URL=f"{other}/cancel",
    )
    settings = StripeRuntimeSettings.from_mapping(values)
    assert settings.gateway.allowed_origins == frozenset({ORIGIN, other})


def test_from_mapping_remove_espacos_dos_arns() -> None:
    values = _values(STRIPE_SECRET_KEY_SECRET_ARN=f"  {KEY_ARN} ")
    assert StripeRuntimeSettings.from_mapping(values).secret_key_arn == KEY_ARN


@pytest.mark.parametrize(
    ("values", "code"),
    [
        (_without("STRIPE_SECRET_KEY_SECRET_ARN"), "stripe_secret_key_arn_required"),
        (_blank("STRIPE_SECRET_KEY_SECRET_ARN"), "stripe_secret_key_arn_required"),
        (_without("STRIPE_WEBHOOK_SECRET_SECRET_ARN"), "stripe_webhook_secret_arn_required"),
        (_blank("STRIPE_WEBHOOK_SECRET_SECRET_ARN"), "stripe_webhook_secret_arn_required"),
        (_without("BILLING_RETURN_ORIGINS"), "billing_return_urls_invalid"),
        (_values(BILLING_RETURN_ORIGINS=" , "), "billing_return_urls_invalid"),
        (_values(BILLING_RETURN_ORIGINS="http://inseguro.test"), "billing_return_urls_invalid"),
        (_without("BILLING_SUCCESS_URL"), "billing_return_urls_invalid"),
        (_values(BILLING_CANCEL_URL="https://fora.example.test/x"), "billing_return_urls_invalid"),
        (_values(STRIPE_RECOVERY_LOOKBACK_HOURS="abc"), "stripe_recovery_settings_invalid"),
        (_values(STRIPE_RECOVERY_BATCH_SIZE="x1"), "stripe_recovery_settings_invalid"),
        (_values(STRIPE_RECOVERY_LOOKBACK_HOURS="0"), "stripe_recovery_settings_invalid"),
        (_values(STRIPE_RECOVERY_BATCH_SIZE="0"), "stripe_recovery_settings_invalid"),
        (_values(STRIPE_RECOVERY_BATCH_SIZE="101"), "stripe_recovery_settings_invalid"),
    ],
)
def test_from_mapping_rejeita_configuracao_invalida(values, code) -> None:
    with pytest.raises(BillingConfigurationError) as caught:
        StripeRuntimeSettings.from_mapping(values)
    assert caught.value.code == code


def test_from_mapping_encadeia_erro_de_urls() -> None:
    with pytest.raises(BillingConfigurationError) as caught:
        StripeRuntimeSettings.from_mapping(_values(BILLING_RETURN_ORIGINS="http://x"))
    assert isinstance(caught.value.__cause__, ValueError)


def _build(clock=None, storage=None):
    secrets = Mock()
    secrets.get_secret.side_effect = {KEY_ARN: API_KEY, WEBHOOK_ARN: WEBHOOK_SECRET}.__getitem__
    settings = StripeRuntimeSettings.from_mapping(_values())
    storage = storage or BillingStorage(Mock(name="ddb"), "billing-table")
    clock = clock or Mock(name="clock")
    with patch("stripe.StripeClient") as stripe_client:
        components = build_stripe_billing(settings, secrets, storage, clock)
    return components, secrets, stripe_client, storage, clock


def test_build_stripe_busca_ambos_os_segredos_pelos_arns() -> None:
    _, secrets, _, _, _ = _build()
    assert [c.args for c in secrets.get_secret.call_args_list] == [(KEY_ARN,), (WEBHOOK_ARN,)]


def test_build_stripe_entrega_api_key_somente_ao_client_stripe_com_retries_de_rede() -> None:
    components, _, stripe_client, _, _ = _build()
    stripe_client.assert_called_once_with(API_KEY, max_network_retries=2)
    assert isinstance(components.gateway, StripeGateway)
    assert components.gateway._client is stripe_client.return_value


def test_build_stripe_monta_adapters_com_mesmo_storage_e_clock() -> None:
    components, _, _, storage, clock = _build()
    assert isinstance(components.catalog, DynamoBillingCatalog)
    assert isinstance(components.projection, DynamoEntitlementProjection)
    assert isinstance(components.inbox, WebhookInbox)
    assert cast("Any", components.gateway)._plans is components.catalog
    for adapter in (components.catalog, components.projection, components.inbox):
        assert adapter._client is storage.client
        assert adapter._clock is clock
    deps = components.recovery._deps
    assert deps.clock is clock
    assert deps.inbox is components.inbox
    assert deps.stripe is components.gateway
    assert cast("Any", deps.cursor)._clock is clock
    assert cast("Any", deps.cursor)._client is storage.client
    assert cast("Any", deps.projector)._deps.clock is clock
    assert cast("Any", deps.projector)._deps.projection is components.projection
    assert isinstance(components.audit, DynamoBillingAudit)
    assert components.audit._client is storage.client
    assert components.audit._table_name == storage.table_name


def test_build_stripe_entrega_webhook_secret_ao_verifier() -> None:
    components, _, _, _, _ = _build()
    assert isinstance(components.verifier, StripeWebhookVerifier)
    with patch("stripe.Webhook.construct_event", side_effect=ValueError("x")) as construct:
        with pytest.raises(Exception):
            components.verifier.verify(b"{}", "sig")
    assert construct.call_args.args[2] == WEBHOOK_SECRET


def test_segredos_nao_aparecem_em_repr_nem_em_logs(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    secrets = Mock()
    secrets.get_secret.side_effect = {KEY_ARN: API_KEY, WEBHOOK_ARN: WEBHOOK_SECRET}.__getitem__
    settings = StripeRuntimeSettings.from_mapping(_values())
    storage = BillingStorage(Mock(name="ddb"), "billing-table")
    components = build_stripe_billing(settings, secrets, storage, Mock(name="clock"))
    texts = [repr(settings), repr(components), caplog.text]
    for text in texts:
        assert API_KEY not in text
        assert WEBHOOK_SECRET not in text


def test_importar_composition_nao_carrega_sdk_remoto() -> None:
    code = (
        "import sys, cnes_infra.billing.composition; "
        "print([m for m in ('stripe','boto3','botocore') if m in sys.modules])"
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", code], capture_output=True, text=True, check=True,
    )
    assert result.stdout.strip() == "[]"


def test_build_webhook_recovery_propaga_o_enforcer_ao_projetor() -> None:
    components, _, _, storage, clock = _build()
    enforcer = Mock()
    dependencies = ProjectorDependencies(
        components.inbox, components.catalog, components.gateway,
        components.projection, clock, enforcer,
    )
    recovery = build_webhook_recovery(storage, dependencies)
    assert cast("Any", recovery._deps.projector)._deps.enforcer is enforcer
    assert cast("Any", recovery._deps.cursor)._client is storage.client
