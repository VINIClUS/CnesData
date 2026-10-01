"""Métricas de billing emitidas pela rota de webhook Stripe."""

from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from central_api.routes.stripe_webhook import (
    STRIPE_WEBHOOK_MAX_BODY_BYTES,
    DiscardBillingMetrics,
    get_billing_metrics,
    get_stripe_webhook_verifier,
    get_webhook_inbox,
    router,
)
from cnes_domain.billing.errors import PermanentBillingError, RetryableBillingError
from cnes_domain.billing.inbox import InboxAcceptResult, InboxDisposition, StripeEvent

URL = "/api/v1/billing/webhooks/stripe"
SIGNATURE = {"Stripe-Signature": "t=1,v1=abc"}


def _event() -> StripeEvent:
    created = datetime.now(UTC) - timedelta(seconds=5)
    return StripeEvent("evt_01", "invoice.paid", created, "cus_1", "sub_1", "a" * 64)


class Env:
    def __init__(self):
        self.verifier = Mock()
        self.verifier.verify.return_value = _event()
        self.inbox = Mock()
        self.inbox.accept.return_value = InboxAcceptResult("evt_01", InboxDisposition.ACCEPTED)
        self.metrics = Mock()
        app = FastAPI()
        app.include_router(router)
        app.dependency_overrides[get_stripe_webhook_verifier] = lambda: self.verifier
        app.dependency_overrides[get_webhook_inbox] = lambda: self.inbox
        app.dependency_overrides[get_billing_metrics] = lambda: self.metrics
        self.client = TestClient(app)

    def emitted(self) -> list:
        return [call.args[0] for call in self.metrics.emit.call_args_list]


@pytest.fixture
def env():
    return Env()


def test_dependencia_padrao_descarta_metricas() -> None:
    assert isinstance(get_billing_metrics(), DiscardBillingMetrics)


def test_evento_aceito_emite_apenas_latencia(env) -> None:
    response = env.client.post(URL, content=b"{}", headers=SIGNATURE)

    assert response.status_code == 200
    [metric] = env.emitted()
    assert metric.name == "WebhookLatencyMs"
    assert metric.unit == "Milliseconds"
    assert 4000 <= metric.value < 60000


def test_evento_duplicado_emite_duplicado_e_latencia(env) -> None:
    env.inbox.accept.return_value = InboxAcceptResult("evt_01", InboxDisposition.DUPLICATE)

    env.client.post(URL, content=b"{}", headers=SIGNATURE)

    assert [m.name for m in env.emitted()] == ["WebhookDuplicates", "WebhookLatencyMs"]
    assert env.emitted()[0].value == 1


def test_assinatura_ausente_emite_falha_signature_invalid(env) -> None:
    env.client.post(URL, content=b"{}")

    [metric] = env.emitted()
    assert metric.name == "WebhookFailures"
    assert metric.dimensions == {"Reason": "signature_invalid"}


def test_assinatura_rejeitada_pelo_verificador_emite_falha(env) -> None:
    env.verifier.verify.side_effect = PermanentBillingError("stripe_signature_invalid")

    env.client.post(URL, content=b"{}", headers=SIGNATURE)

    assert env.emitted()[0].dimensions == {"Reason": "signature_invalid"}


def test_schema_invalido_emite_falha_event_invalid(env) -> None:
    env.verifier.verify.side_effect = PermanentBillingError("stripe_event_schema_invalid")

    env.client.post(URL, content=b"{}", headers=SIGNATURE)

    assert env.emitted()[0].dimensions == {"Reason": "event_invalid"}


def test_body_grande_emite_payload_too_large(env) -> None:
    body = b"x" * (STRIPE_WEBHOOK_MAX_BODY_BYTES + 1)

    response = env.client.post(URL, content=body, headers=SIGNATURE)

    assert response.status_code == 413
    assert env.emitted()[0].dimensions == {"Reason": "payload_too_large"}


def test_content_length_invalido_emite_falha(env) -> None:
    response = env.client.post(
        URL, content=b"{}", headers={**SIGNATURE, "Content-Length": "-1"},
    )

    assert response.status_code == 400
    assert env.emitted()[0].dimensions == {"Reason": "invalid_content_length"}


def test_inbox_indisponivel_emite_dependency_unavailable(env) -> None:
    env.inbox.accept.side_effect = RetryableBillingError("x")

    response = env.client.post(URL, content=b"{}", headers=SIGNATURE)

    assert response.status_code == 503
    assert [m.name for m in env.emitted()] == ["WebhookFailures"]
    assert env.emitted()[0].dimensions == {"Reason": "dependency_unavailable"}
