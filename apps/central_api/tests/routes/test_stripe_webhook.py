"""Testes da rota de webhook Stripe: body raw, verificação antes de persistir."""

import hashlib
import hmac
import json
import logging
import sys
from datetime import UTC, datetime
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from central_api.routes.stripe_webhook import (
    get_stripe_webhook_verifier,
    get_webhook_inbox,
    router,
)
from cnes_domain.billing.errors import BillingDependencyError, PermanentBillingError
from cnes_domain.billing.inbox import InboxAcceptResult, InboxDisposition, StripeEvent
from cnes_infra.billing.webhook_verifier import StripeWebhookVerifier

URL = "/api/v1/billing/webhooks/stripe"
PAYLOAD = b'{"id":"evt_01","type":"invoice.paid"}'
SIGNATURE = "t=1,v1=abc"
SIGNING_KEY = "whsec_route"
EVENT = StripeEvent(
    "evt_01", "invoice.paid", datetime(2026, 9, 30, tzinfo=UTC), "cus_1", "sub_1", "a" * 64,
)


class Env:
    def __init__(self):
        self.verifier = Mock()
        self.verifier.verify.return_value = EVENT
        self.inbox = Mock()
        self.inbox.accept.return_value = InboxAcceptResult("evt_01", InboxDisposition.ACCEPTED)
        self.app = FastAPI()
        self.app.include_router(router)
        self.app.dependency_overrides[get_stripe_webhook_verifier] = lambda: self.verifier
        self.app.dependency_overrides[get_webhook_inbox] = lambda: self.inbox
        self.client = TestClient(self.app)

    def post(self, content=PAYLOAD, signature=SIGNATURE):
        headers = {} if signature is None else {"Stripe-Signature": signature}
        return self.client.post(URL, content=content, headers=headers)


@pytest.fixture
def env():
    return Env()


def test_webhook_valida_assinatura_sobre_body_raw(env):
    response = env.post()
    assert response.status_code == 200
    assert response.json() == {"received": True}
    env.verifier.verify.assert_called_once_with(PAYLOAD, SIGNATURE)
    env.inbox.accept.assert_called_once_with(EVENT)


def test_webhook_assinatura_invalida_nao_persiste(env):
    env.verifier.verify.side_effect = PermanentBillingError("stripe_signature_invalid")
    response = env.post()
    assert response.status_code == 400
    assert response.json() == {"detail": "stripe_signature_invalid"}
    env.inbox.accept.assert_not_called()


def test_webhook_schema_invalido_responde_400_com_codigo(env):
    env.verifier.verify.side_effect = PermanentBillingError("stripe_event_schema_invalid")
    response = env.post()
    assert response.status_code == 400
    assert response.json() == {"detail": "stripe_event_schema_invalid"}
    env.inbox.accept.assert_not_called()


@pytest.mark.parametrize("signature", [None, "", "   "])
def test_webhook_sem_cabecalho_de_assinatura_responde_400(env, signature):
    response = env.post(signature=signature)
    assert response.status_code == 400
    assert response.json() == {"detail": "stripe_signature_invalid"}
    env.verifier.verify.assert_not_called()
    env.inbox.accept.assert_not_called()


@pytest.mark.parametrize("disposition", list(InboxDisposition))
def test_webhook_responde_200_para_toda_disposicao_aceita(env, disposition):
    env.inbox.accept.return_value = InboxAcceptResult("evt_01", disposition)
    response = env.post()
    assert response.status_code == 200
    assert response.json() == {"received": True}


def test_webhook_falha_de_dependencia_responde_503_com_retry_after(env):
    env.inbox.accept.side_effect = BillingDependencyError("dynamodb_unavailable")
    response = env.post()
    assert response.status_code == 503
    assert response.json() == {"detail": "billing_dependency_unavailable"}
    assert response.headers["Retry-After"] == "5"


def test_webhook_repassa_bytes_do_body_sem_alteracao(env):
    payload = '{ "id" : "evt_ü",\n "type":"invoice.paid" }  '.encode()
    env.post(content=payload)
    assert env.verifier.verify.call_args.args[0] == payload
    assert isinstance(env.verifier.verify.call_args.args[0], bytes)


def test_webhook_nao_configurado_falha_fechado_com_503():
    app = FastAPI()
    app.include_router(router)
    response = TestClient(app).post(URL, content=PAYLOAD, headers={"Stripe-Signature": "x"})
    assert response.status_code == 503
    assert response.json() == {"detail": "billing_not_configured"}


def test_webhook_inbox_nao_configurado_falha_fechado_com_503(env):
    del env.app.dependency_overrides[get_webhook_inbox]
    response = env.post()
    assert response.status_code == 503
    assert response.json() == {"detail": "billing_not_configured"}
    env.verifier.verify.assert_not_called()


def test_webhook_registra_linha_sem_payload_nem_assinatura(env, caplog):
    with caplog.at_level(logging.INFO, logger="central_api.routes.stripe_webhook"):
        env.post()
    lines = [record.getMessage() for record in caplog.records]
    expected = "stripe_webhook_received event_id=evt_01 event_type=invoice.paid "
    expected += "disposition=accepted"
    assert expected in lines
    assert all(SIGNATURE not in line and "{" not in line for line in lines)


def _sign(payload: bytes, timestamp: int = 1_780_000_000) -> str:
    signed = f"{timestamp}.{payload.decode()}".encode()
    return f"t={timestamp},v1={hmac.new(SIGNING_KEY.encode(), signed, hashlib.sha256).hexdigest()}"


class SignatureVerificationError(Exception):
    pass


def _construct_event(payload, sig_header, secret):
    timestamp = int(sig_header.split(",")[0].removeprefix("t="))
    if secret != SIGNING_KEY or _sign(payload, timestamp) != sig_header:
        raise SignatureVerificationError("mismatch")
    return json.loads(payload, object_hook=lambda item: SimpleNamespace(**item))


@pytest.fixture
def real_env(monkeypatch):
    module = ModuleType("stripe")
    module.Webhook = SimpleNamespace(construct_event=_construct_event)
    monkeypatch.setitem(sys.modules, "stripe", module)
    env = Env()
    env.app.dependency_overrides[get_stripe_webhook_verifier] = lambda: StripeWebhookVerifier(
        SIGNING_KEY,
    )
    return env


def _signed_payload() -> bytes:
    return json.dumps({
        "id": "evt_real", "type": "invoice.paid", "created": 1_780_000_000,
        "data": {"object": {"object": "invoice", "customer": "cus_1"}},
    }).encode()


def test_webhook_ponta_a_ponta_aceita_assinatura_valida(real_env):
    payload = _signed_payload()
    response = real_env.post(content=payload, signature=_sign(payload))
    assert response.status_code == 200
    accepted = real_env.inbox.accept.call_args.args[0]
    assert (accepted.event_id, accepted.stripe_customer_id) == ("evt_real", "cus_1")
    assert accepted.payload_sha256 == hashlib.sha256(payload).hexdigest()


def test_webhook_ponta_a_ponta_rejeita_body_adulterado(real_env):
    payload = _signed_payload()
    response = real_env.post(content=payload.replace(b"cus_1", b"cus_2"), signature=_sign(payload))
    assert response.status_code == 400
    assert response.json() == {"detail": "stripe_signature_invalid"}
    real_env.inbox.accept.assert_not_called()
