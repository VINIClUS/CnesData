"""Testes do verificador de assinatura de webhooks Stripe com SDK simulado."""

import hashlib
import hmac
import json
import sys
from datetime import UTC, datetime
from types import ModuleType, SimpleNamespace

import pytest

from cnes_domain.billing.errors import PermanentBillingError
from cnes_infra.billing.webhook_verifier import StripeWebhookVerifier

SIGNING_KEY = "whsec_test"
TIMESTAMP = 1_780_000_000
CREATED = 1_780_000_123


class StripeError(Exception):
    pass


class SignatureVerificationError(StripeError):
    pass


def sign(payload: bytes, secret: str = SIGNING_KEY, timestamp: int = TIMESTAMP) -> str:
    signed = f"{timestamp}.{payload.decode()}".encode()
    digest = hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={digest}"


def to_namespace(value):
    if isinstance(value, dict):
        return SimpleNamespace(**{key: to_namespace(item) for key, item in value.items()})
    if isinstance(value, list):
        return [to_namespace(item) for item in value]
    return value


def construct_event(payload: bytes, sig_header: str, secret: str):
    parsed = sig_header.split(",")
    timestamp = int(parsed[0].removeprefix("t="))
    if not hmac.compare_digest(sign(payload, secret, timestamp), sig_header):
        raise SignatureVerificationError("no signatures found matching")
    return to_namespace(json.loads(payload))


@pytest.fixture
def fake_stripe(monkeypatch):
    module = ModuleType("stripe")
    module.StripeError = StripeError
    module.SignatureVerificationError = SignatureVerificationError
    module.Webhook = SimpleNamespace(construct_event=construct_event)
    monkeypatch.setitem(sys.modules, "stripe", module)
    return module


@pytest.fixture
def verifier(fake_stripe):
    return StripeWebhookVerifier(SIGNING_KEY)


def body(event_type: str, obj: dict, **overrides) -> bytes:
    event = {"id": "evt_1", "type": event_type, "created": CREATED, "data": {"object": obj}}
    event.update(overrides)
    return json.dumps(event).encode()


def verify(verifier, payload: bytes):
    return verifier.verify(payload, sign(payload))


INVOICE_PARENT = {"subscription_details": {"subscription": "sub_1"}}
CASES = [
    ("invoice.paid", {"object": "invoice", "customer": "cus_1", "parent": INVOICE_PARENT}),
    (
        "invoice.payment_failed",
        {"object": "invoice", "customer": "cus_1", "parent": INVOICE_PARENT},
    ),
    (
        "invoice.payment_action_required",
        {"object": "invoice", "customer": "cus_1", "parent": INVOICE_PARENT},
    ),
    (
        "checkout.session.completed",
        {"object": "checkout.session", "customer": "cus_1", "subscription": "sub_1"},
    ),
    (
        "customer.subscription.created",
        {"object": "subscription", "id": "sub_1", "customer": "cus_1"},
    ),
    (
        "customer.subscription.updated",
        {"object": "subscription", "id": "sub_1", "customer": "cus_1"},
    ),
    (
        "customer.subscription.deleted",
        {"object": "subscription", "id": "sub_1", "customer": "cus_1"},
    ),
    (
        "customer.subscription.paused",
        {"object": "subscription", "id": "sub_1", "customer": "cus_1"},
    ),
    (
        "customer.subscription.resumed",
        {"object": "subscription", "id": "sub_1", "customer": "cus_1"},
    ),
]


@pytest.mark.parametrize(("event_type", "obj"), CASES)
def test_mapeia_cliente_e_assinatura_por_tipo(verifier, event_type, obj):
    event = verify(verifier, body(event_type, obj))
    assert event.event_type == event_type
    assert event.stripe_customer_id == "cus_1"
    assert event.stripe_subscription_id == "sub_1"


def test_resumo_de_entitlements_mapeia_apenas_cliente(verifier):
    obj = {"object": "entitlements.active_entitlement_summary", "customer": "cus_1"}
    event = verify(verifier, body("entitlements.active_entitlement_summary.updated", obj))
    assert event.stripe_customer_id == "cus_1"
    assert event.stripe_subscription_id is None


def test_tipo_desconhecido_usa_fallback_generico(verifier):
    obj = {"object": "charge", "customer": "cus_2", "subscription": "sub_2"}
    event = verify(verifier, body("charge.succeeded", obj))
    assert (event.stripe_customer_id, event.stripe_subscription_id) == ("cus_2", "sub_2")


def test_tipo_desconhecido_sem_cliente_nem_assinatura(verifier):
    event = verify(verifier, body("charge.succeeded", {"object": "charge"}))
    assert event.stripe_customer_id is None
    assert event.stripe_subscription_id is None


def test_aceita_ids_expandidos_como_objeto(verifier):
    obj = {
        "object": "invoice",
        "customer": {"id": "cus_9", "object": "customer"},
        "parent": {"subscription_details": {"subscription": {"id": "sub_9"}}},
    }
    event = verify(verifier, body("invoice.paid", obj))
    assert (event.stripe_customer_id, event.stripe_subscription_id) == ("cus_9", "sub_9")


def test_fatura_sem_parent_nao_tem_assinatura(verifier):
    obj = {"object": "invoice", "customer": "cus_1", "parent": None}
    event = verify(verifier, body("invoice.paid", obj))
    assert event.stripe_subscription_id is None


def test_id_expandido_sem_id_textual_vira_ausente(verifier):
    obj = {"object": "checkout.session", "customer": {"id": 7}, "subscription": 5}
    event = verify(verifier, body("checkout.session.completed", obj))
    assert event.stripe_customer_id is None
    assert event.stripe_subscription_id is None


def test_calcula_sha256_do_body_raw_e_created_utc(verifier):
    payload = body("invoice.paid", CASES[0][1])
    event = verify(verifier, payload)
    assert event.payload_sha256 == hashlib.sha256(payload).hexdigest()
    assert event.created_at == datetime.fromtimestamp(CREATED, tz=UTC)
    assert event.event_id == "evt_1"


def test_rejeita_assinatura_invalida_sem_vazar_payload(verifier):
    payload = body("invoice.paid", CASES[0][1], id="evt_segredo_xyz")
    with pytest.raises(PermanentBillingError) as raised:
        verifier.verify(payload, sign(payload, "whsec_outro"))
    assert raised.value.code == "stripe_signature_invalid"
    assert "evt_segredo_xyz" not in str(raised.value)
    assert raised.value.__cause__ is None


def test_rejeita_body_adulterado(verifier):
    payload = body("invoice.paid", CASES[0][1])
    signature = sign(payload)
    with pytest.raises(PermanentBillingError) as raised:
        verifier.verify(payload.replace(b"cus_1", b"cus_2"), signature)
    assert raised.value.code == "stripe_signature_invalid"


def test_rejeita_json_invalido_como_assinatura_invalida(verifier):
    payload = b"{nao-json"
    with pytest.raises(PermanentBillingError) as raised:
        verifier.verify(payload, sign(payload))
    assert raised.value.code == "stripe_signature_invalid"


def schema_payloads() -> dict[str, bytes]:
    valid = json.loads(body("invoice.paid", CASES[0][1]))
    no_id = {key: value for key, value in valid.items() if key != "id"}
    no_object = {**valid, "data": {}}
    return {
        "sem_id": json.dumps(no_id).encode(),
        "sem_data_object": json.dumps(no_object).encode(),
        "created_nao_inteiro": json.dumps({**valid, "created": "hoje"}).encode(),
        "id_em_branco": json.dumps({**valid, "id": "  "}).encode(),
    }


@pytest.mark.parametrize("caso", list(schema_payloads()))
def test_rejeita_evento_com_schema_invalido(verifier, caso):
    payload = schema_payloads()[caso]
    with pytest.raises(PermanentBillingError) as raised:
        verify(verifier, payload)
    assert raised.value.code == "stripe_event_schema_invalid"


@pytest.mark.parametrize("secret", ["", "   "])
def test_rejeita_segredo_em_branco(secret):
    with pytest.raises(ValueError, match="reason=blank_webhook_secret"):
        StripeWebhookVerifier(secret)


def test_propaga_erro_do_sdk_que_nao_e_de_assinatura(fake_stripe, verifier):
    def explode(payload, sig_header, secret):
        raise RuntimeError("sdk_quebrado")

    fake_stripe.Webhook = SimpleNamespace(construct_event=explode)
    with pytest.raises(RuntimeError, match="sdk_quebrado"):
        verify(verifier, b"{}")
