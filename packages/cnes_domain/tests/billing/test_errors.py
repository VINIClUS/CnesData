"""Testes dos erros estáveis de billing."""

import pytest

from cnes_domain.billing.errors import (
    BillingDependencyError,
    BillingDisabledError,
    BillingError,
    BillingTenantConflict,
    EntitlementDenied,
    IdempotencyConflict,
    ImmutablePlanConflict,
    PermanentBillingError,
    PublishDenied,
    QuotaExceeded,
    RetryableBillingError,
    StaleInboxClaim,
)


@pytest.mark.parametrize(
    ("error_type", "code"),
    [
        (EntitlementDenied, "entitlement_denied"),
        (ImmutablePlanConflict, "immutable_plan_conflict"),
        (BillingTenantConflict, "billing_tenant_conflict"),
        (QuotaExceeded, "quota_exceeded"),
        (IdempotencyConflict, "idempotency_conflict"),
        (BillingDisabledError, "billing_disabled"),
        (PublishDenied, "publish_denied"),
    ],
)
def test_erro_de_dominio_expoe_codigo_estavel(error_type: type[BillingError], code: str) -> None:
    error = error_type("key=value")
    assert error.code == code
    assert str(error) == "key=value"
    assert isinstance(error, BillingError)


def test_claim_obsoleto_usa_codigo_inbox_claim_stale() -> None:
    assert str(StaleInboxClaim()) == "code=inbox_claim_stale"
    error = StaleInboxClaim("evt_01")
    assert error.code == "inbox_claim_stale"
    assert str(error) == "code=inbox_claim_stale event_id=evt_01"


@pytest.mark.parametrize(
    "error_type", [RetryableBillingError, PermanentBillingError, BillingDependencyError],
)
def test_erro_classificado_expoe_codigo_sanitizado(error_type: type[BillingError]) -> None:
    error = error_type("stripe_unavailable")
    assert error.code == "stripe_unavailable"
    assert str(error) == "code=stripe_unavailable"


@pytest.mark.parametrize(
    "raw", ["", "Stripe Unavailable", '{"card": "4242"}', "a" * 65, "9x", None],
)
def test_erro_classificado_rejeita_payload_cru(raw: object) -> None:
    with pytest.raises(ValueError, match="unsanitized_error_code"):
        RetryableBillingError(raw)  # type: ignore[arg-type]


def test_dependencia_indisponivel_e_retryable() -> None:
    assert issubclass(BillingDependencyError, RetryableBillingError)
    assert not issubclass(PermanentBillingError, RetryableBillingError)


def test_erro_classificado_aceita_detalhe_chave_valor_sanitizado() -> None:
    error = RetryableBillingError("stripe_price_unmapped", detail="price_id=price_123")
    assert error.code == "stripe_price_unmapped"
    assert str(error) == "code=stripe_price_unmapped price_id=price_123"
    multi = PermanentBillingError("stripe_mapping", detail="customer_id=cus_1 count=2")
    assert str(multi) == "code=stripe_mapping customer_id=cus_1 count=2"


@pytest.mark.parametrize(
    "detail", ["", "price 123", '{"card": "4242"}', "k=v=w", "=v", "k=", "k=v  x=y"],
)
def test_erro_classificado_rejeita_detalhe_nao_sanitizado(detail: str) -> None:
    with pytest.raises(ValueError, match="unsanitized_error_detail"):
        RetryableBillingError("stripe_mapping", detail=detail)
