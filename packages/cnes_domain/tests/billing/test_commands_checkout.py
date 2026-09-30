"""Testes dos value objects da reserva de checkout pendente."""

import dataclasses
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from cnes_domain.billing.commands import (
    PendingCheckout,
    ReleasePendingCheckoutCommand,
    ReservePendingCheckoutCommand,
)

_NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
_LATER = _NOW + timedelta(minutes=15)
_NAIVE = _NOW.replace(tzinfo=None)
_KEY = "a" * 64


def _reserve(**o: Any) -> ReservePendingCheckoutCommand:
    values = {"billing_account_id": "ba_1", "request_key": _KEY, "expires_at": _LATER}
    return ReservePendingCheckoutCommand(**{**values, **o})


def _release(**o: Any) -> ReleasePendingCheckoutCommand:
    return ReleasePendingCheckoutCommand(**{"billing_account_id": "ba_1", "request_key": _KEY, **o})


def _pending(**o: Any) -> PendingCheckout:
    values = {
        "billing_account_id": "ba_1",
        "request_key": _KEY,
        "reserved_at": _NOW,
        "expires_at": _LATER,
    }
    return PendingCheckout(**{**values, **o})


@pytest.mark.parametrize("factory", [_reserve, _release, _pending])
def test_value_object_de_checkout_pendente_e_imutavel(factory: Any) -> None:
    instance = factory()
    with pytest.raises(dataclasses.FrozenInstanceError):
        instance.request_key = "b" * 64


def test_pending_checkout_preserva_todos_os_campos() -> None:
    pending = _pending()
    assert (pending.billing_account_id, pending.request_key) == ("ba_1", _KEY)
    assert (pending.reserved_at, pending.expires_at) == (_NOW, _LATER)


def test_pending_checkout_aceita_expiracao_igual_a_reserva() -> None:
    assert _pending(expires_at=_NOW).expires_at == _NOW


_INVALID: list[tuple[Any, dict[str, Any], str]] = [
    (_reserve, {"billing_account_id": ""}, "blank_value"),
    (_reserve, {"request_key": "client-key"}, "invalid_sha256"),
    (_reserve, {"expires_at": _NAIVE}, "datetime_not_utc"),
    (_release, {"billing_account_id": " "}, "blank_value"),
    (_release, {"request_key": "A" * 64}, "invalid_sha256"),
    (_pending, {"billing_account_id": ""}, "blank_value"),
    (_pending, {"request_key": "abc"}, "invalid_sha256"),
    (_pending, {"reserved_at": _NAIVE}, "datetime_not_utc"),
    (_pending, {"expires_at": _NAIVE}, "datetime_not_utc"),
    (_pending, {"expires_at": _NOW - timedelta(seconds=1)}, "expiry_before_reservation"),
]


@pytest.mark.parametrize(
    ("factory", "overrides", "reason"),
    _INVALID,
    ids=[f"{f.__name__}-{next(iter(o))}-{r}" for f, o, r in _INVALID],
)
def test_rejeita_campo_invalido_do_checkout_pendente(
    factory: Any, overrides: dict[str, Any], reason: str,
) -> None:
    with pytest.raises(ValueError, match=f"reason={reason}"):
        factory(**overrides)
