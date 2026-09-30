"""Cursores de recovery de reservas inválidos são rejeitados como erro permanente."""

import base64

import pytest

from cnes_domain.billing.errors import PermanentBillingError
from cnes_domain.billing.inbox import ReservationRecoveryRequest
from packages.cnes_infra.tests.billing.quota_support import quota_env


def _encode(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode()).decode()


_FOREIGN_KEY = '{"gsi1pk": "OTHER", "gsi1sk": "s", "pk": "p", "sk": "s"}'


@pytest.mark.parametrize(
    "cursor",
    [
        "@@@",
        _encode("[1]"),
        _encode('{"pk": 1}'),
        "bm90LWpzb24",
        _encode("{}"),
        _encode('{"pk": "x"}'),
        _encode(_FOREIGN_KEY),
        _encode(_FOREIGN_KEY.replace('"p"', "1")),
        _encode(_FOREIGN_KEY.replace('"OTHER"', '"QUOTA_RESERVATION#DUE"').replace('"p"', '""')),
    ],
)
def test_rejeita_cursor_invalido(cursor: str) -> None:
    with quota_env() as env:
        request = ReservationRecoveryRequest(now=env.clock.now(), limit=10, cursor=cursor)
        with pytest.raises(PermanentBillingError) as error:
            env.repo.reconcile_expired_reservations(request)

        assert error.value.code == "invalid_recovery_cursor"
