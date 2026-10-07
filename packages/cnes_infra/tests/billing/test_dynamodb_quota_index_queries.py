"""Cursores inválidos e falhas de rede nas consultas de índice das reservas."""

import base64
from typing import Any

import pytest
from botocore.exceptions import EndpointConnectionError

from cnes_domain.billing.commands import ConsumeReservationCommand
from cnes_domain.billing.errors import BillingDependencyError, PermanentBillingError
from cnes_domain.billing.inbox import ReservationRecoveryRequest
from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations
from packages.cnes_infra.tests.billing.billing_factories import TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import (
    make_capacity_command,
    make_reserve_command,
    quota_env,
)


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


class _Unreachable:
    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def query(self, **_kwargs: Any) -> Any:
        raise EndpointConnectionError(endpoint_url="http://dynamodb.invalid")


def test_falha_de_rede_na_descoberta_vira_dependencia_indisponivel() -> None:
    with quota_env() as env:
        repo = DynamoQuotaReservations(_Unreachable(env.client), TABLE_NAME, env.clock.now)
        request = ReservationRecoveryRequest(now=env.clock.now(), limit=10, cursor=None)
        with pytest.raises(BillingDependencyError):
            repo.reconcile_expired_reservations(request)


def test_falha_de_rede_no_localizador_vira_dependencia_indisponivel() -> None:
    with quota_env() as env:
        repo = DynamoQuotaReservations(_Unreachable(env.client), TABLE_NAME, env.clock.now)
        command = ConsumeReservationCommand("ba_01", "res-01", 10, env.clock.now())
        with pytest.raises(BillingDependencyError):
            repo.consume(command)


class _Offline(_Unreachable):
    def __init__(self, inner: Any, operation: str) -> None:
        super().__init__(inner)
        self._operation = operation

    def __getattr__(self, name: str) -> Any:
        if name == self._operation:
            return self._fail
        return getattr(self._inner, name)

    def _fail(self, **_kwargs: Any) -> Any:
        raise EndpointConnectionError(endpoint_url="http://dynamodb.invalid")


@pytest.mark.parametrize("operation", ["get_item", "transact_write_items"])
def test_falha_de_rede_na_reserva_vira_dependencia_indisponivel(operation: str) -> None:
    with quota_env() as env:
        client = _Offline(env.client, operation)
        repo = DynamoQuotaReservations(client, TABLE_NAME, env.clock.now)
        with pytest.raises(BillingDependencyError):
            repo.reserve_and_create_run(make_reserve_command())
        with pytest.raises(BillingDependencyError):
            repo.reserve_capacity(make_capacity_command())
