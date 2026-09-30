"""Reserva de checkout pendente no catálogo DynamoDB de billing."""

import math
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError, ReadTimeoutError

from cnes_domain.billing.commands import (
    ReleasePendingCheckoutCommand,
    ReservePendingCheckoutCommand,
)
from cnes_domain.billing.errors import (
    BillingDependencyError,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.ports import BillingCatalogPort
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_items import CORRUPT_CODE, utc_attribute
from cnes_infra.billing.keys import pending_checkout_key
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.dynamodb_catalog_support import (
    catalog_env,
    get_stored,
    put,
)

ACCOUNT = "ba_01"
KEY_A = "a" * 64
KEY_B = "b" * 64
TTL = timedelta(minutes=30)
KEY = pending_checkout_key(ACCOUNT)
THROTTLED = "ProvisionedThroughputExceededException"
NETWORK_FAULTS = [
    EndpointConnectionError(endpoint_url="http://x"),
    ReadTimeoutError(endpoint_url="http://x"),
]
CONDITIONAL_FAILED = "ConditionalCheckFailedException"


def reserve(key: str = KEY_A, expires_in: timedelta = TTL) -> ReservePendingCheckoutCommand:
    return ReservePendingCheckoutCommand(ACCOUNT, key, NOW + expires_in)


def reserve_until(expires_at: datetime, key: str = KEY_A) -> ReservePendingCheckoutCommand:
    return ReservePendingCheckoutCommand(ACCOUNT, key, expires_at)


def release(key: str = KEY_A, reserved_at: datetime = NOW) -> ReleasePendingCheckoutCommand:
    return ReleasePendingCheckoutCommand(ACCOUNT, key, reserved_at)


def client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "fault"}}, operation)


class FaultyClient:
    def __init__(
        self,
        inner: Any,
        blind_reads: int = 0,
        fail_on: str | None = None,
        fault: Exception | None = None,
        after_read: Callable[[], None] | None = None,
    ) -> None:
        self.inner = inner
        self.blind_reads = blind_reads
        self.fail_on = fail_on
        self.fault = fault
        self.after_read = after_read
        self.calls: list[str] = []

    def _raise_if_failing(self, name: str) -> None:
        self.calls.append(name)
        if name == self.fail_on:
            raise self.fault or client_error(THROTTLED, name)

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        self._raise_if_failing("get_item")
        if self.blind_reads > 0:
            self.blind_reads -= 1
            return {}
        response = self.inner.get_item(**kwargs)
        if self.after_read is not None:
            self.after_read()
            self.after_read = None
        return response

    def __getattr__(self, name: str) -> Any:
        target = getattr(self.inner, name)
        if not callable(target):
            return target

        def call(**kwargs: Any) -> Any:
            self._raise_if_failing(name)
            return target(**kwargs)

        return call


def faulty(client: Any, clock: Any, **options: Any) -> DynamoBillingCatalog:
    return DynamoBillingCatalog(FaultyClient(client, **options), TABLE_NAME, clock.now)


def test_reserva_checkout_pendente_grava_item_com_ttl() -> None:
    expires = NOW + TTL + timedelta(microseconds=1)
    with catalog_env() as (client, _, catalog):
        pending = catalog.reserve_pending_checkout(
            ReservePendingCheckoutCommand(ACCOUNT, KEY_A, expires)
        )
        item = get_stored(client, KEY)
    assert (pending.request_key, pending.reserved_at, pending.expires_at) == (KEY_A, NOW, expires)
    assert pending.replayed is False
    assert item == {
        **item_key(*KEY),
        "entity": {"S": "PENDINGCHECKOUT"},
        "billing_account_id": {"S": ACCOUNT},
        "request_key": {"S": KEY_A},
        "reserved_at": {"S": utc_attribute(NOW)},
        "reservation_expires_at": {"S": utc_attribute(expires)},
        "expires_at": {"N": str(math.ceil(expires.timestamp()))},
    }
    assert int(item["expires_at"]["N"]) > expires.timestamp()


def test_replay_com_mesma_chave_proximo_da_expiracao_estende_a_reserva() -> None:
    with catalog_env() as (client, clock, catalog):
        first = catalog.reserve_pending_checkout(reserve())
        clock.advance(TTL - timedelta(milliseconds=1))
        extended_to = clock.now() + TTL
        replay = catalog.reserve_pending_checkout(reserve_until(extended_to))
        item = get_stored(client, KEY)
    assert (replay.reserved_at, replay.expires_at) == (first.reserved_at, extended_to)
    assert replay.replayed is True
    assert item["reserved_at"] == {"S": utc_attribute(NOW)}
    assert item["reservation_expires_at"] == {"S": utc_attribute(extended_to)}
    assert item["expires_at"] == {"N": str(math.ceil(extended_to.timestamp()))}


def test_outra_chave_no_instante_da_expiracao_original_segue_em_progresso() -> None:
    with catalog_env() as (_, clock, catalog):
        catalog.reserve_pending_checkout(reserve())
        clock.advance(TTL - timedelta(milliseconds=1))
        catalog.reserve_pending_checkout(reserve_until(NOW + TTL * 2))
        clock.advance(timedelta(milliseconds=1))
        with pytest.raises(PermanentBillingError) as raised:
            catalog.reserve_pending_checkout(reserve(KEY_B, TTL * 2))
    assert raised.value.code == "checkout_in_progress"


@pytest.mark.parametrize("expires_in", [TTL, TTL - timedelta(minutes=10)])
def test_replay_com_expiracao_nao_posterior_nao_regrava(expires_in: timedelta) -> None:
    with catalog_env() as (client, clock, catalog):
        first = catalog.reserve_pending_checkout(reserve())
        before = get_stored(client, KEY)
        clock.advance(timedelta(minutes=5))
        spy = FaultyClient(client)
        spied = DynamoBillingCatalog(spy, TABLE_NAME, clock.now)
        replay = spied.reserve_pending_checkout(reserve(expires_in=expires_in))
        assert get_stored(client, KEY) == before
    assert replay == replace(first, replayed=True)
    assert replay.replayed is True
    assert "update_item" not in spy.calls


def test_falha_condicional_ao_estender_gera_conflito_retentavel() -> None:
    fault = client_error(CONDITIONAL_FAILED, "UpdateItem")
    with catalog_env() as (client, clock, catalog):
        catalog.reserve_pending_checkout(reserve())
        before = get_stored(client, KEY)
        clock.advance(timedelta(minutes=5))
        with pytest.raises(RetryableBillingError) as raised:
            faulty(client, clock, fail_on="update_item", fault=fault).reserve_pending_checkout(
                reserve(expires_in=TTL * 2)
            )
        assert get_stored(client, KEY) == before
    assert raised.value.code == "billing_transaction_conflict"
    assert not isinstance(raised.value, BillingDependencyError)


def test_falha_de_storage_ao_estender_gera_dependency_error() -> None:
    with catalog_env() as (client, clock, catalog):
        catalog.reserve_pending_checkout(reserve())
        before = get_stored(client, KEY)
        clock.advance(timedelta(minutes=5))
        with pytest.raises(BillingDependencyError) as raised:
            faulty(client, clock, fail_on="update_item").reserve_pending_checkout(
                reserve(expires_in=TTL * 2)
            )
        assert get_stored(client, KEY) == before
    assert raised.value.code == "dynamodb_unavailable"


def _superseding_items() -> dict[str, dict[str, Any] | None]:
    base = valid_item()
    live = {"S": utc_attribute(NOW + TTL)}
    return {
        "liberada": None,
        "outra_chave": {**base, "request_key": {"S": KEY_B}, "reservation_expires_at": live},
        "outro_reserved_at": {
            **base,
            "reserved_at": {"S": utc_attribute(NOW + timedelta(minutes=1))},
            "reservation_expires_at": live,
        },
        "expirada": {**base, "reservation_expires_at": {"S": utc_attribute(NOW)}},
    }


@pytest.mark.parametrize("kind", ["liberada", "outra_chave", "outro_reserved_at", "expirada"])
def test_extensao_nao_sobrescreve_reserva_alterada_entre_leitura_e_gravacao(kind: str) -> None:
    superseding = _superseding_items()[kind]

    def supersede() -> None:
        if superseding is None:
            client.delete_item(TableName=TABLE_NAME, Key=item_key(*KEY))
        else:
            put(client, superseding)

    with catalog_env() as (client, clock, catalog):
        catalog.reserve_pending_checkout(reserve())
        clock.advance(timedelta(minutes=5))
        racing = faulty(client, clock, after_read=supersede)
        with pytest.raises(RetryableBillingError) as raised:
            racing.reserve_pending_checkout(reserve(expires_in=TTL * 2))
        assert get_stored(client, KEY) == superseding
    assert raised.value.code == "billing_transaction_conflict"


def test_reserva_com_outra_chave_viva_gera_checkout_in_progress() -> None:
    with catalog_env() as (client, _, catalog):
        catalog.reserve_pending_checkout(reserve(KEY_A))
        before = get_stored(client, KEY)
        with pytest.raises(PermanentBillingError) as raised:
            catalog.reserve_pending_checkout(reserve(KEY_B))
        assert get_stored(client, KEY) == before
    assert raised.value.code == "checkout_in_progress"


def test_reserva_com_outra_chave_apos_expirar_substitui() -> None:
    with catalog_env() as (client, clock, catalog):
        catalog.reserve_pending_checkout(reserve(KEY_A))
        clock.advance(TTL)
        pending = catalog.reserve_pending_checkout(reserve(KEY_B, TTL * 2))
        item = get_stored(client, KEY)
    assert pending.request_key == KEY_B
    assert pending.reserved_at == NOW + TTL
    assert item["request_key"] == {"S": KEY_B}


def test_reserva_mesma_chave_expirada_renova() -> None:
    with catalog_env() as (_, clock, catalog):
        first = catalog.reserve_pending_checkout(reserve())
        clock.advance(TTL + timedelta(seconds=1))
        renewed = catalog.reserve_pending_checkout(reserve(expires_in=TTL * 2))
    assert renewed.reserved_at > first.reserved_at
    assert renewed.expires_at == NOW + TTL * 2


@pytest.mark.parametrize("delta", [timedelta(0), timedelta(seconds=-1)])
def test_reserva_rejeita_expiracao_no_passado(delta: timedelta) -> None:
    with catalog_env() as (client, _, catalog):
        with pytest.raises(ValueError, match="reason=pending_checkout_expired"):
            catalog.reserve_pending_checkout(reserve(expires_in=delta))
        assert get_stored(client, KEY) is None


def test_corrida_com_mesma_chave_viva_devolve_reserva_gravada() -> None:
    with catalog_env() as (client, clock, catalog):
        stored = catalog.reserve_pending_checkout(reserve())
        clock.advance(timedelta(minutes=1))
        lost_put = faulty(
            client,
            clock,
            blind_reads=1,
            fail_on="put_item",
            fault=client_error(CONDITIONAL_FAILED, "PutItem"),
        )
        raced = lost_put.reserve_pending_checkout(reserve())
    assert raced == replace(stored, replayed=True)


def test_corrida_com_mesma_chave_viva_nao_sobrescreve_a_reserva_gravada() -> None:
    with catalog_env() as (client, clock, catalog):
        stored = catalog.reserve_pending_checkout(reserve())
        clock.advance(timedelta(minutes=1))
        raced = faulty(client, clock, blind_reads=1).reserve_pending_checkout(reserve())
        persisted = get_stored(client, KEY)
    assert raced == replace(stored, replayed=True)
    assert persisted is not None
    assert persisted["reserved_at"] == {"S": utc_attribute(stored.reserved_at)}


def test_corrida_com_outra_chave_viva_gera_checkout_in_progress() -> None:
    with catalog_env() as (client, clock, catalog):
        catalog.reserve_pending_checkout(reserve(KEY_A))
        with pytest.raises(PermanentBillingError) as raised:
            faulty(client, clock, blind_reads=1).reserve_pending_checkout(reserve(KEY_B))
    assert raised.value.code == "checkout_in_progress"


def test_corrida_sem_reserva_visivel_gera_conflito_retentavel() -> None:
    with catalog_env() as (client, clock, catalog):
        catalog.reserve_pending_checkout(reserve(KEY_A))
        with pytest.raises(RetryableBillingError) as raised:
            faulty(client, clock, blind_reads=2).reserve_pending_checkout(reserve(KEY_B))
    assert raised.value.code == "billing_transaction_conflict"
    assert not isinstance(raised.value, BillingDependencyError)


def test_falha_de_storage_ao_gravar_reserva_gera_dependency_error() -> None:
    with catalog_env() as (client, clock, _):
        with pytest.raises(BillingDependencyError) as raised:
            faulty(client, clock, fail_on="put_item").reserve_pending_checkout(reserve())
        assert get_stored(client, KEY) is None
    assert raised.value.code == "dynamodb_unavailable"


def test_falha_de_storage_ao_liberar_reserva_gera_dependency_error() -> None:
    with catalog_env() as (client, clock, catalog):
        catalog.reserve_pending_checkout(reserve())
        with pytest.raises(BillingDependencyError) as raised:
            faulty(client, clock, fail_on="delete_item").release_pending_checkout(release())
        assert get_stored(client, KEY) is not None
    assert raised.value.code == "dynamodb_unavailable"


@pytest.mark.parametrize("fault", NETWORK_FAULTS, ids=type)
def test_falha_de_rede_ao_ler_reserva_gera_dependency_error(fault: Exception) -> None:
    with catalog_env() as (client, clock, _):
        with pytest.raises(BillingDependencyError) as raised:
            faulty(client, clock, fail_on="get_item", fault=fault).reserve_pending_checkout(
                reserve()
            )
    assert raised.value.code == "dynamodb_unavailable"


@pytest.mark.parametrize("fault", NETWORK_FAULTS, ids=type)
def test_falha_de_rede_ao_gravar_reserva_gera_dependency_error(fault: Exception) -> None:
    with catalog_env() as (client, clock, _):
        with pytest.raises(BillingDependencyError) as raised:
            faulty(client, clock, fail_on="put_item", fault=fault).reserve_pending_checkout(
                reserve()
            )
        assert get_stored(client, KEY) is None
    assert raised.value.code == "dynamodb_unavailable"


@pytest.mark.parametrize("fault", NETWORK_FAULTS, ids=type)
def test_falha_de_rede_ao_estender_reserva_gera_dependency_error(fault: Exception) -> None:
    with catalog_env() as (client, clock, catalog):
        catalog.reserve_pending_checkout(reserve())
        before = get_stored(client, KEY)
        clock.advance(timedelta(minutes=5))
        with pytest.raises(BillingDependencyError) as raised:
            faulty(client, clock, fail_on="update_item", fault=fault).reserve_pending_checkout(
                reserve(expires_in=TTL * 2)
            )
        assert get_stored(client, KEY) == before
    assert raised.value.code == "dynamodb_unavailable"


@pytest.mark.parametrize("fault", NETWORK_FAULTS, ids=type)
def test_falha_de_rede_ao_liberar_reserva_gera_dependency_error(fault: Exception) -> None:
    with catalog_env() as (client, clock, catalog):
        catalog.reserve_pending_checkout(reserve())
        with pytest.raises(BillingDependencyError) as raised:
            faulty(client, clock, fail_on="delete_item", fault=fault).release_pending_checkout(
                release()
            )
        assert get_stored(client, KEY) is not None
    assert raised.value.code == "dynamodb_unavailable"


def valid_item() -> dict[str, Any]:
    return {
        **item_key(*KEY),
        "entity": {"S": "PENDINGCHECKOUT"},
        "billing_account_id": {"S": ACCOUNT},
        "request_key": {"S": KEY_A},
        "reserved_at": {"S": utc_attribute(NOW)},
        "reservation_expires_at": {"S": utc_attribute(NOW + TTL)},
        "expires_at": {"N": "1"},
    }


def corrupt_variants() -> list[dict[str, Any]]:
    base = valid_item()
    variants = [
        {**base, "entity": {"S": "OTHER"}},
        {**base, "billing_account_id": {"S": "ba_99"}},
        {**base, "request_key": {"S": "not-a-sha"}},
        {**base, "reserved_at": {"S": "yesterday"}},
        {**base, "reserved_at": {"S": "2026-09-30T12:00:00.000000"}},
        {**base, "reservation_expires_at": {"S": utc_attribute(NOW - TTL)}},
    ]
    without = dict(base)
    del without["request_key"]
    return [*variants, without]


@pytest.mark.parametrize("item", corrupt_variants())
def test_item_corrompido_gera_erro_permanente(item: dict[str, Any]) -> None:
    with catalog_env() as (client, _, catalog):
        put(client, item)
        with pytest.raises(PermanentBillingError) as raised:
            catalog.reserve_pending_checkout(reserve())
    assert raised.value.code == CORRUPT_CODE
    assert "entity=pending_checkout" in str(raised.value)


def test_item_valido_gravado_externamente_e_reconhecido() -> None:
    with catalog_env() as (client, _, catalog):
        put(client, valid_item())
        pending = catalog.reserve_pending_checkout(reserve())
    assert (pending.request_key, pending.reserved_at) == (KEY_A, NOW)
    assert pending.replayed is True


def test_libera_com_mesma_chave_remove_a_reserva() -> None:
    with catalog_env() as (client, _, catalog):
        catalog.reserve_pending_checkout(reserve())
        assert catalog.release_pending_checkout(release()) is True
        assert get_stored(client, KEY) is None
        assert catalog.reserve_pending_checkout(reserve(KEY_B)).request_key == KEY_B


def test_libera_com_outra_chave_preserva_a_reserva() -> None:
    with catalog_env() as (client, _, catalog):
        catalog.reserve_pending_checkout(reserve(KEY_A))
        before = get_stored(client, KEY)
        assert catalog.release_pending_checkout(release(KEY_B)) is False
        assert get_stored(client, KEY) == before


def test_libera_com_outro_reserved_at_preserva_a_reserva_mais_nova() -> None:
    with catalog_env() as (client, clock, catalog):
        stale = catalog.reserve_pending_checkout(reserve())
        clock.advance(TTL)
        catalog.reserve_pending_checkout(reserve(expires_in=TTL * 2))
        before = get_stored(client, KEY)
        assert catalog.release_pending_checkout(release(reserved_at=stale.reserved_at)) is False
        assert get_stored(client, KEY) == before


def test_libera_reserva_inexistente_retorna_falso() -> None:
    with catalog_env() as (_, _, catalog):
        assert catalog.release_pending_checkout(release()) is False


def test_catalogo_continua_implementando_a_porta_de_billing() -> None:
    with catalog_env() as (_, _, catalog):
        assert isinstance(catalog, BillingCatalogPort)
