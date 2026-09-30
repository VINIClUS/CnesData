"""Testes das reservas de capacidade de tenants e agentes."""

import json
from dataclasses import replace
from datetime import timedelta
from functools import partial
from typing import Any

import pytest
from botocore.exceptions import ClientError

from cnes_domain.billing.commands import ConsumeCapacityCommand, ReleaseCapacityCommand
from cnes_domain.billing.errors import (
    EntitlementDenied,
    IdempotencyConflict,
    PermanentBillingError,
    QuotaExceeded,
    RetryableBillingError,
)
from cnes_domain.billing.models import CapacityKind, ReservationStatus, SubscriptionStatus
from cnes_infra.billing.dynamodb_items import deterministic_id, outbox_item
from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations
from cnes_infra.billing.dynamodb_quota_capacity import (
    CAPACITY_RESERVATION_TTL,
    CapacityTransition,
)
from cnes_infra.billing.dynamodb_quota_items import (
    CAPACITY_SCOPE,
    encode_capacity_reservation,
    quota_event,
    usage_counter,
)
from cnes_infra.billing.keys import (
    capacity_reservation_key,
    capacity_usage_key,
    entitlement_snapshot_key,
)
from cnes_infra.control_plane.dynamodb_codec import absent_check_action
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME, make_snapshot
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    HASH_B,
    TENANT,
    make_capacity_command,
    make_quota_snapshot,
    quota_env,
    seed_snapshot,
    table_items,
)

RESERVATION_ID = deterministic_id(CAPACITY_SCOPE, ACCOUNT, TENANT, "cap-01")


class _ContendedClient:
    def __init__(self, client: Any, failures: int, on_fail: Any = None) -> None:
        self._client = client
        self._failures = failures
        self._on_fail = on_fail

    def transact_write_items(self, **request: Any) -> Any:
        if self._failures <= 0:
            return self._client.transact_write_items(**request)
        self._failures -= 1
        if self._on_fail is not None:
            self._on_fail()
        response = {
            "Error": {"Code": "TransactionCanceledException", "Message": "x"},
            "CancellationReasons": [{"Code": "ConditionalCheckFailed"}],
        }
        raise ClientError(response, "TransactWriteItems")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


def _release(reservation_id: str = RESERVATION_ID) -> ReleaseCapacityCommand:
    return ReleaseCapacityCommand(ACCOUNT, reservation_id, NOW, "agent_removed")


def _consume(reservation_id: str = RESERVATION_ID) -> ConsumeCapacityCommand:
    return ConsumeCapacityCommand(ACCOUNT, reservation_id, NOW)


def _counter(env: Any, name: str = "agent_count") -> int:
    pk, sk = capacity_usage_key(ACCOUNT)
    key = {"pk": {"S": pk}, "sk": {"S": sk}}
    item = env.client.get_item(TableName=TABLE_NAME, Key=key, ConsistentRead=True).get("Item")
    return usage_counter(item, name)


def _events(env: Any) -> dict[str, dict[str, Any]]:
    items = [i for i in table_items(env.client) if i["entity"]["S"] == "OUTBOXEVENT"]
    events = [json.loads(i["payload"]["S"]) for i in items]
    return {event["event_type"]: event["payload"] for event in events}


def _event_types(env: Any) -> list[str]:
    return sorted(_events(env))


def _stored(env: Any) -> dict[str, Any]:
    key = capacity_reservation_key(ACCOUNT, RESERVATION_ID)
    return env.client.get_item(
        TableName=TABLE_NAME, Key={"pk": {"S": key[0]}, "sk": {"S": key[1]}}, ConsistentRead=True
    )["Item"]


def test_reserva_grava_reserva_idempotencia_outbox_e_contador() -> None:
    with quota_env() as env:
        reservation = env.repo.reserve_capacity(make_capacity_command())

        assert reservation.reservation_id == RESERVATION_ID
        assert reservation.status is ReservationStatus.RESERVED
        assert reservation.expires_at == NOW + CAPACITY_RESERVATION_TTL
        assert _counter(env) == 1
        assert _stored(env)["status"]["S"] == "reserved"
        assert "gsi1pk" in _stored(env)
        entities = {i["entity"]["S"] for i in table_items(env.client)}
        assert {"IDEMPOTENCYRECORD", "OUTBOXEVENT", "CAPACITYRESERVATION"} <= entities
        assert _event_types(env) == ["quota.reserved"]


def test_reserva_repetida_retorna_mesma_reserva_sem_novo_incremento() -> None:
    with quota_env() as env:
        first = env.repo.reserve_capacity(make_capacity_command())
        second = env.repo.reserve_capacity(make_capacity_command())

        assert second == first
        assert _counter(env) == 1


def test_mesma_chave_com_outro_hash_gera_conflito_de_idempotencia() -> None:
    with quota_env() as env:
        env.repo.reserve_capacity(make_capacity_command())

        with pytest.raises(IdempotencyConflict, match="key=cap-01"):
            env.repo.reserve_capacity(make_capacity_command(request_hash=HASH_B))


def test_ultimo_slot_de_agente_rejeita_segunda_reserva() -> None:
    with quota_env() as env:
        env.repo.reserve_capacity(make_capacity_command(limit=1))
        other = make_capacity_command(limit=1, idempotency_key="cap-02", resource_id="agent-02")

        with pytest.raises(QuotaExceeded, match="max_agents_exceeded limit=1"):
            env.repo.reserve_capacity(other)

        assert _counter(env) == 1


def test_ultimo_slot_de_tenant_rejeita_segunda_reserva() -> None:
    with quota_env() as env:
        kind = CapacityKind.TENANT
        env.repo.reserve_capacity(make_capacity_command(kind, limit=1))
        other = make_capacity_command(kind, 1, idempotency_key="cap-02", resource_id="t-2")

        with pytest.raises(QuotaExceeded, match="max_tenants_exceeded limit=1"):
            env.repo.reserve_capacity(other)


def test_limite_zero_rejeita_antes_de_qualquer_escrita() -> None:
    with quota_env() as env:
        before = len(table_items(env.client))

        with pytest.raises(QuotaExceeded, match="max_agents_exceeded limit=0"):
            env.repo.reserve_capacity(make_capacity_command(limit=0))

        assert len(table_items(env.client)) == before


def test_limite_ausente_permite_reservas_ilimitadas() -> None:
    with quota_env() as env:
        for index in range(3):
            command = make_capacity_command(
                limit=None, idempotency_key=f"cap-{index}", resource_id=f"agent-{index}"
            )
            env.repo.reserve_capacity(command)

        assert _counter(env) == 3


def test_versao_do_snapshot_divergente_nega_reserva() -> None:
    with quota_env() as env:
        seed_snapshot(env.client, make_snapshot(ACCOUNT, version=2))

        with pytest.raises(EntitlementDenied, match="snapshot_changed"):
            env.repo.reserve_capacity(make_capacity_command())

        assert _counter(env) == 0


def test_snapshot_expirado_nega_reserva() -> None:
    with quota_env() as env:
        env.clock.advance(timedelta(days=31))

        with pytest.raises(EntitlementDenied, match="snapshot_changed"):
            env.repo.reserve_capacity(make_capacity_command())


def test_snapshot_ausente_nega_reserva() -> None:
    with quota_env() as env:
        pk, sk = entitlement_snapshot_key(ACCOUNT)
        env.client.delete_item(TableName=TABLE_NAME, Key={"pk": {"S": pk}, "sk": {"S": sk}})

        with pytest.raises(EntitlementDenied, match="snapshot_changed"):
            env.repo.reserve_capacity(make_capacity_command())


def test_reserva_preexistente_gera_conflito_permanente() -> None:
    with quota_env() as env:
        command = make_capacity_command()
        other = env.repo.reserve_capacity(replace(command, idempotency_key="cap-09"))
        existing = replace(other, reservation_id=RESERVATION_ID)
        env.client.put_item(
            TableName=TABLE_NAME, Item=encode_capacity_reservation(existing, TENANT)
        )

        with pytest.raises(PermanentBillingError) as error:
            env.repo.reserve_capacity(command)

        assert error.value.code == "capacity_reservation_conflict"


def test_outbox_preexistente_gera_conflito_permanente() -> None:
    with quota_env() as env:
        payload = {
            "billing_account_id": ACCOUNT,
            "reservation_id": RESERVATION_ID,
        }
        event = quota_event("quota.reserved", TENANT, payload, NOW)
        env.client.put_item(TableName=TABLE_NAME, Item=outbox_item(event))

        with pytest.raises(PermanentBillingError) as error:
            env.repo.reserve_capacity(make_capacity_command())

        assert error.value.code == "capacity_reservation_conflict"


def test_replay_concorrente_na_falha_retorna_reserva_gravada() -> None:
    with quota_env() as env:
        winner = partial(env.repo.reserve_capacity, make_capacity_command())
        client = _ContendedClient(env.client, 1, winner)
        repo = DynamoQuotaReservations(client, TABLE_NAME, env.clock.now)

        reservation = repo.reserve_capacity(make_capacity_command())

        assert reservation.reservation_id == RESERVATION_ID
        assert _counter(env) == 1


def test_consumo_mantem_contador_e_marca_consumida() -> None:
    with quota_env() as env:
        env.repo.reserve_capacity(make_capacity_command())

        consumed = env.repo.consume_capacity(_consume())

        assert consumed.status is ReservationStatus.CONSUMED
        assert _counter(env) == 1
        assert _stored(env)["status"]["S"] == "consumed"
        assert "gsi1pk" not in _stored(env)
        assert _event_types(env) == ["quota.consumed", "quota.reserved"]


def test_consumo_repetido_e_idempotente() -> None:
    with quota_env() as env:
        env.repo.reserve_capacity(make_capacity_command())
        first = env.repo.consume_capacity(_consume())

        assert env.repo.consume_capacity(_consume()) == first
        assert _event_types(env) == ["quota.consumed", "quota.reserved"]


def test_consumo_apos_liberacao_e_rejeitado() -> None:
    with quota_env() as env:
        env.repo.reserve_capacity(make_capacity_command())
        env.repo.release_capacity(_release())

        with pytest.raises(PermanentBillingError) as error:
            env.repo.consume_capacity(_consume())

        assert error.value.code == "capacity_reservation_released"


def test_consumo_e_liberacao_de_reserva_inexistente_falham() -> None:
    with quota_env() as env:
        for action in (
            lambda: env.repo.consume_capacity(_consume("missing")),
            lambda: env.repo.release_capacity(_release("missing")),
        ):
            with pytest.raises(PermanentBillingError) as error:
                action()
            assert error.value.code == "capacity_reservation_missing"


def test_liberacao_decrementa_contador_uma_vez() -> None:
    with quota_env() as env:
        env.repo.reserve_capacity(make_capacity_command())

        released = env.repo.release_capacity(_release())

        assert released.status is ReservationStatus.RELEASED
        assert _counter(env) == 0
        assert "gsi1pk" not in _stored(env)
        assert _events(env)["quota.released"]["reason_code"] == "agent_removed"


def test_liberacao_repetida_e_no_op_estavel() -> None:
    with quota_env() as env:
        env.repo.reserve_capacity(make_capacity_command(limit=5))
        env.repo.reserve_capacity(
            make_capacity_command(idempotency_key="cap-02", resource_id="agent-02")
        )
        first = env.repo.release_capacity(_release())

        assert env.repo.release_capacity(_release()) == first
        assert _counter(env) == 1


def test_liberacao_apos_consumo_decrementa_uma_vez() -> None:
    with quota_env() as env:
        env.repo.reserve_capacity(make_capacity_command())
        env.repo.consume_capacity(_consume())

        released = env.repo.release_capacity(_release())

        assert released.status is ReservationStatus.RELEASED
        assert _counter(env) == 0


def test_contencao_transitoria_e_repetida_ate_sucesso() -> None:
    with quota_env() as env:
        env.repo.reserve_capacity(make_capacity_command())
        repo = DynamoQuotaReservations(_ContendedClient(env.client, 2), TABLE_NAME, env.clock.now)

        assert repo.consume_capacity(_consume()).status is ReservationStatus.CONSUMED


def test_contencao_persistente_levanta_erro_retentavel() -> None:
    with quota_env() as env:
        env.repo.reserve_capacity(make_capacity_command())
        repo = DynamoQuotaReservations(_ContendedClient(env.client, 3), TABLE_NAME, env.clock.now)

        with pytest.raises(RetryableBillingError) as error:
            repo.release_capacity(_release())

        assert error.value.code == "capacity_reservation_contended"
        assert _counter(env) == 1


def test_transicao_com_guarda_ausente_falhando_retorna_falso() -> None:
    with quota_env() as env:
        env.repo.reserve_capacity(make_capacity_command())
        guard = absent_check_action(TABLE_NAME, entitlement_snapshot_key(ACCOUNT))
        change = CapacityTransition(ReservationStatus.RELEASED, NOW, "expired", (guard,))

        assert env.repo._transition_capacity(_stored(env), change) is False
        assert _counter(env) == 1
        assert _stored(env)["status"]["S"] == "reserved"


def test_transicao_liberada_sem_motivo_omite_reason_code() -> None:
    with quota_env() as env:
        env.repo.reserve_capacity(make_capacity_command())
        change = CapacityTransition(ReservationStatus.RELEASED, NOW)

        assert env.repo._transition_capacity(_stored(env), change) is True
        assert "reason_code" not in _events(env)["quota.released"]




def test_carencia_expirada_nega_capacidade_sem_escrever() -> None:
    snapshot = replace(
        make_quota_snapshot(),
        subscription_status=SubscriptionStatus.PAST_DUE,
        grace_until=NOW + timedelta(hours=1),
    )
    with quota_env(snapshot) as env:
        env.clock.advance(timedelta(hours=2))
        before = table_items(env.client)
        with pytest.raises(EntitlementDenied, match="reason=grace_expired"):
            env.repo.reserve_capacity(make_capacity_command())
        assert table_items(env.client) == before


def test_capacidade_com_snapshot_de_outra_versao_nega_sem_escrever() -> None:
    with quota_env() as env:
        before = table_items(env.client)
        with pytest.raises(EntitlementDenied, match="reason=snapshot_changed"):
            env.repo.reserve_capacity(make_capacity_command(entitlement_version=2))
        assert table_items(env.client) == before
