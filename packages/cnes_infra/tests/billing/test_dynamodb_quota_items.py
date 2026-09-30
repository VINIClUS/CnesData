"""Testes do codec de itens DynamoDB de quota."""

from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest

from cnes_domain.billing.errors import (
    EntitlementDenied,
    IdempotencyConflict,
    PermanentBillingError,
)
from cnes_domain.billing.execution import RunBillingState
from cnes_domain.billing.models import (
    AnalyticsAuthorization,
    CapacityKind,
    CapacityReservation,
    QuotaReservation,
    ReservationKind,
    ReservationStatus,
    RunAuthorization,
    SubscriptionStatus,
)
from cnes_domain.control_plane.entities import IdempotencyRecord
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState
from cnes_infra.billing import dynamodb_quota_items as codec
from cnes_infra.billing.dynamodb_quota_items import (
    SnapshotExpectation,
    UsageGuard,
)
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT, HASH_A, HASH_B, TENANT

CORRUPT = "billing_item_corrupt"
KEY = ("USAGE#x", "PERIOD#y")


def reservation(**changes: Any) -> QuotaReservation:
    base = QuotaReservation(
        reservation_id="res-01",
        billing_account_id=ACCOUNT,
        resource_id="run-01",
        kind=ReservationKind.RUN,
        period_start=NOW,
        reserved_runs=1,
        reserved_scan_bytes=0,
        consumed_runs=0,
        consumed_scan_bytes=0,
        status=ReservationStatus.RESERVED,
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=15),
    )
    return replace(base, **changes)


def capacity(**changes: Any) -> CapacityReservation:
    base = CapacityReservation(
        reservation_id="cap-01",
        billing_account_id=ACCOUNT,
        resource_id="agent-01",
        kind=CapacityKind.AGENT,
        status=ReservationStatus.RESERVED,
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=15),
    )
    return replace(base, **changes)


def authorization(**changes: Any) -> RunAuthorization:
    base = RunAuthorization(
        billing_account_id=ACCOUNT,
        plan_version_id="plan_v1",
        entitlement_version=3,
        max_concurrency=2,
        budget_reservation_id="res-01",
        authorized_at=NOW,
    )
    return replace(base, **changes)


def unbound_state(**changes: Any) -> RunBillingState:
    base = RunBillingState(
        billing_account_id=ACCOUNT,
        tenant_id=TENANT,
        run_id="run-01",
        authorization=authorization(),
        execution_generation=0,
        execution_wave_id=None,
        execution_dispatch_id=None,
        execution_ref=None,
        execution_unit_ids=(),
        execution_status=None,
        execution_terminal_outcome=None,
        fencing_token=0,
        cancel_requested=False,
        updated_at=NOW,
    )
    return replace(base, **changes)


def bound_state(**changes: Any) -> RunBillingState:
    return unbound_state(
        execution_generation=1,
        execution_wave_id="a" * 16,
        execution_dispatch_id="b" * 16,
        execution_ref="arn-01",
        execution_unit_ids=("u1", "u2"),
        execution_status=DispatchState.TERMINAL,
        execution_terminal_outcome=DispatchOutcome.SUCCEEDED,
        fencing_token=2,
        **changes,
    )


def idempotency_record() -> IdempotencyRecord:
    return IdempotencyRecord(
        tenant_id=TENANT,
        scope=codec.RUN_SCOPE,
        key="k-1",
        request_hash=HASH_A,
        status="COMPLETED",
        resource_id="run-01",
        created_at=NOW,
        expires_at=NOW + codec.IDEMPOTENCY_TTL,
    )


class FakeClient:
    def __init__(self, item: dict[str, Any] | None) -> None:
        self.item = item
        self.requests: list[dict[str, Any]] = []

    def get_item(self, **request: Any) -> dict[str, Any]:
        self.requests.append(request)
        return {} if self.item is None else {"Item": self.item}


def assert_corrupt(error: pytest.ExceptionInfo[PermanentBillingError]) -> None:
    assert error.value.code == CORRUPT


def test_reserva_reservada_ida_e_volta_com_indices_de_vencimento_e_localizador() -> None:
    item = codec.encode_reservation(reservation(), TENANT)

    assert codec.decode_reservation(item) == (reservation(), TENANT)
    assert item["tenant_id"] == {"S": TENANT}
    assert item["status"] == {"S": "reserved"}
    assert item["gsi1pk"]["S"]
    assert item["gsi1sk"]["S"]
    assert item["gsi2pk"]["S"]
    assert item["gsi2sk"]["S"]
    assert "expires_at" not in item


@pytest.mark.parametrize("status", [ReservationStatus.CONSUMED, ReservationStatus.RELEASED])
def test_reserva_finalizada_perde_indice_de_vencimento_e_mantem_localizador(
    status: ReservationStatus,
) -> None:
    item = codec.encode_reservation(reservation(status=status), TENANT)

    assert "gsi1pk" not in item
    assert "gsi1sk" not in item
    assert item["gsi2pk"]["S"]
    assert item["status"] == {"S": status.value}
    assert codec.decode_reservation(item)[0].status is status
    assert "expires_at" not in item


def test_reserva_de_capacidade_reservada_tem_vencimento_e_nao_tem_localizador() -> None:
    item = codec.encode_capacity_reservation(capacity(), TENANT)

    assert codec.decode_capacity_reservation(item) == (capacity(), TENANT)
    assert item["gsi1pk"]["S"]
    assert item["gsi1sk"]["S"]
    assert "gsi2pk" not in item
    assert "expires_at" not in item


def test_reserva_de_capacidade_liberada_nao_tem_indice_de_vencimento() -> None:
    released = capacity(status=ReservationStatus.RELEASED, kind=CapacityKind.TENANT)
    item = codec.encode_capacity_reservation(released, TENANT)

    assert "gsi1pk" not in item
    assert codec.decode_capacity_reservation(item)[0] == released


def test_estado_de_billing_desvinculado_ida_e_volta() -> None:
    item = codec.encode_run_billing_state(unbound_state())

    assert codec.decode_run_billing_state(item) == unbound_state()


def test_estado_de_billing_vinculado_terminal_ida_e_volta() -> None:
    item = codec.encode_run_billing_state(bound_state())
    decoded = codec.decode_run_billing_state(item)

    assert decoded == bound_state()
    assert decoded.execution_status is DispatchState.TERMINAL
    assert decoded.execution_terminal_outcome is DispatchOutcome.SUCCEEDED


def test_lookup_do_run_carrega_identidade_e_reserva() -> None:
    item = codec.encode_run_lookup(unbound_state(), reservation())

    assert item["entity"] == {"S": codec.RUN_LOOKUP_ENTITY}
    assert item["payload"]["S"] == (
        '{"billing_account_id":"ba_01","period_start":"2026-09-30T12:00:00+00:00",'
        '"reservation_id":"res-01","run_id":"run-01","tenant_id":"354130"}'
    )


def test_resultado_gravado_e_decodificado_para_cada_tipo() -> None:
    base = codec.encode_run_lookup(unbound_state(), reservation())
    analytics = AnalyticsAuthorization(
        billing_account_id=ACCOUNT,
        entitlement_version=3,
        budget_reservation_id=None,
        max_scan_bytes=10,
        authorized_at=NOW,
    )

    assert codec.decode_run_result(codec.with_result(base, authorization())) == authorization()
    assert codec.decode_analytics_result(codec.with_result(base, analytics)) == analytics
    assert codec.decode_capacity_result(codec.with_result(base, capacity())) == capacity()
    assert "result" not in base


def test_resultado_ausente_ou_malformado_e_corrompido() -> None:
    base = codec.encode_run_lookup(unbound_state(), reservation())

    with pytest.raises(PermanentBillingError) as ausente:
        codec.decode_run_result(base)
    with pytest.raises(PermanentBillingError) as malformado:
        codec.decode_capacity_result({**base, "result": {"S": "{nao-json"}})

    assert_corrupt(ausente)
    assert_corrupt(malformado)


def test_reserva_com_entidade_errada_e_corrompida() -> None:
    item = {**codec.encode_reservation(reservation(), TENANT), "entity": {"S": "OUTRA"}}

    with pytest.raises(PermanentBillingError) as error:
        codec.decode_reservation(item)

    assert_corrupt(error)


def test_reserva_com_payload_malformado_e_corrompida() -> None:
    item = codec.encode_reservation(reservation(), TENANT)

    for payload in ("{quebrado", '{"kind":"run"}'):
        with pytest.raises(PermanentBillingError) as error:
            codec.decode_reservation({**item, "payload": {"S": payload}})
        assert_corrupt(error)


@pytest.mark.parametrize("attribute", ["pk", "sk"])
def test_reserva_com_chave_divergente_do_payload_e_corrompida(attribute: str) -> None:
    item = codec.encode_reservation(reservation(), TENANT)

    with pytest.raises(PermanentBillingError) as error:
        codec.decode_reservation({**item, attribute: {"S": "OUTRA#CHAVE"}})

    assert_corrupt(error)


@pytest.mark.parametrize("tenant", [None, {}, {"S": ""}, {"N": "1"}])
def test_reserva_sem_tenant_valido_e_corrompida(tenant: dict[str, str] | None) -> None:
    item = codec.encode_reservation(reservation(), TENANT)
    item.pop("tenant_id")
    if tenant is not None:
        item["tenant_id"] = tenant

    with pytest.raises(PermanentBillingError) as error:
        codec.decode_reservation(item)

    assert_corrupt(error)


def test_reserva_de_capacidade_sem_tenant_e_corrompida() -> None:
    item = codec.encode_capacity_reservation(capacity(), TENANT)
    item.pop("tenant_id")

    with pytest.raises(PermanentBillingError) as error:
        codec.decode_capacity_reservation(item)

    assert_corrupt(error)


def test_estado_de_billing_com_chave_divergente_e_corrompido() -> None:
    item = {**codec.encode_run_billing_state(unbound_state()), "pk": {"S": "OUTRA"}}

    with pytest.raises(PermanentBillingError) as error:
        codec.decode_run_billing_state(item)

    assert_corrupt(error)


def test_verificacao_de_snapshot_sem_status_exige_versao_e_validade() -> None:
    expected = SnapshotExpectation(ACCOUNT, 4, None)

    check = codec.snapshot_check(TABLE_NAME, expected, NOW)["ConditionCheck"]

    assert check["TableName"] == TABLE_NAME
    assert check["ConditionExpression"] == "entitlement_version = :version AND valid_until > :now"
    assert set(check["ExpressionAttributeValues"]) == {":version", ":now"}
    assert check["ExpressionAttributeValues"][":version"] == {"N": "4"}
    assert check["Key"]["pk"]["S"]
    assert check["Key"]["sk"]["S"]


def test_verificacao_de_snapshot_com_status_adiciona_condicao_de_status() -> None:
    expected = SnapshotExpectation(ACCOUNT, 4, SubscriptionStatus.ACTIVE)

    check = codec.snapshot_check(TABLE_NAME, expected, NOW)["ConditionCheck"]

    assert check["ConditionExpression"] == (
        "entitlement_version = :version AND valid_until > :now AND subscription_status = :status"
    )
    assert check["ExpressionAttributeValues"][":status"] == {"S": "active"}


def test_atualizacao_de_uso_sem_guarda_ordena_contadores_e_marca_entidade() -> None:
    action = codec.usage_update(TABLE_NAME, KEY, {"b_count": 2, "a_count": -1}, None)
    update = action["Update"]

    assert update["UpdateExpression"] == "SET #entity = :entity ADD #a0 :a0, #a1 :a1"
    assert update["ExpressionAttributeNames"] == {
        "#entity": "entity",
        "#a0": "a_count",
        "#a1": "b_count",
    }
    assert update["ExpressionAttributeValues"] == {
        ":entity": {"S": codec.USAGE_ENTITY},
        ":a0": {"N": "-1"},
        ":a1": {"N": "2"},
    }
    assert update["Key"] == {"pk": {"S": KEY[0]}, "sk": {"S": KEY[1]}}
    assert "ConditionExpression" not in update


def test_atualizacao_de_uso_com_guarda_exige_contador_ausente_ou_abaixo_do_teto() -> None:
    action = codec.usage_update(TABLE_NAME, KEY, {"a": 1}, UsageGuard("a_committed", 7))
    update = action["Update"]

    assert update["ConditionExpression"] == "attribute_not_exists(#guard) OR #guard <= :ceiling"
    assert update["ExpressionAttributeNames"]["#guard"] == "a_committed"
    assert update["ExpressionAttributeValues"][":ceiling"] == {"N": "7"}


def test_atualizacao_de_liquidacao_exige_item_de_uso_existente() -> None:
    update = codec.settle_usage_update(TABLE_NAME, KEY, {"a": -1})["Update"]

    assert update["ConditionExpression"] == "attribute_exists(pk)"
    assert update["UpdateExpression"] == "SET #entity = :entity ADD #a0 :a0"


@pytest.mark.parametrize(
    ("item", "esperado"),
    [(None, 0), ({}, 0), ({"x": {"N": "42"}}, 42), ({"x": {"N": "0"}}, 0)],
)
def test_contador_de_uso_ausente_vale_zero_e_presente_e_lido(
    item: dict[str, Any] | None, esperado: int
) -> None:
    assert codec.usage_counter(item, "x") == esperado


@pytest.mark.parametrize("value", [{"N": "abc"}, {"S": "1"}, {"N": None}])
def test_contador_de_uso_nao_numerico_e_corrompido(value: dict[str, Any]) -> None:
    with pytest.raises(PermanentBillingError) as error:
        codec.usage_counter({"x": value}, "x")

    assert_corrupt(error)


def test_atributos_de_scan_por_tipo_de_reserva() -> None:
    run = codec.scan_attributes(ReservationKind.RUN)
    analytics = codec.scan_attributes(ReservationKind.ANALYTICS)

    assert (run.reserved, run.consumed, run.committed) == (
        "run_reserved_scan_bytes",
        "run_consumed_scan_bytes",
        "run_committed_scan_bytes",
    )
    assert analytics.committed == "analytics_committed_scan_bytes"


def test_evento_de_quota_tem_id_deterministico_e_campos_do_payload() -> None:
    payload = {"billing_account_id": ACCOUNT, "reservation_id": "res-01", "runs": 1}

    first = codec.quota_event("quota.reserved", TENANT, payload, NOW)
    second = codec.quota_event("quota.reserved", TENANT, dict(payload), NOW)
    other = codec.quota_event("quota.released", TENANT, payload, NOW)

    assert first.event_id == second.event_id != other.event_id
    assert len(first.event_id) == 32
    assert (first.tenant_id, first.aggregate_id) == (TENANT, "res-01")
    assert first.event_type == "quota.reserved"
    assert first.payload == payload
    assert first.created_at == NOW
    assert first.delivered_at is None


def test_put_de_idempotencia_e_condicional_e_carrega_resultado_e_ttl() -> None:
    put = codec.idempotency_put(TABLE_NAME, idempotency_record(), authorization())["Put"]

    assert "attribute_not_exists" in put["ConditionExpression"]
    assert put["Item"]["result"]["S"].startswith("{")
    assert codec.decode_run_result(put["Item"]) == authorization()
    assert put["Item"]["expires_at"] == {"N": str(int((NOW + codec.IDEMPOTENCY_TTL).timestamp()))}


def test_leitura_de_replay_ausente_retorna_none_com_leitura_forte() -> None:
    client = FakeClient(None)

    assert codec.read_replay(client, TABLE_NAME, (TENANT, codec.RUN_SCOPE, "k-1"), HASH_A) is None
    assert client.requests[0]["ConsistentRead"] is True


def test_leitura_de_replay_com_mesmo_hash_retorna_item_gravado() -> None:
    item = codec.idempotency_put(TABLE_NAME, idempotency_record(), authorization())["Put"]["Item"]

    replay = codec.read_replay(
        FakeClient(item), TABLE_NAME, (TENANT, codec.RUN_SCOPE, "k-1"), HASH_A
    )

    assert replay == item


def test_leitura_de_replay_com_hash_diferente_levanta_conflito_de_idempotencia() -> None:
    item = codec.idempotency_put(TABLE_NAME, idempotency_record(), authorization())["Put"]["Item"]

    with pytest.raises(IdempotencyConflict, match="key=k-1"):
        codec.read_replay(FakeClient(item), TABLE_NAME, (TENANT, codec.RUN_SCOPE, "k-1"), HASH_B)


def _access_snapshot(**changes: Any) -> Any:
    from packages.cnes_infra.tests.billing.quota_support import make_quota_snapshot

    return replace(make_quota_snapshot(), **changes)


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"subscription_status": SubscriptionStatus.PAST_DUE, "grace_until": None}, "grace"),
        (
            {"subscription_status": SubscriptionStatus.PAST_DUE, "grace_until": NOW},
            "grace",
        ),
        ({"cancel_at_period_end": True, "period_end": NOW}, "period_ended"),
    ],
)
def test_acesso_no_commit_nega_prazos_temporais_vencidos(
    changes: dict[str, Any], reason: str
) -> None:
    later = NOW + timedelta(seconds=1)
    with pytest.raises(EntitlementDenied, match=reason):
        codec.require_commit_access(_access_snapshot(**changes), later)


def test_acesso_no_commit_permite_carencia_e_periodo_vigentes() -> None:
    snapshot = _access_snapshot(
        subscription_status=SubscriptionStatus.PAST_DUE,
        grace_until=NOW + timedelta(hours=1),
        cancel_at_period_end=True,
    )
    codec.require_commit_access(snapshot, NOW)
