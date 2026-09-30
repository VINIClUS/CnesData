"""Testes do catálogo de chaves DynamoDB de billing."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from cnes_infra.billing import keys
from cnes_infra.control_plane.dynamodb_keys import (
    entity_key,
    key_component,
    tenant_partition,
    timestamp,
)

PERIOD = datetime(2026, 9, 1, tzinfo=UTC)
PERIOD_TS = timestamp(PERIOD)


def _hex(value: str) -> str:
    return value.encode().hex()


def test_particao_da_conta_usa_hex_do_id() -> None:
    assert keys.billing_partition("ba_01") == "BILLING#" + bytes("ba_01", "utf-8").hex()


def test_chaves_base_da_conta_compartilham_particao() -> None:
    partition = keys.billing_partition("ba_01")
    assert keys.entitlement_snapshot_key("ba_01") == (partition, "ENTITLEMENT")
    assert keys.billing_account_key("ba_01") == (partition, "ACCOUNT")
    assert keys.capacity_usage_key("ba_01") == (partition, "CAPACITY")


def test_lista_global_de_contas_usa_particao_fixa() -> None:
    expected = ("BILLING_ACCOUNTS", "ACCOUNT#" + _hex("ba_01"))
    assert keys.billing_account_list_key("ba_01") == expected


def test_link_conta_tenant_e_reverso() -> None:
    partition = keys.billing_partition("ba_01")
    assert keys.account_tenant_key("ba_01", "t1") == (partition, "TENANT#" + _hex("t1"))
    assert keys.tenant_account_key("t1") == (tenant_partition("t1"), "BILLING_ACCOUNT")


def test_tenant_canonico_reusa_entity_key_do_control_plane() -> None:
    assert keys.tenant_entity_key("t1") == entity_key("t1", "TENANT", "t1")


def test_mapas_stripe_e_plano_usam_hex() -> None:
    assert keys.stripe_customer_key("cus_1") == (
        "STRIPE_CUSTOMER#" + _hex("cus_1"),
        "BILLING_ACCOUNT",
    )
    assert keys.plan_version_key("pv1") == ("PLAN_VERSION#" + _hex("pv1"), "META")
    assert keys.stripe_price_key("price_1") == ("STRIPE_PRICE#" + _hex("price_1"), "PLAN_VERSION")


def test_particao_do_periodo_usa_timestamp_de_largura_fixa() -> None:
    expected = f"{keys.billing_partition('ba_01')}#PERIOD#{PERIOD_TS}"
    assert keys.billing_period_partition("ba_01", PERIOD) == expected


def test_uso_e_reserva_ficam_na_particao_do_periodo() -> None:
    partition = keys.billing_period_partition("ba_01", PERIOD)
    assert keys.usage_key("ba_01", PERIOD) == (partition, "USAGE")
    expected = (partition, "RESERVATION#" + _hex("res1"))
    assert keys.reservation_key("ba_01", PERIOD, "res1") == expected


def test_reserva_de_capacidade_fica_na_particao_da_conta() -> None:
    expected = (keys.billing_partition("ba_01"), "CAPACITY_RESERVATION#" + _hex("res1"))
    assert keys.capacity_reservation_key("ba_01", "res1") == expected


def test_run_billing_reusa_entity_key_do_control_plane() -> None:
    assert keys.run_billing_key("t1", "run1") == entity_key("t1", "RUN_BILLING", "run1")


def test_lookup_de_runs_da_conta() -> None:
    partition = keys.billing_partition("ba_01") + "#RUNS"
    assert keys.run_lookup_partition("ba_01") == partition
    expected = (partition, f"RUN#{_hex('t1')}#{_hex('run1')}")
    assert keys.run_lookup_key("ba_01", "t1", "run1") == expected


def test_idempotencia_de_conta_inclui_escopo_e_chave() -> None:
    partition = f"{keys.billing_partition('ba_01')}#IDEMPOTENCY#{_hex('create')}"
    assert keys.billing_idempotency_key("ba_01", "create", "k1") == (
        partition,
        "KEY#" + _hex("k1"),
    )


def test_item_de_inbox_stripe_usa_hex_do_evento() -> None:
    assert keys.stripe_event_key("evt_1") == ("STRIPE_EVENT#" + _hex("evt_1"), "EVENT")


def test_sort_key_de_vencimento_combina_timestamp_e_evento() -> None:
    due = datetime(2026, 9, 30, 12, tzinfo=UTC)
    expected = f"{timestamp(due)}#{key_component('evt_1')}"
    assert keys.stripe_recovery_due_sort_key(due, "evt_1") == expected


def test_constantes_do_indice_de_recovery() -> None:
    assert keys.STRIPE_RECOVERY_DUE_INDEX == "gsi1"
    assert keys.STRIPE_RECOVERY_DUE_PARTITION == "STRIPE_RECOVERY#DUE"


def test_cursor_de_recovery_usa_particao_de_sistema() -> None:
    assert keys.stripe_recovery_cursor_key() == ("BILLING#SYSTEM", "RECOVERY#STRIPE")


def test_progresso_de_revogacao_preenche_20_digitos() -> None:
    partition, sort_key = keys.revocation_progress_key("ba_01", 7)
    assert partition == keys.billing_partition("ba_01")
    assert sort_key == "REVOCATION#" + "7".zfill(20)


def test_progresso_de_revogacao_ordena_como_inteiro() -> None:
    values = [10, 2, 1, 100, 9]
    sort_keys = [keys.revocation_progress_key("ba_01", value)[1] for value in values]
    assert [values[i] for i in sorted(range(5), key=sort_keys.__getitem__)] == sorted(values)


@pytest.mark.parametrize("version", [0, -1, True])
def test_revogacao_rejeita_versao_invalida(version: int) -> None:
    with pytest.raises(ValueError, match="invalid_entitlement_version"):
        keys.revocation_progress_key("ba_01", version)


@pytest.mark.parametrize(
    "naive_or_offset",
    [PERIOD.replace(tzinfo=None), datetime(2026, 9, 1, tzinfo=timezone(timedelta(hours=-3)))],
)
def test_rejeita_datetime_nao_utc(naive_or_offset: datetime) -> None:
    with pytest.raises(ValueError, match="non_utc_datetime"):
        keys.billing_period_partition("ba_01", naive_or_offset)
    with pytest.raises(ValueError, match="non_utc_datetime"):
        keys.stripe_recovery_due_sort_key(naive_or_offset, "evt_1")


def test_aceita_datetime_utc_equivalente_com_largura_fixa() -> None:
    equivalent = datetime(2026, 9, 1, tzinfo=timezone(timedelta(0)))
    assert keys.billing_period_partition("ba_01", equivalent).endswith(PERIOD_TS)
    assert keys.stripe_recovery_due_sort_key(equivalent, "e").startswith(PERIOD_TS)


def test_ids_com_cerquilha_nao_colidem() -> None:
    assert keys.account_tenant_key("a#b", "c") != keys.account_tenant_key("a", "b#c")
    assert keys.run_lookup_key("a", "b#c", "d") != keys.run_lookup_key("a", "b", "c#d")
    assert keys.billing_partition("a#b") != keys.billing_partition("a") + "#b"


@pytest.mark.parametrize("account_id", ["SYSTEM", "system", "", "ba_01"])
def test_particao_de_conta_nunca_e_a_particao_de_sistema(account_id: str) -> None:
    assert keys.billing_partition(account_id) != keys.stripe_recovery_cursor_key()[0]
