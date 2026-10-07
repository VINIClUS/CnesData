"""Testes das chaves de índice das reservas de quota."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from cnes_infra.billing import keys
from cnes_infra.control_plane.dynamodb_keys import key_component, timestamp

EXPIRES = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)


def test_sort_key_de_vencimento_ordena_por_instante_utc() -> None:
    identity = f"{key_component('ba_01')}#{key_component('res_01')}"
    expected = f"{timestamp(EXPIRES)}#{identity}"
    assert keys.quota_reservation_due_sort_key(EXPIRES, "ba_01", "res_01") == expected
    later = keys.quota_reservation_due_sort_key(EXPIRES + timedelta(seconds=1), "ba_00", "r")
    assert later > expected


def test_sort_key_de_vencimento_rejeita_instante_nao_utc() -> None:
    local = EXPIRES.astimezone(timezone(timedelta(hours=-3)))
    with pytest.raises(ValueError, match="reason=non_utc_datetime"):
        keys.quota_reservation_due_sort_key(local, "ba_01", "res_01")


def test_localizador_da_reserva_usa_conta_e_id_em_hex() -> None:
    expected = f"QUOTA_RESERVATION#{key_component('ba_01')}#{key_component('res_01')}"
    assert keys.quota_reservation_locator("ba_01", "res_01") == expected
    assert keys.QUOTA_RESERVATION_LOCATOR_INDEX != keys.QUOTA_RESERVATION_DUE_INDEX
