"""Testes do cache local de entitlements."""

from datetime import timedelta

import pytest

from cnes_infra.billing.cache import (
    MAX_CACHE_TTL_SECONDS,
    CacheKey,
    EntitlementChange,
    LocalEntitlementCache,
    handle_entitlement_changed,
)
from packages.cnes_infra.tests.billing.billing_factories import NOW, make_snapshot
from packages.cnes_infra.tests.contracts.clock import MutableClock


def _cache(ttl: int = 30) -> tuple[LocalEntitlementCache, MutableClock]:
    clock = MutableClock(NOW)
    return LocalEntitlementCache(ttl, clock.now), clock


def _latest_version(cache: LocalEntitlementCache) -> int | None:
    latest = cache.get_latest("ba_01")
    return None if latest is None else latest.entitlement_version


def _change(account: str = "ba_01", version: int = 3) -> EntitlementChange:
    return EntitlementChange(account, version)


def test_cache_rejeita_ttl_acima_de_sessenta():
    with pytest.raises(ValueError, match="cache_ttl_gt_60"):
        LocalEntitlementCache(MAX_CACHE_TTL_SECONDS + 1, MutableClock(NOW).now)


def test_cache_aceita_ttl_de_sessenta():
    LocalEntitlementCache(60, MutableClock(NOW).now)


@pytest.mark.parametrize("ttl", [0, -1, True, "10"])
def test_cache_rejeita_ttl_invalido(ttl):
    with pytest.raises(ValueError, match="cache_ttl_invalid"):
        LocalEntitlementCache(ttl, MutableClock(NOW).now)


def test_cache_devolve_snapshot_por_conta_e_versao():
    cache, _ = _cache()
    snapshot = make_snapshot("ba_01", 2)
    cache.put(snapshot)
    assert cache.get(CacheKey("ba_01", 2)) == snapshot


def test_cache_erra_para_outra_versao_ou_conta():
    cache, _ = _cache()
    cache.put(make_snapshot("ba_01", 2))
    assert cache.get(CacheKey("ba_01", 3)) is None
    assert cache.get(CacheKey("ba_02", 2)) is None


def test_cache_expira_exatamente_no_ttl():
    cache, clock = _cache(30)
    cache.put(make_snapshot("ba_01", 1))
    clock.advance(timedelta(seconds=29))
    assert cache.get(CacheKey("ba_01", 1)) is not None
    clock.advance(timedelta(seconds=1))
    assert cache.get(CacheKey("ba_01", 1)) is None
    assert cache.get_latest("ba_01") is None


def test_cache_get_latest_devolve_versao_mais_nova():
    cache, _ = _cache()
    for version in (2, 3, 1):
        cache.put(make_snapshot("ba_01", version))
    assert _latest_version(cache) == 3


def test_cache_get_latest_sem_entradas_devolve_none():
    cache, _ = _cache()
    assert cache.get_latest("ba_01") is None


def test_invalidacao_por_versao_remove_versoes_anteriores():
    cache, _ = _cache()
    for version in (1, 2, 3):
        cache.put(make_snapshot("ba_01", version))
    handle_entitlement_changed(_change(version=3), cache)
    assert cache.get(CacheKey("ba_01", 1)) is None
    assert cache.get(CacheKey("ba_01", 2)) is None
    assert cache.get(CacheKey("ba_01", 3)) is not None
    assert _latest_version(cache) == 3


def test_invalidacao_sem_versao_restante_zera_latest():
    cache, _ = _cache()
    for version in (1, 2):
        cache.put(make_snapshot("ba_01", version))
    handle_entitlement_changed(_change(version=5), cache)
    assert cache.get_latest("ba_01") is None


def test_invalidacao_mantem_latest_acima_do_piso_e_remove_antigas():
    cache, _ = _cache()
    cache.put(make_snapshot("ba_01", 1))
    cache.put(make_snapshot("ba_01", 4))
    cache.invalidate_before("ba_01", 3)
    assert _latest_version(cache) == 4


def test_invalidacao_preserva_latest_acima_do_piso():
    cache, _ = _cache()
    cache.put(make_snapshot("ba_01", 1))
    cache.put(make_snapshot("ba_01", 4))
    cache.invalidate_before("ba_01", 2)
    assert _latest_version(cache) == 4


def test_piso_impede_repopular_versao_antiga():
    cache, _ = _cache()
    cache.invalidate_before("ba_01", 3)
    cache.put(make_snapshot("ba_01", 2))
    assert cache.get(CacheKey("ba_01", 2)) is None
    assert cache.get_latest("ba_01") is None
    cache.put(make_snapshot("ba_01", 3))
    assert cache.get(CacheKey("ba_01", 3)) is not None


def test_piso_nunca_diminui():
    cache, _ = _cache()
    cache.invalidate_before("ba_01", 5)
    cache.invalidate_before("ba_01", 2)
    cache.put(make_snapshot("ba_01", 4))
    assert cache.get(CacheKey("ba_01", 4)) is None


def test_invalidacao_nao_afeta_outras_contas():
    cache, _ = _cache()
    cache.put(make_snapshot("ba_01", 1))
    cache.put(make_snapshot("ba_02", 1))
    cache.invalidate_before("ba_01", 3)
    assert cache.get(CacheKey("ba_02", 1)) is not None
    assert cache.get_latest("ba_02") is not None


@pytest.mark.parametrize("account", ["", "  "])
def test_mudanca_rejeita_conta_vazia(account):
    with pytest.raises(ValueError, match="blank_value"):
        EntitlementChange(account, 1)


@pytest.mark.parametrize("version", [0, -1])
def test_mudanca_rejeita_versao_nao_positiva(version):
    with pytest.raises(ValueError, match="positive_value_required"):
        EntitlementChange("ba_01", version)


def test_put_remove_entradas_expiradas_de_qualquer_conta() -> None:
    clock = MutableClock(NOW)
    cache = LocalEntitlementCache(60, clock.now)
    for version in range(1, 101):
        cache.put(make_snapshot("ba_old", version))
    clock.advance(timedelta(seconds=60))

    cache.put(make_snapshot("ba_new", 1))

    assert len(cache) == 1
    assert cache.get_latest("ba_old") is None


def test_expiracao_libera_metadados_de_contas_inativas() -> None:
    clock = MutableClock(NOW)
    cache = LocalEntitlementCache(60, clock.now)
    for index in range(50):
        cache.put(make_snapshot(f"ba_{index}", 2))
        handle_entitlement_changed(EntitlementChange(f"ba_{index}", 2), cache)
    clock.advance(timedelta(seconds=60))

    cache.put(make_snapshot("ba_new", 1))

    assert cache.tracked_accounts() == 1


def test_piso_expira_junto_com_o_ttl() -> None:
    clock = MutableClock(NOW)
    cache = LocalEntitlementCache(60, clock.now)
    handle_entitlement_changed(EntitlementChange("ba_01", 3), cache)
    clock.advance(timedelta(seconds=60))

    cache.put(make_snapshot("ba_01", 2))

    assert cache.get_latest("ba_01") == make_snapshot("ba_01", 2)


def test_expiracao_da_ultima_versao_recalcula_indice_pela_versao_viva() -> None:
    clock = MutableClock(NOW)
    cache = LocalEntitlementCache(60, clock.now)
    cache.put(make_snapshot("ba_01", 2))
    clock.advance(timedelta(seconds=30))
    cache.put(make_snapshot("ba_01", 1))
    clock.advance(timedelta(seconds=30))

    assert _latest_version(cache) == 1

    clock.advance(timedelta(seconds=30))
    assert cache.get_latest("ba_01") is None
    assert cache.tracked_accounts() == 0
