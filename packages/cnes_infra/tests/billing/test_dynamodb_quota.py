"""Testes de reserve_and_create_run e reserve_analytics sobre moto."""

from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from functools import partial
from typing import Any

import pytest

from cnes_domain.billing.errors import (
    EntitlementDenied,
    IdempotencyConflict,
    PermanentBillingError,
    QuotaExceeded,
)
from cnes_domain.billing.models import ReservationKind, ReservationStatus, SubscriptionStatus
from cnes_infra.billing.dynamodb_items import decode_idempotency_record, deterministic_id
from cnes_infra.billing.dynamodb_quota_items import (
    ANALYTICS_SCOPE,
    RUN_SCOPE,
    decode_analytics_result,
    decode_reservation,
    decode_run_billing_state,
    decode_run_result,
    scan_attributes,
    usage_counter,
)
from cnes_infra.billing.keys import (
    entitlement_snapshot_key,
    reservation_key,
    run_billing_key,
    run_lookup_key,
    usage_key,
)
from cnes_infra.control_plane.dynamodb_keys import idempotency_key, item_key, outbox_key
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    HASH_B,
    TENANT,
    make_analytics_command,
    make_quota_snapshot,
    make_reserve_command,
    quota_env,
    seed_snapshot,
    table_items,
)

RUN_SCAN = scan_attributes(ReservationKind.RUN)
ANALYTICS_SCAN = scan_attributes(ReservationKind.ANALYTICS)
USAGE = usage_key(ACCOUNT, NOW)


class _RacingClient:
    def __init__(self, inner: Any, before_transact: Callable[[], None]) -> None:
        self._inner = inner
        self._before = before_transact

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def transact_write_items(self, **request: Any) -> Any:
        before, self._before = self._before, lambda: None
        before()
        return self._inner.transact_write_items(**request)


def _get(client: Any, key: tuple[str, str]) -> dict[str, Any] | None:
    response = client.get_item(TableName=TABLE_NAME, Key=item_key(*key), ConsistentRead=True)
    return response.get("Item")


def _usage(client: Any, attribute: str) -> int:
    return usage_counter(_get(client, USAGE), attribute)


def _racing(env: Any, before: Callable[[], None]) -> Any:
    racing = _RacingClient(env.client, before)
    return type(env.repo)(racing, TABLE_NAME, env.clock.now)


def test_reserva_repetida_retorna_mesma_autorizacao() -> None:
    with quota_env() as env:
        first = env.repo.reserve_and_create_run(make_reserve_command())
        second = env.repo.reserve_and_create_run(make_reserve_command())
        assert second == first
        assert _usage(env.client, "consumed_runs") == 1


def test_mesma_chave_payload_diferente_conflita() -> None:
    with quota_env() as env:
        env.repo.reserve_and_create_run(make_reserve_command())
        with pytest.raises(IdempotencyConflict, match="key=req-01"):
            env.repo.reserve_and_create_run(make_reserve_command(request_hash=HASH_B))


def test_reserva_persistida_consome_uma_unidade_e_reserva_scan() -> None:
    with quota_env() as env:
        env.repo.reserve_and_create_run(make_reserve_command())
        item = _get(env.client, reservation_key(ACCOUNT, NOW, "res-run-01"))
        reservation, tenant = decode_reservation(item)
        assert tenant == TENANT
        assert reservation.kind is ReservationKind.RUN
        assert reservation.resource_id == "run-01"
        assert (reservation.reserved_runs, reservation.consumed_runs) == (0, 1)
        assert (reservation.reserved_scan_bytes, reservation.consumed_scan_bytes) == (1_000, 0)
        assert reservation.status is ReservationStatus.RESERVED
        assert reservation.expires_at == NOW + timedelta(minutes=15)
        assert "expires_at" not in item


def test_contadores_de_uso_refletem_a_reserva() -> None:
    with quota_env() as env:
        env.repo.reserve_and_create_run(make_reserve_command())
        assert _usage(env.client, "consumed_runs") == 1
        assert _usage(env.client, RUN_SCAN.reserved) == 1_000
        assert _usage(env.client, RUN_SCAN.committed) == 1_000
        assert _usage(env.client, RUN_SCAN.consumed) == 0


def test_companion_de_billing_nasce_sem_execucao_vinculada() -> None:
    with quota_env() as env:
        authorization = env.repo.reserve_and_create_run(make_reserve_command())
        state = decode_run_billing_state(_get(env.client, run_billing_key(TENANT, "run-01")))
        assert state.authorization == authorization
        assert state.execution_generation == 0
        assert state.fencing_token == 0
        assert state.execution_wave_id is None
        assert state.execution_dispatch_id is None
        assert state.execution_ref is None
        assert state.execution_unit_ids == ()
        assert state.cancel_requested is False
        assert env.control_plane.get_run(TENANT, "run-01") is not None


def test_lookup_do_run_aponta_para_a_reserva() -> None:
    with quota_env() as env:
        env.repo.reserve_and_create_run(make_reserve_command())
        lookup = _get(env.client, run_lookup_key(ACCOUNT, TENANT, "run-01"))
        assert lookup is not None
        assert "res-run-01" in lookup["payload"]["S"]


def test_registro_de_idempotencia_guarda_resultado_do_run() -> None:
    with quota_env() as env:
        authorization = env.repo.reserve_and_create_run(make_reserve_command())
        item = _get(env.client, idempotency_key(TENANT, RUN_SCOPE, "req-01"))
        record = decode_idempotency_record(item, (TENANT, RUN_SCOPE, "req-01"))
        assert record.resource_id == "run-01"
        assert record.status == "COMPLETED"
        assert decode_run_result(item) == authorization


def test_outbox_quota_reserved_fica_pendente() -> None:
    with quota_env() as env:
        env.repo.reserve_and_create_run(make_reserve_command())
        event_id = deterministic_id("quota.reserved", ACCOUNT, "res-run-01")
        item = _get(env.client, outbox_key(event_id))
        assert item["gsi6pk"]["S"] == "OUTBOX#PENDING"
        payload = item["payload"]["S"]
        for fragment in ('"kind":"run"', '"run_id":"run-01"', '"estimated_scan_bytes":1000'):
            assert fragment in payload
        assert "quota.reserved" in payload


@pytest.mark.parametrize(
    ("plan", "deployment", "requested", "expected"),
    [(8, 2, 4, 2), (2, 8, 4, 2), (None, 8, 4, 4), (None, 3, 4, 3), (8, 8, 1, 1)],
)
def test_concorrencia_autorizada_respeita_o_menor_teto(
    plan: int | None, deployment: int, requested: int, expected: int
) -> None:
    snapshot = make_quota_snapshot(max_concurrency=plan)
    with quota_env(snapshot) as env:
        command = make_reserve_command(snapshot, deployment, requested_concurrency=requested)
        assert env.repo.reserve_and_create_run(command).max_concurrency == expected


def test_ultima_unidade_do_periodo_e_aceita_e_a_seguinte_excede() -> None:
    snapshot = make_quota_snapshot(max_runs_per_period=1)
    with quota_env(snapshot) as env:
        env.repo.reserve_and_create_run(make_reserve_command(snapshot))
        before = table_items(env.client)
        second = make_reserve_command(snapshot, run_id="run-02", idempotency_key="req-02")
        with pytest.raises(QuotaExceeded, match="reason=max_runs_per_period_exceeded limit=1"):
            env.repo.reserve_and_create_run(second)
        assert table_items(env.client) == before


def test_limite_zero_rejeita_sem_escrever() -> None:
    snapshot = make_quota_snapshot(max_runs_per_period=0)
    with quota_env(snapshot) as env:
        before = table_items(env.client)
        with pytest.raises(QuotaExceeded, match="limit=0"):
            env.repo.reserve_and_create_run(make_reserve_command(snapshot))
        assert table_items(env.client) == before


def test_limite_ausente_nao_restringe_runs() -> None:
    snapshot = make_quota_snapshot(max_runs_per_period=None)
    with quota_env(snapshot) as env:
        for index in range(3):
            command = make_reserve_command(
                snapshot, run_id=f"run-{index}", idempotency_key=f"req-{index}"
            )
            env.repo.reserve_and_create_run(command)
        assert _usage(env.client, "consumed_runs") == 3


def test_snapshot_mais_novo_nega_a_reserva_sem_escrever() -> None:
    old = make_quota_snapshot()
    with quota_env(old) as env:
        seed_snapshot(env.client, replace(old, entitlement_version=2))
        before = table_items(env.client)
        with pytest.raises(EntitlementDenied, match="reason=snapshot_changed"):
            env.repo.reserve_and_create_run(make_reserve_command(old))
        assert table_items(env.client) == before


def test_status_de_assinatura_alterado_nega_a_reserva() -> None:
    old = make_quota_snapshot()
    with quota_env(old) as env:
        seed_snapshot(env.client, replace(old, subscription_status=SubscriptionStatus.PAST_DUE))
        with pytest.raises(EntitlementDenied, match="reason=snapshot_changed"):
            env.repo.reserve_and_create_run(make_reserve_command(old))


def test_snapshot_expirado_nega_a_reserva() -> None:
    snapshot = make_quota_snapshot()
    with quota_env(snapshot) as env:
        env.clock.advance(snapshot.valid_until - NOW + timedelta(seconds=1))
        expires_at = snapshot.valid_until + timedelta(hours=1)
        command = replace(make_reserve_command(snapshot), expires_at=expires_at)
        with pytest.raises(EntitlementDenied, match="reason=snapshot_changed"):
            env.repo.reserve_and_create_run(command)


def test_snapshot_removido_entre_leitura_e_escrita_nega_a_reserva() -> None:
    with quota_env() as env:
        key = item_key(*entitlement_snapshot_key(ACCOUNT))
        repo = _racing(env, lambda: env.client.delete_item(TableName=TABLE_NAME, Key=key))
        with pytest.raises(EntitlementDenied, match="reason=snapshot_changed"):
            repo.reserve_and_create_run(make_reserve_command())


def test_mesmo_run_com_outra_chave_gera_conflito_permanente() -> None:
    with quota_env() as env:
        env.repo.reserve_and_create_run(make_reserve_command())
        with pytest.raises(PermanentBillingError) as error:
            env.repo.reserve_and_create_run(make_reserve_command(idempotency_key="req-02"))
        assert error.value.code == "quota_reservation_conflict"
        assert _usage(env.client, "consumed_runs") == 1


def test_corrida_com_mesma_chave_retorna_autorizacao_do_vencedor() -> None:
    with quota_env() as env:
        repo = _racing(env, partial(env.repo.reserve_and_create_run, make_reserve_command()))
        authorization = repo.reserve_and_create_run(make_reserve_command())
        assert authorization.budget_reservation_id == "res-run-01"
        assert _usage(env.client, "consumed_runs") == 1


def test_corrida_com_mesma_chave_e_payload_diferente_conflita() -> None:
    with quota_env() as env:
        repo = _racing(env, partial(env.repo.reserve_and_create_run, make_reserve_command()))
        with pytest.raises(IdempotencyConflict, match="key=req-01"):
            repo.reserve_and_create_run(make_reserve_command(request_hash=HASH_B))


def test_corrida_pela_ultima_unidade_classifica_como_quota_excedida() -> None:
    snapshot = make_quota_snapshot(max_runs_per_period=1)
    with quota_env(snapshot) as env:
        other = make_reserve_command(snapshot, run_id="run-02", idempotency_key="req-02")
        repo = _racing(env, lambda: env.repo.reserve_and_create_run(other))
        with pytest.raises(QuotaExceeded, match="reason=max_runs_per_period_exceeded limit=1"):
            repo.reserve_and_create_run(make_reserve_command(snapshot))


def test_analytics_reserva_budget_e_grava_evento() -> None:
    with quota_env() as env:
        authorization = env.repo.reserve_analytics(make_analytics_command())
        assert authorization.max_scan_bytes == 1_000
        assert authorization.budget_reservation_id == "res-query-01"
        assert authorization.authorized_at == NOW
        reservation, _ = decode_reservation(
            _get(env.client, reservation_key(ACCOUNT, NOW, "res-query-01"))
        )
        assert reservation.kind is ReservationKind.ANALYTICS
        assert reservation.resource_id == "query-01"
        assert (reservation.reserved_runs, reservation.consumed_runs) == (0, 0)
        assert reservation.reserved_scan_bytes == 1_000
        assert _usage(env.client, ANALYTICS_SCAN.reserved) == 1_000
        assert _usage(env.client, ANALYTICS_SCAN.committed) == 1_000
        assert _usage(env.client, "consumed_runs") == 0
        event_id = deterministic_id("quota.reserved", ACCOUNT, "res-query-01")
        payload = _get(env.client, outbox_key(event_id))["payload"]["S"]
        assert '"kind":"analytics"' in payload
        assert '"query_id":"query-01"' in payload


def test_analytics_repetida_retorna_mesma_autorizacao() -> None:
    with quota_env() as env:
        first = env.repo.reserve_analytics(make_analytics_command())
        assert env.repo.reserve_analytics(make_analytics_command()) == first
        assert _usage(env.client, ANALYTICS_SCAN.committed) == 1_000
        item = _get(env.client, idempotency_key(TENANT, ANALYTICS_SCOPE, "aq-01"))
        assert decode_analytics_result(item) == first


def test_analytics_mesma_chave_payload_diferente_conflita() -> None:
    with quota_env() as env:
        env.repo.reserve_analytics(make_analytics_command())
        with pytest.raises(IdempotencyConflict, match="key=aq-01"):
            env.repo.reserve_analytics(make_analytics_command(request_hash=HASH_B))


def test_analytics_aceita_os_ultimos_bytes_do_budget() -> None:
    snapshot = make_quota_snapshot(athena_scan_budget_bytes=1_000)
    with quota_env(snapshot) as env:
        env.repo.reserve_analytics(make_analytics_command(snapshot))
        other = make_analytics_command(
            snapshot, query_id="query-02", idempotency_key="aq-02", estimated_scan_bytes=1
        )
        with pytest.raises(QuotaExceeded, match="reason=athena_scan_budget_exceeded limit=1000"):
            env.repo.reserve_analytics(other)
        assert _usage(env.client, ANALYTICS_SCAN.committed) == 1_000


def test_analytics_acima_do_budget_rejeita_sem_escrever() -> None:
    snapshot = make_quota_snapshot(athena_scan_budget_bytes=999)
    with quota_env(snapshot) as env:
        before = table_items(env.client)
        with pytest.raises(QuotaExceeded, match="limit=999"):
            env.repo.reserve_analytics(make_analytics_command(snapshot))
        assert table_items(env.client) == before


def test_analytics_sem_budget_nao_restringe() -> None:
    snapshot = make_quota_snapshot(athena_scan_budget_bytes=None)
    with quota_env(snapshot) as env:
        command = make_analytics_command(snapshot, estimated_scan_bytes=10**12)
        assert env.repo.reserve_analytics(command).max_scan_bytes == 10**12


def test_analytics_com_snapshot_novo_nega_a_reserva() -> None:
    old = make_quota_snapshot()
    with quota_env(old) as env:
        seed_snapshot(env.client, replace(old, entitlement_version=2))
        with pytest.raises(EntitlementDenied, match="reason=snapshot_changed"):
            env.repo.reserve_analytics(make_analytics_command(old))


def test_analytics_reserva_existente_gera_conflito_permanente() -> None:
    with quota_env() as env:
        env.repo.reserve_analytics(make_analytics_command())
        again = make_analytics_command(idempotency_key="aq-02")
        with pytest.raises(PermanentBillingError) as error:
            env.repo.reserve_analytics(again)
        assert error.value.code == "quota_reservation_conflict"


def test_analytics_sem_budget_com_reserva_existente_gera_conflito_permanente() -> None:
    snapshot = make_quota_snapshot(athena_scan_budget_bytes=None)
    with quota_env(snapshot) as env:
        env.repo.reserve_analytics(make_analytics_command(snapshot))
        again = make_analytics_command(snapshot, idempotency_key="aq-02")
        with pytest.raises(PermanentBillingError, match="quota_reservation_conflict"):
            env.repo.reserve_analytics(again)


def test_analytics_corrida_com_mesma_chave_retorna_autorizacao_do_vencedor() -> None:
    with quota_env() as env:
        repo = _racing(env, lambda: env.repo.reserve_analytics(make_analytics_command()))
        authorization = repo.reserve_analytics(make_analytics_command())
        assert authorization.budget_reservation_id == "res-query-01"
        assert _usage(env.client, ANALYTICS_SCAN.committed) == 1_000


def test_analytics_corrida_pelo_ultimo_budget_classifica_como_quota_excedida() -> None:
    snapshot = make_quota_snapshot(athena_scan_budget_bytes=1_000)
    with quota_env(snapshot) as env:
        other = make_analytics_command(snapshot, query_id="query-02", idempotency_key="aq-02")
        repo = _racing(env, lambda: env.repo.reserve_analytics(other))
        with pytest.raises(QuotaExceeded, match="reason=athena_scan_budget_exceeded"):
            repo.reserve_analytics(make_analytics_command(snapshot))


@pytest.mark.parametrize("offset", [timedelta(0), -timedelta(seconds=1)])
def test_expiracao_nao_futura_rejeita_run_sem_escrever(offset: timedelta) -> None:
    with quota_env() as env:
        before = table_items(env.client)
        command = replace(make_reserve_command(), expires_at=NOW + offset)
        with pytest.raises(PermanentBillingError) as error:
            env.repo.reserve_and_create_run(command)
        assert error.value.code == "invalid_reservation_expiry"
        assert table_items(env.client) == before


def test_expiracao_nao_futura_rejeita_analytics_sem_escrever() -> None:
    with quota_env() as env:
        before = table_items(env.client)
        command = replace(make_analytics_command(), expires_at=NOW)
        with pytest.raises(PermanentBillingError) as error:
            env.repo.reserve_analytics(command)
        assert error.value.code == "invalid_reservation_expiry"
        assert table_items(env.client) == before
