"""Classificação de falhas transitórias e expiração da idempotência das reservas."""

from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest
from botocore.exceptions import ClientError

from cnes_domain.billing.commands import (
    ConsumeReservationCommand,
    ReleaseCapacityCommand,
    ReleaseReservationCommand,
)
from cnes_domain.billing.errors import (
    EntitlementDenied,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.inbox import ReservationRecoveryRequest
from cnes_domain.billing.models import ReservationStatus, SubscriptionStatus
from cnes_infra.billing import dynamodb_quota_items as codec
from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations
from cnes_infra.billing.dynamodb_quota_items import IDEMPOTENCY_TTL
from cnes_infra.billing.keys import capacity_reservation_key
from cnes_infra.control_plane.dynamodb_codec import item_key
from cnes_infra.control_plane.dynamodb_keys import run_entity_key
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    HASH_B,
    RESERVATION_TTL,
    TENANT,
    QuotaEnv,
    make_analytics_command,
    make_capacity_command,
    make_quota_snapshot,
    make_reserve_command,
    quota_env,
)

AFTER_IDEMPOTENCY = IDEMPOTENCY_TTL + timedelta(minutes=1)


class _AfterCancellation:
    def __init__(self, inner: Any, hook: Callable[[], object]) -> None:
        self._inner = inner
        self._hook: Callable[[], object] | None = hook

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def transact_write_items(self, **request: Any) -> Any:
        try:
            return self._inner.transact_write_items(**request)
        except ClientError:
            hook, self._hook = self._hook, None
            if hook is not None:
                hook()
            raise


def _after_cancellation(env: QuotaEnv, hook: Callable[[], object]) -> DynamoQuotaReservations:
    return DynamoQuotaReservations(_AfterCancellation(env.client, hook), TABLE_NAME, env.clock.now)


def _later(command: Any, env: QuotaEnv) -> Any:
    return replace(command, expires_at=env.clock.now() + timedelta(minutes=15))


def test_budget_liberado_entre_cancelamento_e_releitura_e_retentavel() -> None:
    snapshot = make_quota_snapshot(athena_scan_budget_bytes=1_500)
    with quota_env(snapshot) as env:
        env.repo.reserve_analytics(make_analytics_command(snapshot))
        release = ReleaseReservationCommand(ACCOUNT, "res-query-01", NOW, "query_canceled")
        repo = _after_cancellation(env, lambda: env.repo.release(release))
        second = make_analytics_command(snapshot, query_id="query-02", idempotency_key="aq-02")
        with pytest.raises(RetryableBillingError) as error:
            repo.reserve_analytics(second)
        assert error.value.code == "quota_reservation_contended"
        assert env.repo.reserve_analytics(second).budget_reservation_id == "res-query-02"


def test_unidade_de_run_liberada_entre_cancelamento_e_releitura_e_retentavel() -> None:
    snapshot = make_quota_snapshot(max_runs_per_period=1)
    with quota_env(snapshot) as env:
        env.repo.reserve_and_create_run(make_reserve_command(snapshot))
        usage_key_item = next(
            item for item in env.client.scan(TableName=TABLE_NAME)["Items"]
            if item["entity"]["S"] == "BILLINGUSAGE"
        )
        key = {"pk": usage_key_item["pk"], "sk": usage_key_item["sk"]}

        def free_unit() -> None:
            env.client.update_item(
                TableName=TABLE_NAME, Key=key, UpdateExpression="SET consumed_runs = :zero",
                ExpressionAttributeValues={":zero": {"N": "0"}},
            )

        repo = _after_cancellation(env, free_unit)
        second = make_reserve_command(snapshot, run_id="run-02", idempotency_key="req-02")
        with pytest.raises(RetryableBillingError) as error:
            repo.reserve_and_create_run(second)
        assert error.value.code == "quota_reservation_contended"


def test_vaga_liberada_entre_cancelamento_e_releitura_e_retentavel() -> None:
    with quota_env() as env:
        first = env.repo.reserve_capacity(make_capacity_command(limit=1))
        release = ReleaseCapacityCommand(ACCOUNT, first.reservation_id, NOW, "agent_removed")
        repo = _after_cancellation(env, lambda: env.repo.release_capacity(release))
        second = make_capacity_command(limit=1, resource_id="agent-02", idempotency_key="cap-02")
        with pytest.raises(RetryableBillingError) as error:
            repo.reserve_capacity(second)
        assert error.value.code == "capacity_reservation_contended"
        assert env.repo.reserve_capacity(second).resource_id == "agent-02"


def test_chave_expirada_ainda_presente_e_tratada_como_ausente() -> None:
    with quota_env() as env:
        env.repo.reserve_and_create_run(make_reserve_command())
        env.clock.advance(AFTER_IDEMPOTENCY)
        command = _later(make_reserve_command(run_id="run-02", request_hash=HASH_B), env)
        authorization = env.repo.reserve_and_create_run(command)
        assert authorization.budget_reservation_id == "res-run-02"
        assert env.repo.reserve_and_create_run(command) == authorization


def test_retry_apos_expiracao_da_chave_conflita_de_forma_deterministica() -> None:
    with quota_env() as env:
        env.repo.reserve_and_create_run(make_reserve_command())
        env.clock.advance(AFTER_IDEMPOTENCY)
        with pytest.raises(PermanentBillingError) as error:
            env.repo.reserve_and_create_run(_later(make_reserve_command(), env))
        assert error.value.code == "quota_reservation_conflict"


def test_analytics_com_chave_expirada_reserva_de_novo() -> None:
    with quota_env() as env:
        env.repo.reserve_analytics(make_analytics_command())
        env.clock.advance(AFTER_IDEMPOTENCY)
        command = _later(make_analytics_command(query_id="query-02"), env)
        assert env.repo.reserve_analytics(command).budget_reservation_id == "res-query-02"


def test_capacidade_com_chave_expirada_nao_reproduz_resultado_antigo() -> None:
    with quota_env() as env:
        env.repo.reserve_capacity(make_capacity_command())
        env.clock.advance(AFTER_IDEMPOTENCY)
        with pytest.raises(PermanentBillingError) as error:
            env.repo.reserve_capacity(make_capacity_command())
        assert error.value.code == "capacity_reservation_conflict"


def _access_snapshot(**changes: Any) -> Any:
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


class _BeforeTransact:
    def __init__(self, inner: Any, hook: Callable[[], None]) -> None:
        self._inner = inner
        self._hook: Callable[[], None] | None = hook

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def transact_write_items(self, **request: Any) -> Any:
        hook, self._hook = self._hook, None
        if hook is not None:
            hook()
        return self._inner.transact_write_items(**request)


def test_run_removido_durante_renovacao_mantem_reserva_vencida_para_liberacao() -> None:
    with quota_env() as env:
        env.repo.reserve_and_create_run(make_reserve_command())
        env.clock.advance(RESERVATION_TTL + timedelta(minutes=1))
        run_key = item_key(*run_entity_key(TENANT, "run-01"))
        client = _BeforeTransact(
            env.client, lambda: env.client.delete_item(TableName=TABLE_NAME, Key=run_key)
        )
        repo = DynamoQuotaReservations(client, TABLE_NAME, env.clock.now)
        request = ReservationRecoveryRequest(now=env.clock.now(), limit=10, cursor=None)
        assert repo.reconcile_expired_reservations(request).released == 0
        assert env.repo.reconcile_expired_reservations(request).released == 1


def test_replay_de_capacidade_liberada_reflete_o_estado_atual() -> None:
    with quota_env() as env:
        first = env.repo.reserve_capacity(make_capacity_command())
        release = ReleaseCapacityCommand(ACCOUNT, first.reservation_id, NOW, "agent_failed")
        env.repo.release_capacity(release)
        replay = env.repo.reserve_capacity(make_capacity_command())
        assert replay.reservation_id == first.reservation_id
        assert replay.status is ReservationStatus.RELEASED


def test_replay_de_capacidade_sem_item_base_devolve_resultado_gravado() -> None:
    with quota_env() as env:
        first = env.repo.reserve_capacity(make_capacity_command())
        key = item_key(*capacity_reservation_key(ACCOUNT, first.reservation_id))
        env.client.delete_item(TableName=TABLE_NAME, Key=key)
        assert env.repo.reserve_capacity(make_capacity_command()) == first


def _release_expired_analytics(env: QuotaEnv) -> None:
    env.repo.reserve_analytics(make_analytics_command())
    env.clock.advance(RESERVATION_TTL + timedelta(minutes=1))
    request = ReservationRecoveryRequest(now=env.clock.now(), limit=10, cursor=None)
    assert env.repo.reconcile_expired_reservations(request).released == 1


def test_replay_de_analytics_liberada_e_rejeitado() -> None:
    with quota_env() as env:
        _release_expired_analytics(env)
        with pytest.raises(PermanentBillingError) as error:
            env.repo.reserve_analytics(make_analytics_command())
        assert error.value.code == "quota_reservation_released"


def test_replay_de_analytics_consumida_devolve_autorizacao() -> None:
    with quota_env() as env:
        first = env.repo.reserve_analytics(make_analytics_command())
        consume = ConsumeReservationCommand(ACCOUNT, "res-query-01", 10, NOW)
        env.repo.consume(consume)
        assert env.repo.reserve_analytics(make_analytics_command()) == first


def test_replay_de_analytics_sem_reserva_localizada_devolve_autorizacao() -> None:
    with quota_env() as env:
        first = env.repo.reserve_analytics(make_analytics_command())
        for item in env.client.scan(TableName=TABLE_NAME)["Items"]:
            if item["entity"]["S"] == "QUOTARESERVATION":
                key = {"pk": item["pk"], "sk": item["sk"]}
                env.client.delete_item(TableName=TABLE_NAME, Key=key)
        assert env.repo.reserve_analytics(make_analytics_command()) == first


def test_replay_de_run_com_reserva_liberada_pelo_recovery_e_rejeitado() -> None:
    with quota_env() as env:
        env.repo.reserve_and_create_run(make_reserve_command())
        env.client.delete_item(
            TableName=TABLE_NAME, Key=item_key(*run_entity_key(TENANT, "run-01"))
        )
        env.clock.advance(RESERVATION_TTL + timedelta(minutes=1))
        request = ReservationRecoveryRequest(now=env.clock.now(), limit=10, cursor=None)
        assert env.repo.reconcile_expired_reservations(request).released == 1
        with pytest.raises(PermanentBillingError) as error:
            env.repo.reserve_and_create_run(make_reserve_command())
        assert error.value.code == "quota_reservation_released"
