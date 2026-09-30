"""Recuperação de reservas de quota abandonadas converge após crashes e perdas."""
from __future__ import annotations

import pytest

pytest.importorskip("moto")

from datetime import timedelta
from typing import Any

from botocore.exceptions import ClientError

from cnes_domain.billing.errors import BillingDependencyError
from cnes_domain.billing.inbox import (
    ReservationRecoveryRequest,
    ReservationRecoveryResult,
)
from cnes_domain.billing.models import ReservationStatus
from cnes_domain.control_plane.enums import RunState
from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations
from cnes_infra.billing.dynamodb_quota_items import decode_reservation
from cnes_infra.billing.keys import usage_key
from cnes_infra.control_plane.dynamodb_codec import item_key
from cnes_infra.control_plane.dynamodb_keys import run_entity_key
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    RESERVATION_TTL,
    TENANT,
    QuotaEnv,
    make_reserve_command,
    quota_env,
    table_items,
)

pytestmark = [pytest.mark.chaos]

_PAST_EXPIRY = RESERVATION_TTL + timedelta(minutes=1)
_ESTIMATE = 1_000


class _FailingTransact:
    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.failures_left = 1

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        if self.failures_left > 0:
            self.failures_left -= 1
            raise ClientError({"Error": {"Code": "InternalServerError"}}, "TransactWriteItems")
        return self._inner.transact_write_items(**kwargs)


def _reconcile(repo: DynamoQuotaReservations, env: QuotaEnv) -> ReservationRecoveryResult:
    request = ReservationRecoveryRequest(now=env.clock.now(), limit=50, cursor=None)
    return repo.reconcile_expired_reservations(request)


def _reserve(env: QuotaEnv) -> None:
    env.repo.reserve_and_create_run(make_reserve_command())


def _snapshot(env: QuotaEnv) -> list[dict[str, Any]]:
    return table_items(env.client)


def _reservation(env: QuotaEnv) -> Any:
    item = next(i for i in _snapshot(env) if i.get("entity", {}).get("S") == "QUOTARESERVATION")
    return decode_reservation(item)[0]


def _counters(env: QuotaEnv) -> dict[str, int]:
    key = usage_key(ACCOUNT, NOW)
    item = env.client.get_item(TableName=TABLE_NAME, Key=item_key(*key), ConsistentRead=True)
    return {name: int(value["N"]) for name, value in item["Item"].items() if "N" in value}


def _drive_run(env: QuotaEnv, state: RunState) -> None:
    run = env.control_plane.get_run(TENANT, "run-01")
    assert run is not None
    env.control_plane.put_run(run.model_copy(update={"state": state, "missing_sources": ()}))


def _lose_run(env: QuotaEnv) -> None:
    run_pk, run_sk = run_entity_key(TENANT, "run-01")
    for item in _snapshot(env):
        pk, sk = item["pk"]["S"], item["sk"]["S"]
        if pk == run_pk and (sk == run_sk or sk.startswith("RUN_DEP#")):
            env.client.delete_item(TableName=TABLE_NAME, Key=item_key(pk, sk))


def test_liquida_reserva_de_run_terminal_e_reconcilia_de_forma_idempotente() -> None:
    with quota_env() as env:
        _reserve(env)
        _drive_run(env, RunState.PUBLISHED_DEGRADED)
        env.clock.advance(_PAST_EXPIRY)

        _reconcile(env.repo, env)
        settled = _snapshot(env)
        second = _reconcile(env.repo, env)

        assert _reservation(env).status is ReservationStatus.CONSUMED
        counters = _counters(env)
        assert counters["run_reserved_scan_bytes"] == 0
        assert counters["run_consumed_scan_bytes"] == _ESTIMATE
        assert counters["run_committed_scan_bytes"] == _ESTIMATE
        assert counters["consumed_runs"] == 1
        assert (second.examined, second.released) == (0, 0)
        assert _snapshot(env) == settled


def test_libera_reserva_quando_run_foi_perdido_fora_de_banda() -> None:
    with quota_env() as env:
        _reserve(env)
        _lose_run(env)
        env.clock.advance(_PAST_EXPIRY)

        result = _reconcile(env.repo, env)

        assert result.released == 1
        assert _reservation(env).status is ReservationStatus.RELEASED
        counters = _counters(env)
        assert counters["consumed_runs"] == 0
        assert counters["run_reserved_scan_bytes"] == 0
        assert counters["run_committed_scan_bytes"] == 0


def test_run_ativo_nunca_perde_unidade_consumida_entre_ciclos() -> None:
    with quota_env() as env:
        _reserve(env)
        for _ in range(3):
            env.clock.advance(_PAST_EXPIRY)

            result = _reconcile(env.repo, env)

            assert result.released == 0
            assert _reservation(env).status is ReservationStatus.RESERVED
            assert _counters(env)["consumed_runs"] == 1
        assert env.control_plane.get_run(TENANT, "run-01") is not None


def test_converge_apos_falha_entre_descoberta_e_liquidacao() -> None:
    with quota_env() as env:
        _reserve(env)
        _lose_run(env)
        env.clock.advance(_PAST_EXPIRY)
        client = _FailingTransact(env.client)
        flaky = DynamoQuotaReservations(client, TABLE_NAME, env.clock.now)

        with pytest.raises(BillingDependencyError):
            _reconcile(flaky, env)
        assert _reservation(env).status is ReservationStatus.RESERVED
        assert _counters(env)["consumed_runs"] == 1
        result = _reconcile(flaky, env)

        assert result.released == 1
        assert _reservation(env).status is ReservationStatus.RELEASED
        assert _counters(env)["consumed_runs"] == 0
        assert _reservation(env).consumed_runs == 0
