"""Testes da recuperação de reservas de quota abandonadas."""

import base64
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pytest
from botocore.exceptions import ClientError

from cnes_domain.billing.errors import BillingDependencyError, PermanentBillingError
from cnes_domain.billing.inbox import ReservationRecoveryRequest, ReservationRecoveryResult
from cnes_domain.billing.models import (
    CapacityKind,
    CapacityReservation,
    QuotaReservation,
    ReservationKind,
    ReservationStatus,
)
from cnes_domain.control_plane.entities import Agent, Run, Tenant
from cnes_domain.control_plane.enums import AgentState, RunState
from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations
from cnes_infra.billing.dynamodb_quota_items import (
    decode_capacity_reservation,
    decode_reservation,
    encode_capacity_reservation,
    encode_reservation,
    reservation_item_key,
)
from cnes_infra.billing.keys import (
    capacity_reservation_key,
    capacity_usage_key,
    entitlement_snapshot_key,
    usage_key,
)
from cnes_infra.control_plane.dynamodb_codec import item_key
from packages.cnes_infra.tests.billing.billing_factories import TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    DEPENDENCIES,
    HASH_A,
    NOW,
    RESERVATION_TTL,
    TENANT,
    QuotaEnv,
    quota_env,
)

if TYPE_CHECKING:
    from collections.abc import Callable

ESTIMATE = 1_000
CAPACITY_CASES = (
    (CapacityKind.TENANT, TENANT, "tenant_count"),
    (CapacityKind.AGENT, "agent-01", "agent_count"),
)
PAST_EXPIRY = RESERVATION_TTL + timedelta(minutes=1)


class _Client:
    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.before_transact: Callable[[], None] | None = None
        self.query_items: list[dict[str, Any]] | None = None
        self.query_error: ClientError | None = None
        self.transact_error: ClientError | None = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def query(self, **kwargs: Any) -> dict[str, Any]:
        if self.query_error is not None:
            raise self.query_error
        if self.query_items is not None:
            return {"Items": self.query_items}
        return self._inner.query(**kwargs)

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        if self.transact_error is not None:
            raise self.transact_error
        hook, self.before_transact = self.before_transact, None
        if hook is not None:
            hook()
        return self._inner.transact_write_items(**kwargs)


def _reservation(
    kind: ReservationKind = ReservationKind.RUN, resource_id: str = "run-01", **changes: Any
) -> QuotaReservation:
    reservation = QuotaReservation(
        reservation_id=f"res-{resource_id}",
        billing_account_id=ACCOUNT,
        resource_id=resource_id,
        kind=kind,
        period_start=NOW,
        reserved_runs=0,
        reserved_scan_bytes=ESTIMATE,
        consumed_runs=1 if kind is ReservationKind.RUN else 0,
        consumed_scan_bytes=0,
        status=ReservationStatus.RESERVED,
        created_at=NOW,
        expires_at=NOW + RESERVATION_TTL,
    )
    return replace(reservation, **changes)


def _number(value: int) -> dict[str, str]:
    return {"N": str(value)}


def _seed_usage(env: QuotaEnv, kind: ReservationKind, runs: int) -> None:
    key = usage_key(ACCOUNT, NOW)
    item = {"pk": {"S": key[0]}, "sk": {"S": key[1]}, "consumed_runs": _number(runs)}
    for suffix in ("reserved", "committed"):
        item[f"{kind.value}_{suffix}_scan_bytes"] = _number(ESTIMATE)
    item[f"{kind.value}_consumed_scan_bytes"] = _number(0)
    env.client.put_item(TableName=TABLE_NAME, Item=item)


def _seed(env: QuotaEnv, reservation: QuotaReservation) -> None:
    runs = reservation.consumed_runs
    _seed_usage(env, reservation.kind, runs)
    env.client.put_item(TableName=TABLE_NAME, Item=encode_reservation(reservation, TENANT))


def _run(state: RunState, run_id: str = "run-01") -> Run:
    return Run(
        tenant_id=TENANT,
        run_id=run_id,
        competencia="2026-08",
        dataset_name="cnes_vinculos",
        state=state,
        dependencies=DEPENDENCIES,
        missing_sources=("CNES/LFCES",) if state is RunState.WAITING_INPUTS else (),
        created_at=NOW,
    )


def _reconcile(
    repo: DynamoQuotaReservations, env: QuotaEnv, limit: int = 10, cursor: str | None = None
) -> ReservationRecoveryResult:
    request = ReservationRecoveryRequest(now=env.clock.now(), limit=limit, cursor=cursor)
    return repo.reconcile_expired_reservations(request)


def _expire(env: QuotaEnv) -> None:
    env.clock.advance(PAST_EXPIRY)


def _stored(env: QuotaEnv, reservation: QuotaReservation) -> tuple[QuotaReservation, dict]:
    key = reservation_item_key(reservation)
    item = env.client.get_item(TableName=TABLE_NAME, Key=item_key(*key), ConsistentRead=True)
    return decode_reservation(item["Item"])[0], item["Item"]


def _counters(env: QuotaEnv) -> dict[str, int]:
    key = usage_key(ACCOUNT, NOW)
    item = env.client.get_item(TableName=TABLE_NAME, Key=item_key(*key), ConsistentRead=True)
    return {n: int(v["N"]) for n, v in item["Item"].items() if "N" in v}


def _capacity(kind: CapacityKind, resource_id: str) -> CapacityReservation:
    return CapacityReservation(
        reservation_id=f"cap-{resource_id}",
        billing_account_id=ACCOUNT,
        resource_id=resource_id,
        kind=kind,
        status=ReservationStatus.RESERVED,
        created_at=NOW,
        expires_at=NOW + RESERVATION_TTL,
    )


def _seed_capacity(env: QuotaEnv, reservation: CapacityReservation) -> None:
    key = capacity_usage_key(ACCOUNT)
    counter = f"{reservation.kind.value}_count"
    usage = {"pk": {"S": key[0]}, "sk": {"S": key[1]}, counter: _number(1)}
    env.client.put_item(TableName=TABLE_NAME, Item=usage)
    env.client.put_item(TableName=TABLE_NAME, Item=encode_capacity_reservation(reservation, TENANT))


def _stored_capacity(env: QuotaEnv, reservation: CapacityReservation) -> CapacityReservation:
    key = capacity_reservation_key(ACCOUNT, reservation.reservation_id)
    item = env.client.get_item(TableName=TABLE_NAME, Key=item_key(*key), ConsistentRead=True)
    return decode_capacity_reservation(item["Item"])[0]


def _capacity_counter(env: QuotaEnv, name: str) -> int:
    key = capacity_usage_key(ACCOUNT)
    item = env.client.get_item(TableName=TABLE_NAME, Key=item_key(*key), ConsistentRead=True)
    return int(item["Item"][name]["N"])


def _agent() -> Agent:
    return Agent(
        tenant_id=TENANT,
        agent_id="agent-01",
        state=AgentState.ACTIVE,
        version="1.0",
        certificate_fingerprint=HASH_A,
        last_seen_at=None,
        created_at=NOW,
    )


def test_libera_reserva_completa_quando_run_esta_ausente() -> None:
    with quota_env() as env:
        reservation = _reservation()
        _seed(env, reservation)
        _expire(env)

        result = _reconcile(env.repo, env)

        stored, _ = _stored(env, reservation)
        assert (result.examined, result.released, result.next_cursor) == (1, 1, None)
        assert (stored.status, stored.consumed_runs) == (ReservationStatus.RELEASED, 0)
        counters = _counters(env)
        assert counters["consumed_runs"] == 0
        assert counters["run_reserved_scan_bytes"] == 0
        assert counters["run_committed_scan_bytes"] == 0


@pytest.mark.parametrize("state", [RunState.PUBLISHED_DEGRADED, RunState.FAILED])
def test_liquida_como_consumida_quando_run_esta_terminal(state: RunState) -> None:
    with quota_env() as env:
        reservation = _reservation()
        _seed(env, reservation)
        env.control_plane.put_run(_run(state))
        _expire(env)

        result = _reconcile(env.repo, env)

        stored, _ = _stored(env, reservation)
        assert result.released == 0
        assert stored.status is ReservationStatus.CONSUMED
        assert stored.consumed_scan_bytes == ESTIMATE
        counters = _counters(env)
        assert counters["consumed_runs"] == 1
        assert counters["run_reserved_scan_bytes"] == 0
        assert counters["run_consumed_scan_bytes"] == ESTIMATE
        assert counters["run_committed_scan_bytes"] == ESTIMATE


@pytest.mark.parametrize("state", [RunState.WAITING_INPUTS, RunState.PROCESSING])
def test_renova_lease_sem_liberar_run_ativo(state: RunState) -> None:
    with quota_env() as env:
        reservation = _reservation()
        _seed(env, reservation)
        env.control_plane.put_run(_run(state))
        _expire(env)

        result = _reconcile(env.repo, env)

        stored, item = _stored(env, reservation)
        assert result.released == 0
        assert stored.status is ReservationStatus.RESERVED
        assert stored.expires_at == env.clock.now() + RESERVATION_TTL
        assert item["gsi1sk"]["S"].startswith(stored.expires_at.strftime("%Y-%m-%d"))
        assert _counters(env)["consumed_runs"] == 1
        assert _reconcile(env.repo, env).examined == 0


def test_ignora_reserva_ainda_nao_vencida() -> None:
    with quota_env() as env:
        reservation = _reservation()
        _seed(env, reservation)

        result = _reconcile(env.repo, env)

        assert (result.examined, result.released) == (0, 0)
        assert _stored(env, reservation)[0] == reservation


def _stale_repo(env: QuotaEnv, rows: list[dict[str, Any]]) -> DynamoQuotaReservations:
    client = _Client(env.client)
    client.query_items = rows
    return DynamoQuotaReservations(client, TABLE_NAME, env.clock.now)


def _row(key: tuple[str, str]) -> dict[str, Any]:
    return {"pk": {"S": key[0]}, "sk": {"S": key[1]}}


def test_ignora_candidato_do_indice_sem_item_base() -> None:
    with quota_env() as env:
        repo = _stale_repo(env, [_row(reservation_item_key(_reservation()))])

        result = _reconcile(repo, env)

        assert (result.examined, result.released) == (1, 0)


def test_ignora_candidato_obsoleto_ja_liquidado() -> None:
    with quota_env() as env:
        settled = _reservation(status=ReservationStatus.RELEASED)
        _seed(env, settled)
        _expire(env)
        repo = _stale_repo(env, [_row(reservation_item_key(settled))])

        assert _reconcile(repo, env).released == 0
        assert _counters(env)["consumed_runs"] == 1


def test_ignora_candidato_obsoleto_ainda_nao_vencido() -> None:
    with quota_env() as env:
        reservation = _reservation(ReservationKind.ANALYTICS, "query-01")
        _seed(env, reservation)
        repo = _stale_repo(env, [_row(reservation_item_key(reservation))])

        assert _reconcile(repo, env).released == 0
        assert _stored(env, reservation)[0].status is ReservationStatus.RESERVED


def test_ignora_candidato_de_entidade_desconhecida() -> None:
    with quota_env() as env:
        repo = _stale_repo(env, [_row(entitlement_snapshot_key(ACCOUNT))])

        assert _reconcile(repo, env).released == 0


def test_ignora_capacidade_obsoleta_ainda_nao_vencida() -> None:
    with quota_env() as env:
        capacity = _capacity(CapacityKind.AGENT, "agent-01")
        _seed_capacity(env, capacity)
        key = capacity_reservation_key(ACCOUNT, capacity.reservation_id)
        repo = _stale_repo(env, [_row(key)])

        assert _reconcile(repo, env).released == 0
        assert _stored_capacity(env, capacity).status is ReservationStatus.RESERVED


def test_libera_scan_de_analytics_vencido() -> None:
    with quota_env() as env:
        reservation = _reservation(ReservationKind.ANALYTICS, "query-01")
        _seed(env, reservation)
        _expire(env)

        result = _reconcile(env.repo, env)

        assert result.released == 1
        assert _stored(env, reservation)[0].status is ReservationStatus.RELEASED
        counters = _counters(env)
        assert counters["analytics_reserved_scan_bytes"] == 0
        assert counters["analytics_committed_scan_bytes"] == 0


def test_mantem_contador_quando_recurso_de_capacidade_existe() -> None:
    for kind, resource_id, counter in CAPACITY_CASES:
        with quota_env() as env:
            capacity = _capacity(kind, resource_id)
            _seed_capacity(env, capacity)
            env.control_plane.put_tenant(
                Tenant(tenant_id=TENANT, municipality_name="Presidente Epitacio", created_at=NOW)
            )
            env.control_plane.put_agent(_agent())
            _expire(env)

            result = _reconcile(env.repo, env)

            assert result.released == 0
            assert _stored_capacity(env, capacity).status is ReservationStatus.CONSUMED
            assert _capacity_counter(env, counter) == 1


def test_libera_capacidade_quando_recurso_esta_ausente() -> None:
    for kind, resource_id, counter in CAPACITY_CASES:
        with quota_env() as env:
            capacity = _capacity(kind, resource_id)
            _seed_capacity(env, capacity)
            _expire(env)

            result = _reconcile(env.repo, env)

            assert result.released == 1
            assert _stored_capacity(env, capacity).status is ReservationStatus.RELEASED
            assert _capacity_counter(env, counter) == 0


def test_nao_libera_quando_run_aparece_entre_leitura_e_transacao() -> None:
    with quota_env() as env:
        reservation = _reservation()
        _seed(env, reservation)
        _expire(env)
        client = _Client(env.client)
        client.before_transact = lambda: env.control_plane.put_run(_run(RunState.PROCESSING))
        repo = DynamoQuotaReservations(client, TABLE_NAME, env.clock.now)

        result = _reconcile(repo, env)

        assert result.released == 0
        assert _stored(env, reservation)[0].status is ReservationStatus.RESERVED
        assert _counters(env)["consumed_runs"] == 1


def test_nao_libera_capacidade_quando_recurso_aparece_entre_leitura_e_transacao() -> None:
    with quota_env() as env:
        capacity = _capacity(CapacityKind.AGENT, "agent-01")
        _seed_capacity(env, capacity)
        _expire(env)
        client = _Client(env.client)
        client.before_transact = lambda: env.control_plane.put_agent(_agent())
        repo = DynamoQuotaReservations(client, TABLE_NAME, env.clock.now)

        assert _reconcile(repo, env).released == 0
        assert _capacity_counter(env, "agent_count") == 1


def test_pagina_candidatos_com_cursor() -> None:
    with quota_env() as env:
        reservations = [_reservation(ReservationKind.ANALYTICS, f"query-{i}") for i in range(2)]
        for reservation in reservations:
            _seed(env, reservation)
        _expire(env)

        first = _reconcile(env.repo, env, limit=1)
        assert (first.examined, first.released) == (1, 1)
        assert first.next_cursor is not None
        second = _reconcile(env.repo, env, limit=1, cursor=first.next_cursor)

        assert (second.examined, second.released) == (1, 1)
        statuses = {_stored(env, r)[0].status for r in reservations}
        assert statuses == {ReservationStatus.RELEASED}


def _encode(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode()).decode()


@pytest.mark.parametrize("cursor", ["@@@", _encode("[1]"), _encode('{"pk": 1}'), "bm90LWpzb24"])
def test_rejeita_cursor_invalido(cursor: str) -> None:
    with quota_env() as env:
        with pytest.raises(PermanentBillingError) as error:
            _reconcile(env.repo, env, cursor=cursor)

        assert error.value.code == "invalid_recovery_cursor"


def test_propaga_falha_do_storage_na_descoberta() -> None:
    with quota_env() as env:
        client = _Client(env.client)
        client.query_error = ClientError({"Error": {"Code": "InternalServerError"}}, "Query")
        repo = DynamoQuotaReservations(client, TABLE_NAME, env.clock.now)

        with pytest.raises(BillingDependencyError):
            _reconcile(repo, env)
