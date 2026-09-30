"""Testes da liquidação de reservas de quota (consume/release) sobre moto."""

from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest
from botocore.exceptions import ClientError

from cnes_domain.billing.commands import ConsumeReservationCommand, ReleaseReservationCommand
from cnes_domain.billing.errors import (
    BillingDependencyError,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.models import QuotaReservation, ReservationKind, ReservationStatus
from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations
from cnes_infra.billing.dynamodb_quota_items import decode_reservation, encode_reservation
from cnes_infra.billing.dynamodb_quota_settlement import ReservationTransition
from cnes_infra.billing.keys import reservation_key, usage_key
from cnes_infra.control_plane.dynamodb_codec import absent_check_action
from packages.cnes_infra.tests.billing.billing_factories import TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    NOW,
    RESERVATION_TTL,
    TENANT,
    QuotaEnv,
    quota_env,
    table_items,
)

RID = "res-01"
RESERVED_BYTES = 1_000


def _reservation(**changes: Any) -> QuotaReservation:
    reservation = QuotaReservation(
        reservation_id=RID,
        billing_account_id=ACCOUNT,
        resource_id="run-01",
        kind=ReservationKind.RUN,
        period_start=NOW.replace(day=1, hour=0, minute=0),
        reserved_runs=1,
        reserved_scan_bytes=RESERVED_BYTES,
        consumed_runs=1,
        consumed_scan_bytes=0,
        status=ReservationStatus.RESERVED,
        created_at=NOW,
        expires_at=NOW + RESERVATION_TTL,
    )
    return replace(reservation, **changes)


def _number(value: int) -> dict[str, str]:
    return {"N": str(value)}


def _seed(env: QuotaEnv, reservation: QuotaReservation | None = None) -> QuotaReservation:
    reservation = reservation or _reservation()
    prefix = reservation.kind.value
    usage = {
        "consumed_runs": _number(1),
        f"{prefix}_reserved_scan_bytes": _number(reservation.reserved_scan_bytes),
        f"{prefix}_consumed_scan_bytes": _number(0),
        f"{prefix}_committed_scan_bytes": _number(reservation.reserved_scan_bytes),
    }
    pk, sk = usage_key(ACCOUNT, reservation.period_start)
    usage_item = {"pk": {"S": pk}, "sk": {"S": sk}, "entity": {"S": "BILLINGUSAGE"}, **usage}
    env.client.put_item(TableName=TABLE_NAME, Item=usage_item)
    env.client.put_item(TableName=TABLE_NAME, Item=encode_reservation(reservation, TENANT))
    return reservation


def _usage(env: QuotaEnv, reservation: QuotaReservation | None = None) -> dict[str, int]:
    period = (reservation or _reservation()).period_start
    pk, sk = usage_key(ACCOUNT, period)
    item = env.client.get_item(
        TableName=TABLE_NAME, Key={"pk": {"S": pk}, "sk": {"S": sk}}, ConsistentRead=True
    )["Item"]
    return {k: int(v["N"]) for k, v in item.items() if "N" in v}


def _stored(env: QuotaEnv, reservation: QuotaReservation | None = None) -> dict[str, Any]:
    reservation = reservation or _reservation()
    pk, sk = reservation_key(ACCOUNT, reservation.period_start, RID)
    return env.client.get_item(
        TableName=TABLE_NAME, Key={"pk": {"S": pk}, "sk": {"S": sk}}, ConsistentRead=True
    )["Item"]


def _consume(actual: int = 400, account: str = ACCOUNT, rid: str = RID) -> Any:
    return ConsumeReservationCommand(account, rid, actual, NOW)


def _release(reason: str = "run_failed") -> ReleaseReservationCommand:
    return ReleaseReservationCommand(ACCOUNT, RID, NOW, reason)


class _ClientStub:
    def __init__(self, client: Any, failures: int = 0, query_error: bool = False) -> None:
        self._client = client
        self.failures = failures
        self.query_error = query_error
        self.ghost_key: tuple[str, str] | None = None
        self.transactions = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    def query(self, **kwargs: Any) -> Any:
        if self.query_error:
            raise ClientError({"Error": {"Code": "InternalServerError"}}, "Query")
        if self.ghost_key is not None:
            pk, sk = self.ghost_key
            return {"Items": [{"pk": {"S": pk}, "sk": {"S": sk}}]}
        return self._client.query(**kwargs)

    def transact_write_items(self, **kwargs: Any) -> Any:
        self.transactions += 1
        if self.failures > 0:
            self.failures -= 1
            raise ClientError(
                {
                    "Error": {"Code": "TransactionCanceledException"},
                    "CancellationReasons": [{"Code": "ConditionalCheckFailed"}],
                },
                "TransactWriteItems",
            )
        return self._client.transact_write_items(**kwargs)


def _stub_repo(env: QuotaEnv, **options: Any) -> DynamoQuotaReservations:
    stub = _ClientStub(env.client, **options)
    return DynamoQuotaReservations(stub, TABLE_NAME, env.clock.now)


def test_consome_reserva_ajustando_contadores_sem_incrementar_runs() -> None:
    with quota_env() as env:
        _seed(env)
        result = env.repo.consume(_consume(400))
        usage = _usage(env)
        assert result.status is ReservationStatus.CONSUMED
        assert (result.reserved_scan_bytes, result.consumed_scan_bytes) == (0, 400)
        assert usage["run_reserved_scan_bytes"] == 0
        assert usage["run_consumed_scan_bytes"] == 400
        assert usage["run_committed_scan_bytes"] == 400
        assert usage["consumed_runs"] == 1
        assert decode_reservation(_stored(env))[0] == result


def test_consumo_acima_do_reservado_ajusta_committed() -> None:
    with quota_env() as env:
        _seed(env)
        env.repo.consume(_consume(1_500))
        usage = _usage(env)
        assert usage["run_committed_scan_bytes"] == 1_500
        assert usage["run_consumed_scan_bytes"] == 1_500


def test_consumo_parcial_libera_o_restante_via_committed() -> None:
    with quota_env() as env:
        _seed(env)
        env.repo.consume(_consume(0))
        assert _usage(env)["run_committed_scan_bytes"] == 0


def test_libera_reserva_sem_decrementar_runs_consumidos() -> None:
    with quota_env() as env:
        _seed(env)
        result = env.repo.release(_release())
        usage = _usage(env)
        assert result.status is ReservationStatus.RELEASED
        assert result.reserved_scan_bytes == 0
        assert usage["run_reserved_scan_bytes"] == 0
        assert usage["run_committed_scan_bytes"] == 0
        assert usage["consumed_runs"] == 1
        assert decode_reservation(_stored(env))[0].consumed_runs == 1


def test_consumo_repetido_e_idempotente() -> None:
    with quota_env() as env:
        _seed(env)
        first = env.repo.consume(_consume(400))
        usage = _usage(env)
        assert env.repo.consume(_consume(900)) == first
        assert _usage(env) == usage


def test_liberacao_repetida_e_idempotente() -> None:
    with quota_env() as env:
        _seed(env)
        first = env.repo.release(_release())
        usage = _usage(env)
        assert env.repo.release(_release("outro_motivo")) == first
        assert _usage(env) == usage


def test_liberar_apos_consumo_nao_altera_nada() -> None:
    with quota_env() as env:
        _seed(env)
        consumed = env.repo.consume(_consume(400))
        usage = _usage(env)
        assert env.repo.release(_release()) == consumed
        assert _usage(env) == usage


def test_consumir_apos_liberacao_e_rejeitado() -> None:
    with quota_env() as env:
        _seed(env)
        env.repo.release(_release())
        with pytest.raises(PermanentBillingError) as error:
            env.repo.consume(_consume(400))
        assert error.value.code == "quota_reservation_released"


def test_liquidacao_analitica_usa_contadores_analytics() -> None:
    reservation = _reservation(kind=ReservationKind.ANALYTICS, reserved_runs=0, consumed_runs=0)
    with quota_env() as env:
        _seed(env, reservation)
        env.repo.consume(_consume(300))
        usage = _usage(env, reservation)
        assert usage["analytics_reserved_scan_bytes"] == 0
        assert usage["analytics_consumed_scan_bytes"] == 300
        assert usage["analytics_committed_scan_bytes"] == 300
        assert "run_reserved_scan_bytes" not in usage


def test_reserva_expirada_ainda_presente_e_liquidada() -> None:
    with quota_env() as env:
        _seed(env)
        env.clock.advance(RESERVATION_TTL + timedelta(hours=1))
        result = env.repo.consume(_consume(400))
        assert result.status is ReservationStatus.CONSUMED
        assert _usage(env)["run_committed_scan_bytes"] == 400


def test_indice_de_vencimento_removido_e_localizador_mantido() -> None:
    with quota_env() as env:
        _seed(env)
        assert "gsi1pk" in _stored(env)
        env.repo.release(_release())
        stored = _stored(env)
        assert "gsi1pk" not in stored
        assert "gsi2pk" in stored
        assert "expires_at" not in stored


def test_emite_eventos_de_outbox_na_liquidacao() -> None:
    with quota_env() as env:
        _seed(env)
        env.repo.consume(_consume(400))
        events = [i for i in table_items(env.client) if i["entity"]["S"] == "OUTBOXEVENT"]
        assert len(events) == 1
        assert '"quota.consumed"' in events[0]["payload"]["S"]
        assert '"actual_scan_bytes":400' in events[0]["payload"]["S"].replace(" ", "")


def test_evento_de_liberacao_inclui_codigo_do_motivo() -> None:
    with quota_env() as env:
        _seed(env)
        env.repo.release(_release("run_failed"))
        events = [i for i in table_items(env.client) if i["entity"]["S"] == "OUTBOXEVENT"]
        assert '"quota.released"' in events[0]["payload"]["S"]
        assert "run_failed" in events[0]["payload"]["S"]


def test_reserva_desconhecida_e_nao_encontrada() -> None:
    with quota_env() as env:
        with pytest.raises(RetryableBillingError) as error:
            env.repo.consume(_consume(rid="res-inexistente"))
        assert error.value.code == "quota_reservation_not_found"


def test_localizador_sem_item_base_e_nao_encontrado() -> None:
    with quota_env() as env:
        stub = _ClientStub(env.client)
        stub.ghost_key = ("BILLING#ghost", "RESERVATION#ghost")
        repo = DynamoQuotaReservations(stub, TABLE_NAME, env.clock.now)
        with pytest.raises(RetryableBillingError) as error:
            repo.release(_release())
        assert error.value.code == "quota_reservation_not_found"


def test_item_base_de_outra_conta_e_nao_encontrado() -> None:
    with quota_env() as env:
        other = _reservation(billing_account_id="ba_02", reservation_id="res-99")
        item = encode_reservation(other, TENANT)
        item["gsi2pk"] = encode_reservation(_reservation(), TENANT)["gsi2pk"]
        env.client.put_item(TableName=TABLE_NAME, Item=item)
        with pytest.raises(RetryableBillingError) as error:
            env.repo.consume(_consume())
        assert error.value.code == "quota_reservation_not_found"


def test_falha_na_consulta_do_localizador_vira_dependencia_indisponivel() -> None:
    with quota_env() as env:
        repo = _stub_repo(env, query_error=True)
        with pytest.raises(BillingDependencyError):
            repo.consume(_consume())


def test_contencao_transitoria_e_repetida_ate_sucesso() -> None:
    with quota_env() as env:
        _seed(env)
        repo = _stub_repo(env, failures=2)
        assert repo.consume(_consume(400)).status is ReservationStatus.CONSUMED


def test_contencao_persistente_esgota_tentativas() -> None:
    with quota_env() as env:
        _seed(env)
        repo = _stub_repo(env, failures=3)
        with pytest.raises(RetryableBillingError) as error:
            repo.consume(_consume(400))
        assert error.value.code == "quota_reservation_contended"
        assert _stored(env)["status"]["S"] == "reserved"


def _transition_item(env: QuotaEnv) -> dict[str, Any]:
    _seed(env)
    return _stored(env)


def test_transicao_com_devolucao_do_run_e_guarda_satisfeita() -> None:
    with quota_env() as env:
        item = _transition_item(env)
        guard = absent_check_action(TABLE_NAME, ("RUN#ausente", "META"))
        change = ReservationTransition(
            ReservationStatus.RELEASED, NOW, release_run=True, guards=(guard,)
        )
        assert env.repo._transition_reservation(item, change) is True
        stored, _ = decode_reservation(_stored(env))
        assert stored.consumed_runs == 0
        assert stored.status is ReservationStatus.RELEASED
        assert _usage(env)["consumed_runs"] == 0


def test_transicao_com_guarda_violada_nao_altera_nada() -> None:
    with quota_env() as env:
        item = _transition_item(env)
        env.client.put_item(
            TableName=TABLE_NAME, Item={"pk": {"S": "RUN#presente"}, "sk": {"S": "META"}}
        )
        guard = absent_check_action(TABLE_NAME, ("RUN#presente", "META"))
        change = ReservationTransition(
            ReservationStatus.RELEASED, NOW, release_run=True, guards=(guard,)
        )
        before = _usage(env)
        assert env.repo._transition_reservation(item, change) is False
        assert _usage(env) == before
        assert _stored(env) == item
