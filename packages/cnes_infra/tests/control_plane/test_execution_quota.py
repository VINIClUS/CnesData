"""Mixin DynamoDB da quota mensal sobre moto: um ADD condicional, item antigo e erros do gate."""

from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.control_plane.environment_gate import PermitHolder
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_domain.control_plane.execution_quota import (
    AttemptOutcome,
    AttemptStatus,
    ConsumeExecutionAttempt,
    execution_period,
)
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
from cnes_domain.ports.environment_gate import (
    ExecutionObservation,
    ExecutionPresence,
    UnitAdmission,
)
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_environment_gate import attempts_key
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import _TABLE_NAME, _create_table
from packages.cnes_infra.tests.control_plane.test_environment_gate import ScriptedClient, _raise

NOW = datetime(2026, 10, 13, 12, tzinfo=UTC)
ENV = "prod"
DISPATCH = "0123456789abcdef"
LIMIT = 200
EXHAUSTING_IDS = tuple(f"{index:016x}" for index in range(LIMIT))
_UPDATE = "SET #entity = :entity, #expires = :expires ADD #count :one, #ids :idset"
_CONDITION = (
    "(attribute_not_exists(#count) OR #count < :limit) "
    "AND (attribute_not_exists(#ids) OR NOT contains(#ids, :id))"
)
type _Quota = tuple[DynamoDBControlPlane, ScriptedClient]


def attempts_item(environment: str, now: datetime, dispatch_ids: Sequence[str]) -> dict[str, Any]:
    """Semente do item mensal: contador e dispatches já contados no mês UTC de `now`."""
    return {
        **item_key(*attempts_key(environment, execution_period(now))),
        "attempts": {"N": str(len(dispatch_ids))},
        "dispatch_ids": {"SS": list(dispatch_ids)},
    }


class _AbsentProbe:
    def probe(self, dispatch_id: str) -> ExecutionObservation:
        return ExecutionObservation(ExecutionPresence.ABSENT, None)


@pytest.fixture
def quota() -> Iterator[_Quota]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        scripted = ScriptedClient(client)
        yield DynamoDBControlPlane(scripted, _TABLE_NAME, lambda: NOW), scripted


def _attempt(dispatch_id: str = DISPATCH, *, now: datetime = NOW) -> ConsumeExecutionAttempt:
    return ConsumeExecutionAttempt(ENV, execution_period(now), dispatch_id, now)


def _seed(client: ScriptedClient, dispatch_ids: Sequence[str]) -> None:
    client.client.put_item(TableName=_TABLE_NAME, Item=attempts_item(ENV, NOW, dispatch_ids))


def _stored(client: ScriptedClient, now: datetime = NOW) -> dict[str, Any] | None:
    key = item_key(*attempts_key(ENV, execution_period(now)))
    response = client.client.get_item(TableName=_TABLE_NAME, Key=key, ConsistentRead=True)
    return response.get("Item")


def _ledger(client: ScriptedClient, now: datetime = NOW) -> tuple[int, set[str]]:
    item = _stored(client, now) or {}
    return int(item["attempts"]["N"]), set(item["dispatch_ids"]["SS"])


def test_tentativa_199_para_200_passa(quota: _Quota) -> None:
    adapter, client = quota
    _seed(client, EXHAUSTING_IDS[:199])

    outcome = adapter.consume_execution_attempt(_attempt())

    assert outcome == AttemptOutcome(AttemptStatus.COUNTED, 200, LIMIT)
    assert _ledger(client) == (200, {*EXHAUSTING_IDS[:199], DISPATCH})


def test_tentativa_200_para_201_rejeita(quota: _Quota) -> None:
    adapter, client = quota
    _seed(client, EXHAUSTING_IDS)

    outcome = adapter.consume_execution_attempt(_attempt())

    assert outcome == AttemptOutcome(AttemptStatus.EXHAUSTED, LIMIT, LIMIT)
    assert _ledger(client) == (LIMIT, set(EXHAUSTING_IDS))
    assert client.operations() == ["update_item"]


def test_falha_de_start_conta_como_tentativa(quota: _Quota) -> None:
    adapter, client = quota
    admission = EnvironmentAdmission(adapter, _AbsentProbe(), ENV, 300)
    holder = PermitHolder("354130", "run-01", DISPATCH)
    assert admission.admit(holder, NOW) == UnitAdmission(ENV, 1)

    # StartExecution raised: the caller only settles the permit, nothing refunds the attempt.
    admission.settle(holder, NOW)

    assert _ledger(client) == (1, {DISPATCH})
    assert admission.admit(holder, NOW + timedelta(seconds=1)) == UnitAdmission(ENV, 1)
    assert _ledger(client) == (1, {DISPATCH})


def test_replay_do_mesmo_dispatch_nao_conta_de_novo(quota: _Quota) -> None:
    adapter, client = quota

    first = adapter.consume_execution_attempt(_attempt())
    replay = adapter.consume_execution_attempt(_attempt())

    assert first == AttemptOutcome(AttemptStatus.COUNTED, 1, LIMIT)
    assert replay == AttemptOutcome(AttemptStatus.ALREADY_COUNTED, 1, LIMIT)
    assert _ledger(client) == (1, {DISPATCH})
    assert client.operations() == ["update_item", "update_item"]


def test_replay_com_contador_no_limite_ja_esta_contado(quota: _Quota) -> None:
    adapter, client = quota
    _seed(client, (*EXHAUSTING_IDS[:199], DISPATCH))

    outcome = adapter.consume_execution_attempt(_attempt())

    assert outcome == AttemptOutcome(AttemptStatus.ALREADY_COUNTED, LIMIT, LIMIT)
    assert client.operations() == ["update_item"]


def test_virada_do_mes_conta_de_novo_no_mes_novo(quota: _Quota) -> None:
    # Conservative by design: the dispatch set lives in the month's item, so a replay that
    # crosses the turn of the month counts once more in the new one. It never under-counts.
    adapter, client = quota
    october = datetime(2026, 10, 31, 23, 59, 59, 999999, tzinfo=UTC)
    november = october + timedelta(microseconds=1)

    late = adapter.consume_execution_attempt(_attempt(now=october))
    early = adapter.consume_execution_attempt(_attempt(now=november))

    assert late == early == AttemptOutcome(AttemptStatus.COUNTED, 1, LIMIT)
    assert _ledger(client, october) == _ledger(client, november) == (1, {DISPATCH})


def test_escrita_e_um_unico_update_condicional_que_devolve_o_item_antigo(quota: _Quota) -> None:
    adapter, client = quota

    adapter.consume_execution_attempt(_attempt())

    (request,) = client.requests("update_item")
    assert request["Key"] == item_key("ENV#prod#ATTEMPTS", "MONTH#2026-10")
    assert (request["UpdateExpression"], request["ConditionExpression"]) == (_UPDATE, _CONDITION)
    assert request["ExpressionAttributeNames"] == {
        "#entity": "entity", "#expires": "expires_at", "#count": "attempts",
        "#ids": "dispatch_ids",
    }
    assert {name: request["ExpressionAttributeValues"][name] for name in (":id", ":limit")} == {
        ":id": {"S": DISPATCH}, ":limit": {"N": "200"},
    }
    assert (request["ReturnValues"], request["ReturnValuesOnConditionCheckFailure"]) == (
        "ALL_OLD", "ALL_OLD",
    )
    assert client.operations() == ["update_item"]


def test_item_mensal_guarda_contador_dispatches_e_ttl_de_400_dias(quota: _Quota) -> None:
    adapter, client = quota

    adapter.consume_execution_attempt(_attempt())

    expires_at = datetime(2026, 10, 1, tzinfo=UTC) + timedelta(days=400)
    assert _stored(client) == {
        **item_key("ENV#prod#ATTEMPTS", "MONTH#2026-10"),
        "entity": {"S": "EXECUTIONATTEMPTS"},
        "expires_at": {"N": str(int(expires_at.timestamp()))},
        "attempts": {"N": "1"},
        "dispatch_ids": {"SS": [DISPATCH]},
    }


def test_ttl_vencido_nao_entra_na_contagem(quota: _Quota) -> None:
    adapter, client = quota
    expired = {**attempts_item(ENV, NOW, EXHAUSTING_IDS), "expires_at": {"N": "0"}}
    client.client.put_item(TableName=_TABLE_NAME, Item=expired)

    outcome = adapter.consume_execution_attempt(_attempt())

    assert outcome == AttemptOutcome(AttemptStatus.EXHAUSTED, LIMIT, LIMIT)


def test_conflito_transacional_no_update_e_retentado(quota: _Quota) -> None:
    adapter, client = quota
    client.inject("update_item", _raise("TransactionConflictException"))

    outcome = adapter.consume_execution_attempt(_attempt())

    assert outcome == AttemptOutcome(AttemptStatus.COUNTED, 1, LIMIT)
    assert len(client.requests("update_item")) == 2


def test_conflito_transacional_persistente_esgota_tentativas(quota: _Quota) -> None:
    adapter, client = quota
    client.inject("update_item", *(_raise("TransactionConflictException") for _ in range(3)))

    with pytest.raises(Conflict) as error:
        adapter.consume_execution_attempt(_attempt())

    assert error.value.code is ErrorCode.TRANSACTION_CONFLICT
    assert _stored(client) is None


@pytest.mark.parametrize(
    "code", ["ProvisionedThroughputExceededException", "ConditionalCheckFailedException"],
)
def test_erro_sem_item_antigo_propaga_sem_classificar(quota: _Quota, code: str) -> None:
    adapter, client = quota
    client.inject("update_item", _raise(code))

    with pytest.raises(ClientError, match=code):
        adapter.consume_execution_attempt(_attempt())

    assert client.operations() == ["update_item"]
    assert _stored(client) is None
