"""C3 do start nativo do unit: requests do keeper e reconcile do container no mesmo DynamoDB."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from moto import mock_aws

from cnes_domain.control_plane.environment_gate import PermitHolder
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
from cnes_infra.aws.mode_canary import ModeCanaryContext
from cnes_infra.aws.mode_canary_dispatch import (
    StartPermit,
    UnitStart,
    acquire_request,
    attempt_request,
    bind_request,
    claim_request,
    confirm_requests,
    reserve_request,
    start_execution,
    start_request_sha256,
    unit_dispatch,
)
from cnes_infra.aws.mode_canary_frames import dispatch_effect
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import (
    dispatch_key,
    item_key,
    run_entity_key,
    timestamp,
    unit_key,
)
from cnes_infra.executor.step_functions_probe import StepFunctionsExecutionProbe, execution_arn
from packages.cnes_infra.tests.aws.mode_canary_native import (
    MACHINE,
    NativeCase,
    seed_unit_run,
)
from packages.cnes_infra.tests.aws.test_mode_canary_reentry import (
    _NOW,
    _TENANT,
    _UNIT_RUN,
    _counts,
    _keeper_admit,
    _Recorder,
    _seed,
    _tickets,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    _create_table,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from cnes_domain.orchestration.mode_canary import ModeCanaryBinding

LEASE = 90
LATER = _NOW + timedelta(seconds=60)
ENDPOINT = os.getenv("DYNAMODB_ENDPOINT_URL", "http://127.0.0.1:18000")
FENCE, SEMAPHORE = ("ENV#prod#FENCE", "FENCE"), ("ENV#prod#SEMAPHORE", "UNIT")


def _local() -> Any:
    client = boto3.client(
        "dynamodb", endpoint_url=ENDPOINT, region_name="us-east-1", aws_access_key_id="test",
        aws_secret_access_key="test",  # noqa: S106
        config=Config(retries={"max_attempts": 1}, connect_timeout=2, read_timeout=10))
    try:
        client.list_tables(Limit=1)
    except (BotoCoreError, ClientError, OSError):
        pytest.skip("reason=dynamodb_local_unreachable")
    return client


def _renamed(value: Any, table: str) -> Any:
    if isinstance(value, dict):
        return {key: table if key == "TableName" and item == _TABLE_NAME else _renamed(item, table)
                for key, item in cast("dict[str, Any]", value).items()}
    if isinstance(value, list):
        return [_renamed(item, table) for item in cast("list[Any]", value)]
    return value


class _Renamed:
    def __init__(self, client: Any, table: str) -> None:
        self._client, self._table = client, table

    def __getattr__(self, name: str) -> Any:
        method = getattr(self._client, name)
        return lambda **request: method(**_renamed(request, self._table))


@pytest.fixture(params=["moto", pytest.param("dynamodb_local", marks=pytest.mark.dynamodb_local)])
def client(request: pytest.FixtureRequest) -> Iterator[Any]:
    if request.param == "moto":
        with mock_aws():
            dynamo = boto3.client("dynamodb", region_name="us-east-1")
            _create_table(dynamo)
            yield dynamo
        return
    dynamo = _Renamed(_local(), f"c3-unit-start-{uuid4().hex[:12]}")
    _create_table(dynamo)
    yield dynamo
    dynamo.delete_table(TableName=_TABLE_NAME)


class _StepFunctions:
    def __init__(self) -> None:
        self.inputs: dict[str, str] = {}
        self.status = "RUNNING"

    def start_execution(self, **request: str) -> dict[str, str]:
        self.inputs[request["name"]] = request["input"]
        return {"executionArn": execution_arn(request["stateMachineArn"], request["name"])}

    def describe_execution(self, **request: str) -> dict[str, str]:
        name = request["executionArn"].rpartition(":")[2]
        if name not in self.inputs:
            raise ClientError({"Error": {"Code": "ExecutionDoesNotExist"}}, "DescribeExecution")
        return {"executionArn": request["executionArn"], "status": self.status,
                "input": self.inputs[name]}


@dataclass(frozen=True)
class _Keeper:
    binding: ModeCanaryBinding
    start: UnitStart
    permit: StartPermit
    effect: str
    run_item: dict[str, Any]


def _item(client: Any, key: tuple[str, str]) -> dict[str, Any] | None:
    return client.get_item(TableName=_TABLE_NAME, Key=item_key(*key),
                           ConsistentRead=True).get("Item")


def _seed_run(client: Any, promotion_id: str) -> list[dict[str, Any]]:
    seed_unit_run(client, _TABLE_NAME, NativeCase(_TENANT, _UNIT_RUN, _NOW, LEASE, promotion_id))
    plane = DynamoDBControlPlane(client, _TABLE_NAME, lambda: _NOW)
    return [json.loads(unit.model_dump_json())
            for unit in plane.list_run_units(_TENANT, _UNIT_RUN)]


def _admitted(client: Any, mutate: Callable[[dict[str, Any]], dict[str, Any]]) -> _Keeper:
    binding, record = _seed(client, "unit")
    promotion_id = cast("str", record.promotion_id)
    dispatch = unit_dispatch(binding, _seed_run(client, promotion_id), _NOW)
    start = UnitStart(dispatch, _TABLE_NAME, _NOW, LEASE, promotion_id)
    keys = [run_entity_key(_TENANT, _UNIT_RUN),
            *(unit_key(_TENANT, _UNIT_RUN, unit_id) for unit_id in dispatch.unit_ids)]
    run_item, *unit_items = [cast("dict[str, Any]", _item(client, key)) for key in keys]
    client.transact_write_items(**reserve_request(start, run_item, unit_items))
    holder = PermitHolder(dispatch.tenant_id, dispatch.run_id, dispatch.dispatch_id)
    effect = _keeper_admit(client, binding, record,
                           dispatch_effect(binding, record, holder, dispatch.unit_ids))
    client.transact_write_items(**mutate(acquire_request(start)))
    client.update_item(**attempt_request(start))
    token = int(cast("dict[str, Any]", _item(client, SEMAPHORE))["token"]["N"])
    fence = _item(client, FENCE)
    permit = StartPermit(token, 0 if fence is None else int(fence["version"]["N"]))
    return _Keeper(binding, start, permit, effect, run_item)


def _started(client: Any, keeper: _Keeper, sfn: _StepFunctions, max_concurrency: int) -> str:
    start, permit = keeper.start, keeper.permit
    client.transact_write_items(**claim_request(start, permit))
    sent = replace(start.dispatch, max_concurrency=max_concurrency)
    execution = sfn.start_execution(stateMachineArn=MACHINE, **start_execution(sent))
    ref = execution["executionArn"]
    client.transact_write_items(**bind_request(start, permit, ref, keeper.run_item))
    for request in confirm_requests(start, permit, ref):
        client.update_item(**request)
    return ref


def _reconcile(client: Any, keeper: _Keeper, sfn: _StepFunctions) -> list[str]:
    recorder = _Recorder(client)
    plane = DynamoDBControlPlane(client, _TABLE_NAME, lambda: LATER)
    context = ModeCanaryContext(recorder, _TABLE_NAME, keeper.binding, plane)
    probe = StepFunctionsExecutionProbe(cast("Any", sfn), MACHINE)
    EnvironmentAdmission(plane, probe, "prod", LEASE, plane, mode_canary=context).reconcile(LATER)
    return recorder.writes


def _uncontended(client: Any, keeper: _Keeper, writes: list[str],
                 caplog: pytest.LogCaptureFixture) -> None:
    assert writes == []
    assert "synthetic_permit_untouched" not in caplog.text
    assert [ticket["effect_id"] for ticket in _tickets(client)] == [keeper.effect]
    assert _counts(client)["unit_tasks"] == len(keeper.start.dispatch.unit_ids) == 2


def _semaphore(client: Any) -> dict[str, Any]:
    return cast("dict[str, Any]", _item(client, SEMAPHORE))


def _dispatch(client: Any) -> dict[str, Any]:
    item = cast("dict[str, Any]", _item(client, dispatch_key(_TENANT, _UNIT_RUN)))
    return json.loads(item["payload"]["S"])


def _honest(request: dict[str, Any]) -> dict[str, Any]:
    return request


def _second_generation(request: dict[str, Any]) -> dict[str, Any]:
    request["TransactItems"][1]["Update"]["ExpressionAttributeValues"][":generation"] = {"N": "2"}
    return request


def _bare_holder(request: dict[str, Any]) -> dict[str, Any]:
    update = request["TransactItems"][1]["Update"]
    update["UpdateExpression"] = update["UpdateExpression"].replace(
        ", #generation = :generation, #request = :request, #start = :reserved", "")
    for name in ("#generation", "#request", "#start"):
        del update["ExpressionAttributeNames"][name]
    for value in (":generation", ":request", ":reserved"):
        del update["ExpressionAttributeValues"][value]
    return request


def test_permit_do_keeper_sem_execucao_o_container_cancela_antes_do_envio(
        client: Any, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    keeper = _admitted(client, _honest)
    writes = _reconcile(client, keeper, _StepFunctions())
    _uncontended(client, keeper, writes, caplog)
    semaphore = _semaphore(client)
    assert "holder_dispatch_id" not in semaphore
    settlement = json.loads(semaphore["settlement"]["S"])
    assert settlement["resolution"] == "CANCEL_BEFORE_SEND"
    assert settlement["request_sha256"] == start_request_sha256(keeper.start.dispatch)
    dispatch = _dispatch(client)
    assert (dispatch["state"], dispatch["terminal_outcome"]) == ("TERMINAL", "CANCELED")
    assert dispatch["canceled_before_send"] is True


@pytest.mark.parametrize("mutate", [_second_generation, _bare_holder])
def test_permit_sem_a_geracao_ou_o_digest_do_dispatch_o_container_nao_cancela(
        client: Any, caplog: pytest.LogCaptureFixture,
        mutate: Callable[[dict[str, Any]], dict[str, Any]]) -> None:
    caplog.set_level(logging.INFO)
    keeper = _admitted(client, mutate)
    before = _semaphore(client)
    writes = _reconcile(client, keeper, _StepFunctions())
    _uncontended(client, keeper, writes, caplog)
    assert _semaphore(client) == before
    assert before["holder_dispatch_id"] == {"S": keeper.start.dispatch.dispatch_id}
    assert _dispatch(client)["state"] == "RESERVED"


def test_start_do_keeper_com_execucao_viva_o_container_renova(
        client: Any, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    keeper = _admitted(client, _honest)
    sfn = _StepFunctions()
    execution = _started(client, keeper, sfn, keeper.start.dispatch.max_concurrency)
    before = _semaphore(client)
    writes = _reconcile(client, keeper, sfn)
    _uncontended(client, keeper, writes, caplog)
    after = _semaphore(client)
    assert before["lease_until"] == {"S": timestamp(_NOW + timedelta(seconds=LEASE))}
    assert after["lease_until"] == {"S": timestamp(LATER + timedelta(seconds=LEASE))}
    assert (after["execution_ref"], after["start_state"]) == ({"S": execution}, {"S": "STARTED"})
    assert _dispatch(client)["state"] == "STARTED"


def test_start_do_keeper_com_execucao_terminal_o_container_libera(
        client: Any, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    keeper = _admitted(client, _honest)
    sfn = _StepFunctions()
    execution = _started(client, keeper, sfn, keeper.start.dispatch.max_concurrency)
    sfn.status = "SUCCEEDED"
    writes = _reconcile(client, keeper, sfn)
    _uncontended(client, keeper, writes, caplog)
    semaphore = _semaphore(client)
    assert "holder_dispatch_id" not in semaphore
    settlement = json.loads(semaphore["settlement"]["S"])
    assert (settlement["resolution"], settlement["execution_ref"]) == ("TERMINAL", execution)
    dispatch = _dispatch(client)
    assert (dispatch["state"], dispatch["terminal_outcome"]) == ("TERMINAL", "SUCCEEDED")


@pytest.mark.parametrize("status", ["RUNNING", "SUCCEEDED"])
def test_execucao_com_input_fora_do_digest_do_permit_o_container_nao_toca(
        client: Any, caplog: pytest.LogCaptureFixture, status: str) -> None:
    caplog.set_level(logging.INFO)
    keeper = _admitted(client, _honest)
    sfn = _StepFunctions()
    _started(client, keeper, sfn, keeper.start.dispatch.max_concurrency + 1)
    sfn.status = status
    before = _semaphore(client)
    writes = _reconcile(client, keeper, sfn)
    _uncontended(client, keeper, writes, caplog)
    assert _semaphore(client) == before
    assert _dispatch(client)["state"] == "STARTED"
