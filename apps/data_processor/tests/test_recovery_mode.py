"""Recuperação composta com disputa persistida e falhas reais nas fronteiras SDK."""

import json
import logging
import socket
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from threading import Barrier, Event, Lock
from typing import Any
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from apps.data_processor.tests.test_aws_mode_composition import (
    _NOW,
    _invoke_recovery,
    _offline_aws,
    _OfflineAws,
    _publication_snapshot,
    _seed_publication,
    _values,
)
from apps.data_processor.tests.test_aws_unit_composition import (
    _RUN,
    _TENANT,
    _graph,
    _raw_input,
    _seed_graph,
)
from cnes_domain.control_plane.commands import PutRunUnits, ReserveRunDispatch
from cnes_domain.control_plane.enums import DispatchState, RunState, RunUnitState
from cnes_domain.control_plane.environment_gate import (
    AcquireUnitPermit,
    BindUnitPermit,
    PermitHolder,
)
from cnes_domain.orchestration.planner import logical_wave_id
from cnes_domain.ports.processing import StartRunExecution
from cnes_infra.aws.processor_settings import AwsRecoverySettings
from cnes_infra.billing.settings import BillingSettings
from cnes_infra.control_plane.dynamodb_keys import item_key, run_entity_key
from cnes_infra.executor.step_functions import ProcessorExecutionUnavailable, StepFunctionsExecutor
from data_processor.aws_composition import AwsExecutionPolicy, build_aws_recovery_runtime
from data_processor.orchestration.coordinator import RecoveryFailed, noop_execution_started

__all__ = ["_offline_aws"]
_PREFIX = "arn:aws:states:us-east-2:000000000000:execution:cnesdata-prod:"


@pytest.fixture(autouse=True)
def _deny_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def denied(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("network=forbidden")

    monkeypatch.setattr(socket.socket, "connect", denied)
    monkeypatch.setattr(socket, "create_connection", denied)


def _error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "offline"}}, operation)


class _Executions:
    def __init__(self, aws: _OfflineAws) -> None:
        self.lock = Lock()
        self.calls: list[dict[str, Any]] = []
        self.stored: dict[str, dict[str, Any]] = {}
        aws.step_functions.start_execution.side_effect = self.start
        aws.step_functions.describe_execution.side_effect = self.describe

    def start(self, **request: Any) -> dict[str, Any]:
        with self.lock:
            self.calls.append(deepcopy(request))
            name = request["name"]
            if name in self.stored:
                existing = self.stored[name]
                if existing["status"] != "RUNNING" or existing["input"] != request["input"]:
                    raise _error("ExecutionAlreadyExists", "StartExecution")
                return {"executionArn": self.stored[name]["executionArn"]}
            execution = {"executionArn": _PREFIX + name, "status": "RUNNING",
                         "input": request["input"]}
            self.stored[name] = execution
            return {"executionArn": execution["executionArn"]}

    def describe(self, **request: Any) -> dict[str, Any]:
        name = request["executionArn"].rsplit(":", 1)[1]
        with self.lock:
            if name not in self.stored:
                raise _error("ExecutionDoesNotExist", "DescribeExecution")
            return dict(self.stored[name])


class _Schedule:
    def __init__(self, winner: int) -> None:
        self.winner = winner
        self.barrier = Barrier(2, timeout=10)
        self.finished = Event()
        self.lock = Lock()
        self.arrived: list[int] = []
        self.claims: list[dict[str, Any]] = []
        self.submitted: list[int] = []

    def arrive(self, identity: int, request: dict[str, Any]) -> None:
        self.arrived.append(identity)
        self.claims.append(deepcopy(request))
        self.barrier.wait()
        if identity != self.winner:
            assert self.finished.wait(10), "winner=timeout"

    def close(self) -> None:
        self.finished.set()
        self.barrier.abort()


class _Client:
    def __init__(self, aws: _OfflineAws, schedule: _Schedule, identity: int) -> None:
        self.aws, self.schedule, self.identity = aws, schedule, identity
        self.visible: str | None = None
        self.pages: list[dict[str, Any]] = []
        self.claimed = False

    def __getattr__(self, method: str) -> Any:
        def invoke(**request: Any) -> Any:
            with self.schedule.lock:
                return getattr(self.aws.dynamodb, method)(**request)

        return invoke

    def query(self, **request: Any) -> dict[str, Any]:
        with self.schedule.lock:
            response = self.aws.dynamodb.query(**request)
        if self.visible is None or request.get("IndexName") != "gsi4":
            return response
        assert not request.get("ConsistentRead", False)
        assert not response.get("LastEvaluatedKey")
        selected = item_key(*run_entity_key(_TENANT, self.visible))
        items = [item for item in response["Items"] if item["sk"] == selected["sk"]]
        view = response | {"Items": items, "Count": len(items), "ScannedCount": len(items)}
        self.pages.append(deepcopy({"request": request, "raw": response, "view": view}))
        return view

    def transact_write_items(self, **request: Any) -> dict[str, Any]:
        claims = [action["Update"] for action in request["TransactItems"]
                  if "Update" in action and action["Update"].get("ExpressionAttributeValues", {})
                  .get(":entity") == {"S": "UNITSEMAPHORE"}]
        if claims and not self.claimed:
            self.claimed = True
            self.schedule.arrive(self.identity, request)
        with self.schedule.lock:
            if claims:
                self.schedule.submitted.append(self.identity)
            return self.aws.dynamodb.transact_write_items(**request)


def _runtime(aws: _OfflineAws, client: Any = None) -> Any:
    session = Mock(spec_set=["client"])
    session.client.side_effect = lambda service, **kwargs: (
        client if service == "dynamodb" and client is not None
        else aws.session.client(service, **kwargs)
    )
    policy = AwsExecutionPolicy(BillingSettings.from_mapping(_values()), noop_execution_started)
    return build_aws_recovery_runtime(
        AwsRecoverySettings.from_mapping(_values()), session, lambda: _NOW, policy,
    )


def _other_graph(aws: _OfflineAws, run_id: str) -> None:
    original = aws.control_plane.get_run(_TENANT, _RUN)
    assert original is not None
    aws.control_plane.put_run(original.model_copy(update={"run_id": run_id}))
    units = tuple(unit.model_copy(update={"run_id": run_id}) for unit in _graph(_raw_input(aws)))
    aws.control_plane.put_run_units(PutRunUnits(
        tenant_id=_TENANT, run_id=run_id, expected_run_state=RunState.PROCESSING, units=units,
    ))


def _race(runtimes: list[Any], schedule: _Schedule) -> list[Any]:
    def recover(identity: int) -> Any:
        try:
            return runtimes[identity].recovery.run_once(100)
        finally:
            if identity == schedule.winner:
                schedule.finished.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(recover, identity) for identity in range(2)]
        try:
            return [future.result(timeout=20) for future in futures]
        finally:
            schedule.close()


def _snapshot(aws: _OfflineAws, run_id: str = _RUN) -> tuple[Any, Any, Any]:
    dispatch = aws.control_plane.get_active_run_dispatch(_TENANT, run_id)
    assert dispatch is not None
    units = aws.control_plane.list_run_units(_TENANT, run_id)
    permit = aws.control_plane.observe_environment_gate("prod").permit
    return dispatch, units, permit


def _assert_active(aws: _OfflineAws, executions: _Executions, run_id: str = _RUN) -> None:
    dispatch, units, permit = _snapshot(aws, run_id)
    assert (dispatch.state, dispatch.generation, dispatch.unit_ids) == (
        DispatchState.STARTED, 1, ("unit-n",),
    )
    assert permit is not None
    assert permit.holder == PermitHolder(_TENANT, run_id, dispatch.dispatch_id)
    assert (permit.execution_ref, permit.token) == (dispatch.execution_ref, 1)
    assert dispatch.execution_ref == executions.stored[dispatch.dispatch_id]["executionArn"]
    assert len(units) == 3
    assert all(unit.state is RunUnitState.PENDING and unit.attempt == 0 for unit in units)
    assert {unit.dispatch_id for unit in units} == {None}
    assert sum(len(json.loads(value["input"])["unit_ids"])
               for value in executions.stored.values()) == 1


@pytest.mark.parametrize("winner", [0, 1])
def test_recuperacoes_sobrepostas_disputam_um_permit_persistido(
    offline_aws: _OfflineAws, caplog: pytest.LogCaptureFixture, winner: int, record_property: Any,
) -> None:
    _seed_graph(offline_aws)
    _other_graph(offline_aws, "rival")
    executions, schedule = _Executions(offline_aws), _Schedule(winner)
    clients = [_Client(offline_aws, schedule, identity) for identity in range(2)]
    runs = [_RUN, "rival"]
    for client, run_id in zip(clients, runs, strict=True):
        client.visible = run_id
    runtimes = [_runtime(offline_aws, client) for client in clients]
    assert runtimes[0].recovery is not runtimes[1].recovery
    caplog.set_level(logging.INFO)

    results = _race(runtimes, schedule)

    assert sorted(schedule.arrived) == [0, 1]
    assert len(schedule.claims) == 2
    assert schedule.submitted[0] == winner
    assert [(result.scanned, result.recovered) for result in results] == [(1, 1), (1, 1)]
    assert len(executions.calls) == len(executions.stored) == 1
    assert json.loads(executions.calls[0]["input"])["run_id"] == runs[winner]
    _assert_active(offline_aws, executions, runs[winner])
    loser = runs[1 - winner]
    deferred = [record.getMessage() for record in caplog.records
                if record.getMessage().startswith("dispatch_deferred ")]
    assert any(f"run_id={loser} " in line and "reason=permit_held" in line for line in deferred)
    dispatch, units, _ = _snapshot(offline_aws, loser)
    assert (dispatch.state, dispatch.execution_ref, dispatch.generation) == (
        DispatchState.RESERVED, None, 1,
    )
    assert all(unit.dispatch_id is None and unit.attempt == 0 for unit in units)
    for client in clients:
        assert client.pages
        assert all(len(page["raw"]["Items"]) == 2 for page in client.pages)
        assert all(len(page["view"]["Items"]) == 1 for page in client.pages)
    record_property("race", json.dumps({"arrived": schedule.arrived, "claims": schedule.claims,
                                       "pages": [client.pages for client in clients],
                                       "wire": executions.calls, "stored": executions.stored}))


@pytest.mark.parametrize("winner", [0, 1])
def test_mesmo_dispatch_reservado_concorrente_reutiliza_cas_sem_nova_task(
    offline_aws: _OfflineAws, record_property: Any, winner: int,
) -> None:
    _seed_graph(offline_aws)
    ready = tuple(unit for unit in offline_aws.control_plane.list_run_units(_TENANT, _RUN)
                  if unit.unit_id == "unit-n")
    reserved = offline_aws.control_plane.reserve_run_dispatch(ReserveRunDispatch(
        tenant_id=_TENANT, run_id=_RUN, wave_id=logical_wave_id(ready), unit_ids=("unit-n",),
        now=_NOW, lease_seconds=3600,
    ))
    executions, schedule = _Executions(offline_aws), _Schedule(winner)
    runtimes = [_runtime(offline_aws, _Client(offline_aws, schedule, identity))
                for identity in range(2)]

    results = _race(runtimes, schedule)

    assert sorted(schedule.arrived) == [0, 1]
    assert schedule.submitted[0] == winner
    assert [result.recovered for result in results] == [1, 1]
    assert len(executions.calls) == 2
    assert executions.calls[0] == executions.calls[1]
    assert len(executions.stored) == 1
    _assert_active(offline_aws, executions)
    before = _snapshot(offline_aws)
    assert (before[0].dispatch_id, before[0].generation) == (reserved.dispatch_id, 1)
    assert _runtime(offline_aws).recovery.run_once(100).recovered == 1
    assert _snapshot(offline_aws) == before
    assert len(executions.calls) == 2
    record_property("wire_and_creation", json.dumps({
        "wire": executions.calls, "stored": executions.stored, "snapshot": repr(before),
    }))


@pytest.mark.asyncio
async def test_permit_livre_inicia_uma_task_e_ocupado_adia_com_exit_zero(
    offline_aws: _OfflineAws,
) -> None:
    _seed_graph(offline_aws)
    executions = _Executions(offline_aws)
    rc, events = await _invoke_recovery(offline_aws, _values())
    assert rc == 0, events
    _assert_active(offline_aws, executions)
    before = _snapshot(offline_aws)
    _other_graph(offline_aws, "rival")

    rc, events = await _invoke_recovery(offline_aws, _values())

    assert rc == 0, events
    assert _snapshot(offline_aws) == before
    assert len(executions.calls) == len(executions.stored) == 1
    assert any(event["event"].startswith("dispatch_deferred ")
               and "run_id=rival " in event["event"] for event in events)
    summary = next(event for event in events if event["event"] == "processor_recovery_completed")
    assert (summary["scanned"], summary["recovered"], summary["failed"]) == (2, 2, 0)
    assert summary["reconcile_failed"] is False


def _orphan(aws: _OfflineAws) -> None:
    holder = PermitHolder(_TENANT, "orphan", "1111111111111111")
    acquired = aws.control_plane.acquire_unit_permit(AcquireUnitPermit("prod", holder, _NOW, 3600))
    assert acquired.permit is not None
    assert aws.control_plane.bind_unit_permit(BindUnitPermit(
        "prod", acquired.permit.token, _PREFIX + holder.dispatch_id, _NOW,
    ))


def _fail_run_write(aws: _OfflineAws, monkeypatch: pytest.MonkeyPatch) -> ClientError:
    original = aws.dynamodb.transact_write_items
    error = _error("AccessDenied", "TransactWriteItems")
    key = item_key(*run_entity_key(_TENANT, "aaa-failure"))

    def transact_write_items(**request: Any) -> Any:
        if any(action.get("ConditionCheck", {}).get("Key") == key
               for action in request["TransactItems"]):
            raise error
        return original(**request)

    monkeypatch.setattr(aws.dynamodb, "transact_write_items", transact_write_items)
    return error


def _failure_seed(aws: _OfflineAws, monkeypatch: pytest.MonkeyPatch, kind: str) -> ClientError:
    _seed_publication(aws)
    if kind == "run":
        _seed_graph(aws)
        _other_graph(aws, "aaa-failure")
        original = aws.control_plane.get_run(_TENANT, _RUN)
        assert original is not None
        aws.control_plane.put_run(original.model_copy(update={"state": RunState.FAILED}))
        return _fail_run_write(aws, monkeypatch)
    _orphan(aws)
    error = _error("AccessDenied", "DescribeExecution")
    aws.step_functions.describe_execution.side_effect = error
    return error


def _assert_failure_summary(events: list[dict[str, Any]], kind: str) -> None:
    summary = next(event for event in events if event["event"] == "processor_recovery_completed")
    failed = int(kind == "run")
    assert (summary["scanned"], summary["recovered"], summary["failed"],
            summary["reconcile_failed"]) == (1 + failed, 1, failed, not failed)
    recovery = [event for event in events if event["event"].startswith("processor_recovery_")
                or event["event"] == "processor_execution_observed"]
    rendered = json.dumps(recovery)
    for forbidden in ("arn:", "serving/", "raw/", "manifest", "https://", "Bearer"):
        assert forbidden not in rendered


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", [
    ("run", False), ("reconcile", False), ("run", True), ("reconcile", True),
], ids=["run-factory", "reconcile-factory", "run-main", "reconcile-main"])
async def test_falha_real_preserva_resumo_e_processa_candidato_restante(
    offline_aws: _OfflineAws, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
    scenario: tuple[str, bool],
) -> None:
    kind, public = scenario
    injected = _failure_seed(offline_aws, monkeypatch, kind)
    caplog.set_level(logging.INFO)
    failure: RecoveryFailed | None = None
    if public:
        rc, events = await _invoke_recovery(offline_aws, _values())
        assert rc == 1, events
        failure_event = next(e for e in events if e["event"] == "processor_entrypoint_failed")
        assert failure_event["exception_type"] == "RecoveryFailed"
    else:
        runtime = _runtime(offline_aws)
        with pytest.raises(RecoveryFailed) as caught:
            runtime.recovery.run_once(100)
        failure = caught.value
        events = [record.__dict__ | {"event": record.getMessage()} for record in caplog.records]
    _publication_snapshot(offline_aws)
    if not public:
        assert failure is not None
        assert (failure.failed, len(failure.results), failure.reconcile_failed) == (
            int(kind == "run"), 1, kind == "reconcile",
        )
        if kind == "reconcile":
            assert isinstance(failure.__cause__, ProcessorExecutionUnavailable)
            assert failure.__cause__.__cause__ is injected
        else:
            assert any(record.exc_info and record.exc_info[1] is injected
                       for record in caplog.records)
    elif kind == "run":
        assert any(event["event"].startswith("recover_run_error ")
                   and event["exception_type"] == "ClientError" for event in events)
    offline_aws.step_functions.start_execution.assert_not_called()
    _assert_failure_summary(events, kind)


@pytest.mark.asyncio
async def test_permit_running_persistido_adia_sem_start_e_retorna_zero(
    offline_aws: _OfflineAws,
) -> None:
    _seed_graph(offline_aws)
    _orphan(offline_aws)
    executions = _Executions(offline_aws)
    executions.stored["1111111111111111"] = {
        "executionArn": _PREFIX + "1111111111111111", "status": "RUNNING", "input": "{}",
    }
    permit = offline_aws.control_plane.observe_environment_gate("prod").permit

    rc, events = await _invoke_recovery(offline_aws, _values())

    assert rc == 0, events
    assert executions.calls == []
    offline_aws.step_functions.describe_execution.assert_any_call(
        executionArn=_PREFIX + "1111111111111111",
    )
    assert len(executions.stored) == 1
    assert offline_aws.control_plane.observe_environment_gate("prod").permit == permit
    assert any(event["event"].startswith("dispatch_deferred ")
               and "reason=permit_held" in event["event"] for event in events)
    dispatch, units, _ = _snapshot(offline_aws)
    assert (dispatch.state, dispatch.execution_ref) == (DispatchState.RESERVED, None)
    assert all(unit.state is RunUnitState.PENDING and unit.attempt == 0 for unit in units)


def test_retry_terminal_already_exists_confirma_identidade_sem_criar_task(
    offline_aws: _OfflineAws,
) -> None:
    _seed_graph(offline_aws)
    executions = _Executions(offline_aws)
    assert _runtime(offline_aws).recovery.run_once(100).recovered == 1
    wire = executions.calls[0]
    identity = executions.stored[wire["name"]]
    identity["status"] = "SUCCEEDED"
    executor = StepFunctionsExecutor(offline_aws.step_functions, wire["stateMachineArn"])
    request = StartRunExecution.model_validate_json(wire["input"])

    assert executor.start(request) == identity["executionArn"]

    assert len(executions.calls) == 2
    assert executions.calls[0] == executions.calls[1]
    assert len(executions.stored) == 1
    offline_aws.step_functions.describe_execution.assert_called_with(
        executionArn=identity["executionArn"],
    )
