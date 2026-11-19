"""Orquestrador do canary de modo: rota pós-B/C, RunTask exato, freeze simulado e resultado."""
from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

import pytest

from cnes_infra.aws.mode_canary import canonical_text
from scripts.promotion.mode_canary import (
    RESULT_FIELDS,
    ModeCanaryDenied,
    client_token,
    execute_mode_canary,
    route_from_readbacks,
    run_task_request,
    validate_run_task_request,
)
from scripts.promotion.mode_canary_contract import ContractError

_CANARY = "0123456789abcdef" * 2
_TENANT = f"prod-canary-{_CANARY}"
_OWNER = "4" * 64
_RUNS = (f"recovery-a-{_OWNER[:16]}", f"recovery-b-{_OWNER[:16]}")
_ACCOUNT = "arn:aws:ecs:us-east-2:123456789012"
_MODES = ("unit", "recovery", "audit")
_REV = {m: f"{_ACCOUNT}:task-definition/cnesdata-prod-{m}:17" for m in _MODES}
_CLUSTER = f"{_ACCOUNT}:cluster/cnesdata-prod"
_MACHINE = "arn:aws:states:us-east-2:123456789012:stateMachine:cnesdata-prod"
_NOW = datetime(2026, 11, 13, 12, 5, tzinfo=UTC)
_NETWORK = {"awsvpcConfiguration": {"Subnets": ["subnet-a"], "SecurityGroups": ["sg-a"],
                                    "AssignPublicIp": "ENABLED"}}


def _record(mode: str = "recovery", **changes: Any) -> dict[str, Any]:
    scenario = {"unit": "unit_wave", "recovery": "recovery_overlap_a", "audit": "audit_delivery"}
    runs = [f"unit-{_OWNER[:16]}"] if mode == "unit" else list(_RUNS)
    return {
        "schema": "cnesdata.mode-canary/v2", "activation_sha256": "a" * 64,
        "candidate_sha256": "b" * 64, "source_sha": "d" * 40, "release_id": "r-499",
        "canary_id": _CANARY, "owner_sha256": _OWNER, "promotion_id": "promo-1",
        "promotion_generation": 3, "phase_receipt_sha256": "c" * 64, "machine_arn": _MACHINE,
        "revisions": dict(_REV), "run_ids": runs, "fixture_manifest_sha256": "e" * 64,
        "seed_manifest_sha256": "f" * 64, "fixed_deadline": "2026-11-13T12:30:00Z",
        "stage_deadline": "2026-11-13T12:25:00Z", "canary_deadline": "2026-11-13T12:25:00Z",
        "phase": mode, "freeze_policy_sha256": "1" * 64, "expires_at": "2026-11-13T12:30:00Z",
        "context_kind": "promotion", "context_manifest_sha256": "2" * 64,
        "scenario": scenario[mode], "fault": "none", "mode": mode,
    } | changes


def _schedule(mode: str) -> dict[str, Any]:
    return {"Target": {
        "Arn": _CLUSTER, "RetryPolicy": {"MaximumRetryAttempts": 0},
        "EcsParameters": {"TaskDefinitionArn": _REV[mode], "TaskCount": 1,
                          "LaunchType": "FARGATE", "PlatformVersion": "1.4.0",
                          "NetworkConfiguration": _NETWORK},
    }}


def _task_definition(mode: str) -> dict[str, Any]:
    command = {"recovery": ["recover-once"], "audit": ["dispatch-outbox-once"]}.get(mode, [])
    return {"taskDefinition": {"taskDefinitionArn": _REV[mode], "containerDefinitions": [
        {"name": "processor", "command": command}]}}


def _readbacks() -> dict[str, Any]:
    definition = {"States": {"Units": {"ItemProcessor": {"States": {"Run": {
        "Parameters": {"TaskDefinition": _REV["unit"]}}}}}}}
    return {
        "state_machine": {"stateMachineArn": _MACHINE, "definition": json.dumps(definition)},
        "schedules": {m: _schedule(m) for m in ("recovery", "audit")},
        "task_definitions": {m: _task_definition(m) for m in ("unit", "recovery", "audit")},
    }


class _Ledger:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def admit_effect(self, kind: str, request: str, target: str, now: datetime) -> str:
        self.calls.append(("admit", (kind, request, target, now)))
        return hashlib.sha256(f"{kind}{request}".encode()).hexdigest()

    def mark_unknown(self, effect_id: str) -> None:
        self.calls.append(("unknown", effect_id))

    def settle(self, effect_id: str, receipt: str) -> None:
        self.calls.append(("settle", effect_id))


class _Context:
    def __init__(self, responses: dict[str, Any] | None = None) -> None:
        self.responses = responses or {}
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.effects = _Ledger()
        self.units: list[Any] = []

    def now(self) -> datetime:
        return _NOW

    def call(self, alias: str, operation: str, request: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((alias, operation, request))
        response = self.responses.get(operation)
        if isinstance(response, Exception):
            raise response
        return deepcopy(response) if response is not None else {}

    def observe(self, name: str, body: bytes) -> dict[str, Any]:
        return {"sha256": hashlib.sha256(body).hexdigest(), "size": len(body)}

    def ledger(self) -> _Ledger:
        return self.effects

    def dispatch_unit(self, binding: Any) -> dict[str, Any]:
        self.units.append(binding)
        return deepcopy(self.responses["unit"])


def _originals(mode: str = "recovery", **changes: Any) -> dict[str, Any]:
    record = canonical_text(_record(mode)).encode()
    return {"record": record, "readbacks": _readbacks(), "readbacks_after": _readbacks(),
            "previous": None, "frozen": False,
            "audit_event_ids": ["run.publishing." + _RUNS[0]], "seed_sha256": "5" * 64,
            "started_at": "2026-11-13T12:01:00Z"} | changes


def _stopped(mode: str, code: int = 0) -> dict[str, Any]:
    return {"tasks": [{"taskArn": f"{_ACCOUNT}:task/cnesdata-prod/abc", "lastStatus": "STOPPED",
                       "taskDefinitionArn": _REV[mode], "containers": [{"exitCode": code}]}],
            "failures": []}


def _launched(mode: str) -> dict[str, Any]:
    return {"tasks": [{"taskArn": f"{_ACCOUNT}:task/cnesdata-prod/abc",
                       "taskDefinitionArn": _REV[mode]}], "failures": []}


def _route(mode: str) -> Any:
    from cnes_domain.orchestration.mode_canary import ModeCanaryRecord

    return route_from_readbacks(ModeCanaryRecord.from_mapping(_record(mode)), _readbacks(), mode)


def test_unit_usa_maquina_prod_e_revision_pos_b() -> None:
    unit = {"execution": {"status": "SUCCEEDED", "stateMachineArn": _MACHINE},
            "tasks": _stopped("unit")["tasks"], "outputs": [{"sha256": "6" * 64, "size": 10}]}
    context = _Context({"unit": unit})
    result = json.loads(execute_mode_canary("unit", _originals("unit"), context))
    assert context.units[0].run_ids == (f"unit-{_OWNER[:16]}",)
    assert (result["pending"], result["unknown"]) == ([], [])
    assert set(result) == set(RESULT_FIELDS)
    readbacks = _readbacks()
    readbacks["state_machine"]["stateMachineArn"] += "-clone"
    with pytest.raises(ContractError):
        execute_mode_canary("unit", _originals("unit", readbacks=readbacks), _Context())
    other = _readbacks()
    other["state_machine"]["definition"] = other["state_machine"]["definition"].replace(
        ":17", ":16")
    blocked = _Context({"unit": unit})
    with pytest.raises(ContractError):
        execute_mode_canary("unit", _originals("unit", readbacks=other), blocked)
    assert blocked.units == []


def test_recovery_usa_target_pos_c_sem_role_override() -> None:
    context = _Context({"ecs:RunTask": _launched("recovery"),
                        "ecs:DescribeTasks": _stopped("recovery")})
    result = json.loads(execute_mode_canary("recovery", _originals(), context))
    alias, _, request = context.calls[0]
    assert alias == "mode_dispatch"
    assert (request["taskDefinition"], request["cluster"], request["count"]) == (
        _REV["recovery"], _CLUSTER, 1)
    assert request["enableExecuteCommand"] is False
    assert request["networkConfiguration"] == {"awsvpcConfiguration": {
        "subnets": ["subnet-a"], "securityGroups": ["sg-a"], "assignPublicIp": "ENABLED"}}
    override = request["overrides"]
    assert set(override) == {"containerOverrides"}
    container = override["containerOverrides"][0]
    assert set(container) == {"name", "environment"}
    assert [item["name"] for item in container["environment"]] == ["PROD_CANARY_BINDING"]
    locator = json.loads(container["environment"][0]["value"])
    assert (locator["tenant_id"], locator["run_ids"]) == (_TENANT, list(_RUNS))
    assert [name for name, _ in context.effects.calls] == ["admit", "settle"]
    assert result["pending"] == []
    changed = _readbacks()
    changed["schedules"]["recovery"]["Target"]["EcsParameters"]["TaskCount"] = 2
    again = _Context({"ecs:RunTask": _launched("recovery"),
                      "ecs:DescribeTasks": _stopped("recovery")})
    moved = json.loads(execute_mode_canary("recovery", _originals(readbacks_after=changed), again))
    assert "routing_changed" in moved["pending"]
    assert moved["routing_after_sha256"] != moved["routing_before_sha256"]
    schedule = _readbacks()
    schedule["schedules"]["recovery"]["Target"]["RetryPolicy"]["MaximumRetryAttempts"] = 1
    with pytest.raises(ContractError):
        execute_mode_canary("recovery", _originals(readbacks=schedule), _Context())


def test_audit_pos_c_nao_reusa_helper_471() -> None:
    context = _Context({"ecs:RunTask": _launched("audit"), "ecs:DescribeTasks": _stopped("audit")})
    execute_mode_canary("audit", _originals("audit"), context)
    alias, operation, request = context.calls[0]
    assert (alias, operation, request["taskDefinition"]) == (
        "mode_audit_launch", "ecs:RunTask", _REV["audit"])
    environment = request["overrides"]["containerOverrides"][0]["environment"]
    assert [item["name"] for item in environment] == ["PROD_CANARY_AUDIT_OBSERVE"]
    assert json.loads(environment[0]["value"])["event_ids"] == ["run.publishing." + _RUNS[0]]
    assert context.effects.calls == []


@pytest.mark.parametrize("override", [
    {"command": ["sh"]}, {"taskRoleArn": "arn:aws:iam::1:role/x"},
    {"executionRoleArn": "arn:aws:iam::1:role/x"}, {"cpu": "4096"},
])
def test_context_recusa_override_de_comando_role_endpoint_e_fonte(override: dict[str, Any]) -> None:
    route = _route("recovery")
    request = run_task_request(route, '{"x":1}', client_token("9" * 64, "recovery_resume", 0))
    validate_run_task_request(request, route)
    mutated = deepcopy(request)
    mutated["overrides"]["containerOverrides"][0] |= override
    with pytest.raises(ContractError):
        validate_run_task_request(mutated, route)
    endpoint = deepcopy(request)
    endpoint["overrides"]["containerOverrides"][0]["environment"].append(
        {"name": "AWS_ENDPOINT_URL", "value": "http://x"})
    source = deepcopy(request) | {"taskDefinition": _REV["recovery"].replace(":17", ":18")}
    for mutated in (endpoint, source, deepcopy(request) | {"count": 2},
                    deepcopy(request) | {"enableExecuteCommand": True}):
        with pytest.raises(ContractError):
            validate_run_task_request(mutated, route)


def test_freeze_simulado_nega_seed_unit_recovery_e_permite_audit() -> None:
    blocked = _Context({"ecs:RunTask": _launched("recovery")})
    with pytest.raises(ModeCanaryDenied, match="operation=recovery-start"):
        execute_mode_canary("recovery", _originals(frozen=True), blocked)
    assert blocked.calls == []
    assert blocked.effects.calls == []
    audit = _Context({"ecs:RunTask": _launched("audit"), "ecs:DescribeTasks": _stopped("audit")})
    result = json.loads(execute_mode_canary("audit", _originals("audit", frozen=True), audit))
    assert result["freeze_proof_sha256"] is not None
    assert json.loads(execute_mode_canary("audit", _originals("audit"), audit))[
        "freeze_proof_sha256"] is None


def test_timeout_run_task_preserva_unknown_e_client_token() -> None:
    lost = _Context({"ecs:RunTask": TimeoutError("read timeout")})
    first = json.loads(execute_mode_canary("recovery", _originals(), lost))
    assert first["unknown"]
    assert first["task_receipts"] == []
    assert lost.effects.calls[-1][0] == "unknown"
    again = _Context({"ecs:RunTask": _launched("recovery"),
                      "ecs:DescribeTasks": _stopped("recovery")})
    previous = canonical_text(first).encode()
    second = json.loads(execute_mode_canary("recovery", _originals(previous=previous), again))
    assert again.calls[0][2]["clientToken"] == lost.calls[0][2]["clientToken"]
    assert len(lost.calls[0][2]["clientToken"]) <= 64
    assert set(first["unknown"]) <= set(second["unknown"])


def test_sfn_terminal_sem_task_stopped_nao_libera() -> None:
    running = _stopped("unit")["tasks"]
    running[0]["lastStatus"] = "DEPROVISIONING"
    unit = {"execution": {"status": "SUCCEEDED", "stateMachineArn": _MACHINE},
            "tasks": running, "outputs": [{"sha256": "6" * 64, "size": 10}]}
    result = json.loads(execute_mode_canary("unit", _originals("unit"), _Context({"unit": unit})))
    assert "unit_task_not_stopped" in result["pending"]


def test_exit_zero_sem_output_nao_aceita() -> None:
    unit = {"execution": {"status": "SUCCEEDED", "stateMachineArn": _MACHINE},
            "tasks": _stopped("unit")["tasks"], "outputs": []}
    result = json.loads(execute_mode_canary("unit", _originals("unit"), _Context({"unit": unit})))
    assert "unit_output_missing" in result["pending"]
    assert result["output_receipts"] == []


def test_f_imutavel_atravessa_reentrada() -> None:
    context = _Context({"ecs:RunTask": _launched("recovery"),
                        "ecs:DescribeTasks": _stopped("recovery", 1)})
    first = json.loads(execute_mode_canary("recovery", _originals(), context))
    assert first["fixed_deadline"] == "2026-11-13T12:30:00Z"
    assert "recovery_task_failed" in first["pending"]
    previous = canonical_text(first).encode()
    later = _originals(previous=previous, started_at="2026-11-13T12:04:00Z")
    with pytest.raises(ContractError):
        execute_mode_canary("recovery", later, _Context())
    record = canonical_text(_record(fixed_deadline="2026-11-13T12:40:00Z",
                                    expires_at="2026-11-13T12:40:00Z")).encode()
    with pytest.raises(ContractError):
        execute_mode_canary("recovery", _originals(previous=previous, record=record), _Context())
    replay = json.loads(execute_mode_canary("recovery", _originals(previous=previous), context))
    assert set(first["pending"]) <= set(replay["pending"])
    assert replay["started_at"] == first["started_at"]
