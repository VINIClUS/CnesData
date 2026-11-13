"""Orquestrador do canary de modo: rota pós-B/C, lançamento exato, freeze simulado e resultado."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final, Protocol, cast

from cnes_domain.orchestration.mode_canary import ModeCanaryBinding, ModeCanaryRecord
from scripts.promotion.mode_canary_contract import (
    SESSION_OPERATIONS,
    ContractError,
    record_bytes,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

type Node = dict[str, Any]
RESULT_SCHEMA: Final = "cnesdata.mode-result/v1"
RESULT_FIELDS: Final = (
    "schema", "mode", "common", "phase_sha256", "routing_before_sha256",
    "routing_after_sha256", "seed_sha256", "binding_sha256", "stage_checkpoint_sha256",
    "fixed_deadline", "started_at", "observed_at", "execution_receipts", "task_receipts",
    "native_state_receipts", "output_receipts", "freeze_proof_sha256", "deadline_drill_sha256",
    "pending", "unknown", "operational_requirements",
)
_FREEZE_TARGETS: Final = frozenset({"mode_seed", "mode_dispatch"})
_COMMANDS: Final = {"recovery": ["recover-once"], "audit": ["dispatch-outbox-once"]}
_LOCATOR_KEYS: Final = {"recovery": "PROD_CANARY_BINDING", "audit": "PROD_CANARY_AUDIT_OBSERVE"}
_LAUNCHERS: Final = {"recovery": "mode_dispatch", "audit": "mode_audit_launch"}
_PROBES: Final = ("unit-seed", "unit-start", "recovery-seed", "recovery-start")
_HANG_EXIT: Final = 3
_TOKEN_LIMIT: Final = 64
_STAMP: Final = "%Y-%m-%dT%H:%M:%SZ"


class ModeCanaryDenied(ContractError):
    """Operação recusada pelo freeze simulado antes de qualquer efeito."""


class EffectLedger(Protocol):
    def admit_effect(self, kind: str, request: str, target: str, now: datetime) -> str: ...
    def mark_unknown(self, effect_id: str) -> None: ...
    def settle(self, effect_id: str, receipt: str) -> None: ...


class ModeCanaryCalls(Protocol):
    """Fronteira do context autenticado: chamadas por alias, observação e ledger."""

    def now(self) -> datetime: ...
    def call(self, alias: str, operation: str, request: Node) -> Node: ...
    def observe(self, name: str, body: bytes) -> Node: ...
    def ledger(self) -> EffectLedger: ...
    def dispatch_unit(self, binding: ModeCanaryBinding) -> Node: ...


def _require(condition: bool, field_name: str) -> None:
    if not condition:
        raise ContractError(f"reason=invalid field={field_name}")


def _text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _stamp(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).strftime(_STAMP)


@dataclass(frozen=True, slots=True)
class ModeRoute:
    """Alvo PROD lido e validado: máquina para unit, revisão do Scheduler para recovery/audit."""

    mode: str
    machine_arn: str
    revision: str
    cluster_arn: str | None = None
    container: str | None = None
    network: Node | None = None
    launch: Node = field(default_factory=dict)


def _task_definitions(node: Any) -> Iterator[Any]:
    if type(node) is dict:
        for key, value in cast("Node", node).items():
            if key == "TaskDefinition":
                yield value
            yield from _task_definitions(value)
    elif type(node) is list:
        for item in cast("list[Any]", node):
            yield from _task_definitions(item)


def _unit_route(record: ModeCanaryRecord, readbacks: Node) -> ModeRoute:
    machine = readbacks["state_machine"]
    _require(machine["stateMachineArn"] == record.machine_arn, "machine_arn")
    targets = set(_task_definitions(json.loads(machine["definition"])))
    _require(targets == {record.revisions["unit"]}, "revisions.unit")
    return ModeRoute("unit", record.machine_arn, record.revisions["unit"])


def _network(configuration: Node) -> Node:
    vpc = configuration["awsvpcConfiguration"]
    return {"awsvpcConfiguration": {"subnets": vpc["Subnets"],
                                    "securityGroups": vpc["SecurityGroups"],
                                    "assignPublicIp": vpc["AssignPublicIp"]}}


def _scheduled_route(record: ModeCanaryRecord, readbacks: Node, mode: str) -> ModeRoute:
    target = readbacks["schedules"][mode]["Target"]
    ecs = target["EcsParameters"]
    revision = record.revisions[mode]
    _require(ecs["TaskDefinitionArn"] == revision and ecs.get("TaskCount", 1) == 1,
             f"schedule.{mode}")
    _require(target["RetryPolicy"]["MaximumRetryAttempts"] == 0, f"retries.{mode}")
    definition = readbacks["task_definitions"][mode]["taskDefinition"]
    containers = definition["containerDefinitions"]
    _require(definition["taskDefinitionArn"] == revision and len(containers) == 1,
             f"task_definition.{mode}")
    _require(containers[0].get("command") == _COMMANDS[mode], f"command.{mode}")
    launch = {name: ecs[key] for key, name in (("LaunchType", "launchType"),
                                               ("PlatformVersion", "platformVersion"))
              if key in ecs}
    return ModeRoute(mode, record.machine_arn, revision, target["Arn"], containers[0]["name"],
                     _network(ecs["NetworkConfiguration"]), launch)


def route_from_readbacks(record: ModeCanaryRecord, readbacks: Node, mode: str) -> ModeRoute:
    """Returns: Rota do modo conferida contra o record e os readbacks originais.

    Raises: ContractError: Máquina/revisão divergentes, retries, comando ou rede fora do alvo.
    """
    try:
        if mode == "unit":
            return _unit_route(record, readbacks)
        return _scheduled_route(record, readbacks, mode)
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, ContractError):
            raise
        raise ContractError(f"reason=invalid field=readbacks.{mode}") from error


def client_token(record_sha256: str, scenario: str, ordinal: int) -> str:
    """Returns: clientToken determinístico (<=64) do record, cenário e ordinal."""
    digest = _sha(_text({"ordinal": ordinal, "record_sha256": record_sha256,
                         "scenario": scenario}))
    return f"mc-{digest[:60]}"


def run_task_request(route: ModeRoute, locator: str, token: str) -> Node:
    """Returns: RunTask da revisão exata, Count1, sem ExecuteCommand e só o locator no ambiente."""
    environment = [{"name": _LOCATOR_KEYS[route.mode], "value": locator}]
    return {
        "cluster": route.cluster_arn, "taskDefinition": route.revision, "count": 1,
        "enableExecuteCommand": False, "networkConfiguration": route.network,
        "overrides": {"containerOverrides": [{"name": route.container,
                                              "environment": environment}]},
        "clientToken": token,
    } | route.launch


def validate_run_task_request(request: Node, route: ModeRoute) -> None:
    """Raises: ContractError: Override de comando/role/recursos/endpoint, fonte ou count."""
    try:
        environment = request["overrides"]["containerOverrides"][0]["environment"]
        locator, token = environment[0]["value"], request["clientToken"]
    except (KeyError, IndexError, TypeError) as error:
        raise ContractError("reason=invalid field=run_task") from error
    _require(type(token) is str and len(token) <= _TOKEN_LIMIT, "clientToken")
    _require(request == run_task_request(route, locator, token), "run_task")


class FreezeGuard:
    """Freeze simulado: alvos (seed/dispatch) negados antes de efeito, audit/observe admitidos."""

    def __init__(self, frozen: bool) -> None:
        self.frozen = frozen
        self.decisions: list[Node] = []

    def decide(self, operation: str) -> bool:
        alias = SESSION_OPERATIONS[operation][1]
        allowed = not (self.frozen and alias in _FREEZE_TARGETS)
        self.decisions.append({"operation": operation, "alias": alias, "allowed": allowed})
        return allowed

    def admit(self, operation: str) -> None:
        """Raises: ModeCanaryDenied: Operação alvo do freeze."""
        if not self.decide(operation):
            raise ModeCanaryDenied(f"reason=freeze_denied operation={operation}")

    def proof_sha256(self) -> str | None:
        return _sha(_text(self.decisions)) if self.frozen else None


@dataclass(slots=True)
class _Parts:
    binding_sha256: str = ""
    executions: list[Node] = field(default_factory=list)
    tasks: list[Node] = field(default_factory=list)
    outputs: list[Node] = field(default_factory=list)
    pending: set[str] = field(default_factory=set)
    unknown: set[str] = field(default_factory=set)


def _locator(record: ModeCanaryRecord, record_sha: str, originals: Node) -> str:
    if record.mode == "audit":
        events = originals.get("audit_event_ids")
        _require(type(events) is list and bool(events), "audit_event_ids")
        value: Node = {"event_ids": events, "owner_sha256": record.owner_sha256,
                       "record_sha256": record_sha}
    else:
        value = {"context_kind": record.context_kind, "owner_sha256": record.owner_sha256,
                 "phase": record.phase, "record_sha256": record_sha,
                 "run_ids": list(record.run_ids), "tenant_id": f"prod-canary-{record.canary_id}"}
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _admit(context: ModeCanaryCalls, record: ModeCanaryRecord, request: Node,
           route: ModeRoute) -> str | None:
    if route.mode == "audit":
        return None
    kind = "run-hang" if record.fault == "hang_no_dispatch" else "run-recovery"
    return context.ledger().admit_effect(kind, _sha(_text(request)), _sha(route.revision),
                                         context.now())


def _observe_task(context: ModeCanaryCalls, route: ModeRoute, task_arn: str,
                  fault: str) -> tuple[Node | None, str | None]:
    response = context.call("mode_observe", "ecs:DescribeTasks",
                            {"cluster": route.cluster_arn, "tasks": [task_arn]})
    tasks = response.get("tasks") or []
    if len(tasks) != 1 or tasks[0].get("lastStatus") != "STOPPED":
        return None, f"{route.mode}_task_not_stopped"
    task = tasks[0]
    ref = context.observe(f"{route.mode}-task", _text(task).encode())
    expected = _HANG_EXIT if fault == "hang_no_dispatch" else 0
    codes = [container.get("exitCode") for container in task.get("containers", [])]
    if task.get("taskDefinitionArn") != route.revision or codes != [expected]:
        return ref, f"{route.mode}_task_failed"
    return ref, None


def _launch(context: ModeCanaryCalls, record: ModeCanaryRecord, route: ModeRoute,
            request: Node) -> _Parts:
    parts = _Parts()
    effect_id = _admit(context, record, request, route)
    try:
        response = context.call(_LAUNCHERS[route.mode], "ecs:RunTask", request)
        tasks = response.get("tasks") or []
        launched = not response.get("failures") and len(tasks) == 1
    except Exception:
        response, tasks, launched = {}, [], False
    if not launched or tasks[0].get("taskDefinitionArn") != route.revision:
        if effect_id is not None:
            context.ledger().mark_unknown(effect_id)
        parts.unknown.add(f"run_task:{request['clientToken']}")
        return parts
    ref, problem = _observe_task(context, route, tasks[0]["taskArn"], record.fault)
    if ref is not None:
        parts.tasks.append(ref)
    if problem is not None:
        parts.pending.add(problem)
    elif effect_id is not None and ref is not None:
        context.ledger().settle(effect_id, ref["sha256"])
    return parts


def _scheduled(context: ModeCanaryCalls, record: ModeCanaryRecord, record_sha: str,
               originals: Node) -> _Parts:
    route = route_from_readbacks(record, originals["readbacks"], record.mode)
    locator = _locator(record, record_sha, originals)
    request = run_task_request(route, locator, client_token(record_sha, record.scenario, 0))
    validate_run_task_request(request, route)
    parts = _launch(context, record, route, request)
    parts.binding_sha256 = _sha(locator)
    return parts


def _unit_checks(observed: Node, route: ModeRoute) -> set[str]:
    pending: set[str] = set()
    execution, tasks = observed["execution"], observed["tasks"]
    if (execution.get("status"), execution.get("stateMachineArn")) != ("SUCCEEDED",
                                                                       route.machine_arn):
        pending.add("unit_execution_not_succeeded")
    stopped = [task for task in tasks if task.get("lastStatus") == "STOPPED"
               and task.get("taskDefinitionArn") == route.revision]
    if not tasks or len(stopped) != len(tasks):
        pending.add("unit_task_not_stopped")
    if any(c.get("exitCode") != 0 for task in stopped for c in task.get("containers", [])):
        pending.add("unit_task_failed")
    if not observed["outputs"]:
        pending.add("unit_output_missing")
    return pending


def _unit(context: ModeCanaryCalls, record: ModeCanaryRecord, record_sha: str,
          originals: Node) -> _Parts:
    route = route_from_readbacks(record, originals["readbacks"], "unit")
    binding = ModeCanaryBinding(f"prod-canary-{record.canary_id}", record.run_ids,
                                record.owner_sha256, record_sha, "unit", record.context_kind)
    parts = _Parts(binding_sha256=_sha(_text({"record_sha256": record_sha,
                                               "run_ids": list(record.run_ids)})))
    try:
        observed = context.dispatch_unit(binding)
    except Exception:
        parts.unknown.add("unit_dispatch")
        return parts
    parts.executions.append(context.observe("unit-execution",
                                            _text(observed["execution"]).encode()))
    parts.tasks = [context.observe("unit-task", _text(task).encode())
                   for task in observed["tasks"]]
    parts.outputs = [context.observe("unit-output", _text(item).encode())
                     for item in observed["outputs"]]
    parts.pending = _unit_checks(observed, route)
    return parts


def _record(originals: Node, mode: str) -> tuple[ModeCanaryRecord, str]:
    raw = originals["record"]
    try:
        mapping = json.loads(raw)
        canonical = record_bytes(mapping)
    except ValueError as error:
        raise ContractError("reason=invalid field=record") from error
    _require(canonical == raw, "record")
    record = ModeCanaryRecord.from_mapping(mapping)
    _require(record.mode == mode, "mode")
    return record, hashlib.sha256(raw).hexdigest()


def _common(record: ModeCanaryRecord) -> Node:
    return {"activation_sha256": record.activation_sha256, "canary_id": record.canary_id,
            "candidate_sha256": record.candidate_sha256, "owner_sha256": record.owner_sha256,
            "promotion_generation": record.promotion_generation,
            "release_id": record.release_id, "source_sha": record.source_sha}


def _previous(originals: Node, mode: str, anchor: Node) -> Node | None:
    raw = originals.get("previous")
    if raw is None:
        return None
    previous = json.loads(raw)
    _require(type(previous) is dict and set(previous) == set(RESULT_FIELDS), "previous")
    for name, value in anchor.items():
        _require(previous[name] == value, f"reentry.{name}")
    _require(previous["mode"] == mode, "reentry.mode")
    return cast("Node", previous)


def _guard(mode: str, frozen: bool) -> FreezeGuard:
    guard = FreezeGuard(frozen)
    if mode == "audit" and frozen:
        for operation in _PROBES:
            guard.decide(operation)
    guard.admit(f"{mode}-start")
    return guard


def _routing(originals: Node, parts: _Parts) -> tuple[str, str]:
    before = _sha(_text(originals["readbacks"]))
    after = originals.get("readbacks_after")
    if after is None:
        parts.pending.add("routing_after_missing")
        return before, _sha(_text(None))
    if _sha(_text(after)) != before:
        parts.pending.add("routing_changed")
    return before, _sha(_text(after))


def execute_mode_canary(mode: str, originals: Node, context: ModeCanaryCalls) -> bytes:
    """Returns: Bytes canônicos do mode-result com os 21 campos fechados.

    Raises: ContractError: Record, rota, reentrada ou prazo fora do contrato;
        ModeCanaryDenied: operação negada pelo freeze simulado antes de efeito.
    """
    record, record_sha = _record(originals, mode)
    anchor = {"common": _common(record), "fixed_deadline": _stamp(record.fixed_deadline),
              "started_at": originals["started_at"]}
    previous = _previous(originals, mode, anchor)
    guard = _guard(mode, bool(originals.get("frozen")))
    now = context.now()
    _require(now < min(record.canary_deadline, record.expires_at), "canary_deadline")
    if mode == "unit":
        parts = _unit(context, record, record_sha, originals)
    else:
        parts = _scheduled(context, record, record_sha, originals)
    before, after = _routing(originals, parts)
    carried = previous or {"pending": [], "unknown": []}
    return _text({
        "schema": RESULT_SCHEMA, "mode": mode, **anchor,
        "phase_sha256": record.phase_receipt_sha256, "routing_before_sha256": before,
        "routing_after_sha256": after, "seed_sha256": originals["seed_sha256"],
        "binding_sha256": parts.binding_sha256, "stage_checkpoint_sha256": record_sha,
        "observed_at": _stamp(now), "execution_receipts": parts.executions,
        "task_receipts": parts.tasks, "native_state_receipts": [],
        "output_receipts": parts.outputs, "freeze_proof_sha256": guard.proof_sha256(),
        "deadline_drill_sha256": None,
        "pending": sorted(parts.pending | set(carried["pending"])),
        "unknown": sorted(parts.unknown | set(carried["unknown"])),
        "operational_requirements": [],
    }).encode()


__all__ = [
    "RESULT_FIELDS",
    "FreezeGuard",
    "ModeCanaryCalls",
    "ModeCanaryDenied",
    "ModeRoute",
    "client_token",
    "execute_mode_canary",
    "route_from_readbacks",
    "run_task_request",
    "validate_run_task_request",
]
