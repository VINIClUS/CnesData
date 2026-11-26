"""Unit-start do canary de modo: execução, tasks e outputs amarrados ao dispatch do record."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Final

from cnes_infra.aws.mode_canary_unit_observe import (
    MANIFEST_FIELDS,
    OUTPUT_FIELDS,
    unit_execution_request,
    unit_task_arns,
)

if TYPE_CHECKING:
    from cnes_domain.orchestration.mode_canary import ModeCanaryBinding
    from scripts.promotion.mode_canary import ModeRoute

type Node = dict[str, Any]
OBSERVED_FIELDS: Final = frozenset({"execution", "tasks", "failures", "outputs"})
INPUT_FIELDS: Final = frozenset({"dispatch_id", "max_concurrency", "run_id", "tenant_id",
                                 "unit_ids", "wave_id"})


def _dig(node: Any, *keys: str) -> Any:
    for key in keys:
        node = node.get(key) if type(node) is dict else None
    return node


def records(value: Any) -> bool:
    """Returns: Lista em que todo item é objeto."""
    return type(value) is list and all(type(item) is dict for item in value)


def _input(execution: Node, route: ModeRoute, binding: ModeCanaryBinding) -> Node | None:
    try:
        request = json.loads(execution["input"])
        arn = unit_execution_request(route.machine_arn, request["dispatch_id"])["executionArn"]
    except (KeyError, TypeError, ValueError):
        return None
    if type(request) is not dict or set(request) != INPUT_FIELDS:
        return None
    units = request["unit_ids"]
    listed = type(units) is list and bool(units) and all(type(unit) is str for unit in units)
    bound = (request["tenant_id"], execution.get("name"), execution.get("executionArn")) == (
        binding.tenant_id, request["dispatch_id"], arn) and request["run_id"] in binding.run_ids
    return request if listed and len(set(units)) == len(units) and bound else None


def _environment(task: Node) -> dict[str, str]:
    overrides = _dig(task, "overrides", "containerOverrides")
    found = [item for item in overrides if type(item) is dict
             and item.get("name") == "processor"] if type(overrides) is list else []
    entries: Any = _dig(found[0], "environment") if len(found) == 1 else None
    if not records(entries) or not all(
            type(entry.get("name")) is str and type(entry.get("value")) is str
            for entry in entries):
        return {}
    environment = {entry["name"]: entry["value"] for entry in entries}
    return environment if len(environment) == len(entries) else {}


def _bound(tasks: list[Node], route: ModeRoute, request: Node, execution_arn: str) -> bool:
    expected = {"EXECUTION_OWNER": execution_arn, "DISPATCH_ID": request["dispatch_id"],
                "TENANT_ID": request["tenant_id"], "RUN_ID": request["run_id"]}
    units: list[str] = []
    for task in tasks:
        environment = _environment(task)
        if task.get("clusterArn") != route.cluster_arn or "UNIT_ID" not in environment or any(
                environment.get(name) != value for name, value in expected.items()):
            return False
        units.append(environment["UNIT_ID"])
    return sorted(units) == sorted(request["unit_ids"])


def _exited(task: Node) -> bool:
    containers = task.get("containers")
    return records(containers) and bool(containers) and all(
        type(container.get("exitCode")) is int and container["exitCode"] == 0
        for container in containers)


def _task_pending(observed: Node, route: ModeRoute, request: Node | None) -> set[str]:
    execution, tasks, failures = observed["execution"], observed["tasks"], observed["failures"]
    candidates = unit_task_arns(execution, route.cluster_arn) if route.cluster_arn else ()
    described = sorted([task.get("taskArn") for task in tasks]
                       + [failure.get("arn") for failure in failures], key=str)
    if request is None or not candidates or described != list(candidates):
        return {"unit_task_unbound"}
    if failures:
        missing = all(failure.get("reason") == "MISSING" for failure in failures)
        return {"unit_task_missing" if missing else "unit_task_unbound"}
    pending: set[str] = set()
    if any(task.get("lastStatus") != "STOPPED" or task.get("taskDefinitionArn") != route.revision
           for task in tasks):
        pending.add("unit_task_not_stopped")
    if not all(_exited(task) for task in tasks):
        pending.add("unit_task_failed")
    if not _bound(tasks, route, request, execution["executionArn"]):
        pending.add("unit_task_unbound")
    return pending


def _output_valid(output: Node, request: Node) -> bool:
    attempt, manifests = output.get("attempt"), output.get("output_manifests")
    if set(output) != set(OUTPUT_FIELDS) or type(attempt) is not int or attempt < 1 or not (
            records(manifests) and manifests):
        return False
    prefix = f"tmp/{request['tenant_id']}/{request['run_id']}/{output['unit_id']}/{attempt}"
    return (output["state"], output["dispatch_id"]) == ("SUCCEEDED", request["dispatch_id"]) and all(
        set(manifest) == set(MANIFEST_FIELDS) and manifest["manifest_key"]
        == f"{prefix}/manifests/{manifest['manifest_id']}/manifest.json" for manifest in manifests)


def _output_pending(outputs: list[Node], request: Node | None) -> set[str]:
    if not outputs:
        return {"unit_output_missing"}
    units = [output.get("unit_id") for output in outputs]
    valid = request is not None and sorted(units, key=str) == sorted(request["unit_ids"]) and all(
        _output_valid(output, request) for output in outputs)
    return set() if valid else {"unit_output_invalid"}


def unit_pending(observed: Node, route: ModeRoute, binding: ModeCanaryBinding) -> set[str]:
    """Returns: Pending do unit-start sobre o observado de formato fechado."""
    execution = observed["execution"]
    request = _input(execution, route, binding)
    pending = _task_pending(observed, route, request) | _output_pending(observed["outputs"],
                                                                        request)
    if request is None or (execution.get("status"), execution.get("stateMachineArn")) != (
            "SUCCEEDED", route.machine_arn):
        pending.add("unit_execution_not_succeeded")
    return pending


__all__ = ["INPUT_FIELDS", "OBSERVED_FIELDS", "records", "unit_pending"]
