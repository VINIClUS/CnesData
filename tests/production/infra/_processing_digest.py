"""Casos inválidos e classificação de evidência nativa da variável de digest."""

import json
import subprocess
from pathlib import Path
from re import finditer, match
from typing import Any

import pytest

type Node = dict[str, Any]

INVALID_DIGESTS = (
    ("empty", ""),
    ("tag", "latest"),
    (
        "full-foreign-uri",
        "123456789012.dkr.ecr.us-east-2.amazonaws.com/foreign@sha256:" + "a" * 64,
    ),
    ("uppercase", "sha256:" + "A" * 64),
    ("short", "sha256:" + "a" * 63),
    ("nonhex", "sha256:" + "g" * 64),
)
RULE_MARKER = "image_digest_shape=expected_null_or_sha256_lower_hex64"


def _setup_error(case: str, reason: str) -> None:
    pytest.fail(f"setup_error digest_case={case} reason={reason}")


def _events(result: subprocess.CompletedProcess[str], case: str) -> list[Node]:
    try:
        events = [json.loads(line) for line in result.stdout.splitlines()]
    except json.JSONDecodeError:
        _setup_error(case, "native_json_invalid")
        return []
    if not events or not all(isinstance(event, dict) for event in events):
        _setup_error(case, "native_events_invalid")
    return events


def _module_variables(root: Path, configuration: Node, case: str) -> tuple[Path, str]:
    module = (root / "../../modules/processing").resolve()
    calls = [
        (name, call)
        for name, call in configuration["root_module"].get("module_calls", {}).items()
        if (root / call["source"]).resolve() == module
    ]
    if len(calls) != 1 or "image_digest" not in calls[0][1]["module"].get("variables", {}):
        _setup_error(case, "digest_module_configuration")
    return module / "variables.tf", f"module.{calls[0][0]}.var.image_digest"


def _rule_source(diagnostic: Node, root: Path, target: Path) -> bool:
    locations = [diagnostic.get("range", {}).get("filename", "")]
    locations += [
        match[1]
        for match in finditer(r"([^\s:]+\.tf):[0-9]+(?:,[0-9]+-[0-9]+)?", diagnostic["detail"])
    ]
    return any(location and (root / location).resolve() == target for location in locations)


def _owning_variable(diagnostic: Node, root: Path, target: tuple[Path, str]) -> bool:
    snippet = diagnostic.get("snippet", {})
    context = snippet.get("context")
    location = (root / diagnostic.get("range", {}).get("filename", "")).resolve()
    declaration = context == 'variable "image_digest"' and location == target[0]
    address = diagnostic.get("address") == target[1]
    name = target[1].removeprefix("module.").removesuffix(".var.image_digest")
    argument = (
        context == f'module "{name}"'
        and location.parent == root.resolve()
        and match(r"\s*image_digest\s*=", snippet.get("code", "")) is not None
    )
    return declaration or address or argument


def _variable_rejection(diagnostic: Node, root: Path, target: tuple[Path, str]) -> bool:
    summary = diagnostic.get("summary")
    return (
        summary in {"Invalid value for variable", "Invalid value for input variable"}
        and RULE_MARKER in diagnostic.get("detail", "")
        and _owning_variable(diagnostic, root, target)
        and _rule_source(diagnostic, root, target[0])
    )


def assert_digest_rejected(
    result: subprocess.CompletedProcess[str], context: tuple[Path, Node], case: tuple[str, str]
) -> None:
    case_id, supplied = case
    root, configuration = context
    name = "digest_" + case_id.replace("-", "_")
    target = _module_variables(root, configuration, case_id)
    events = _events(result, case_id)
    statuses = {
        event["test_run"]["run"]: event["test_run"]["status"]
        for event in events
        if event.get("type") == "test_run"
    }
    diagnostics = [
        event["diagnostic"]
        for event in events
        if event.get("type") == "diagnostic" and event["diagnostic"]["severity"] == "error"
    ]
    plans = {
        event["@testrun"]: event["test_plan"]
        for event in events
        if event.get("type") == "test_plan"
    }
    if result.returncode == 0 and statuses == {name: "pass"} and not diagnostics:
        plan = plans.get(name, {})
        if not plan.get("resource_changes") or plan.get("configuration") != configuration:
            _setup_error(case_id, "successful_plan_missing_or_configuration_changed")
        if plan.get("variables", {}).get("image_digest") != {"value": supplied}:
            _setup_error(case_id, "supplied_digest_not_evaluated")
        assert diagnostics, f"processing_digest_validation_missing={case_id}"
    if result.returncode != 1 or statuses != {name: "error"} or not diagnostics:
        _setup_error(case_id, f"native_outcome_rc{result.returncode}_statuses{statuses!r}")
    if not all(_variable_rejection(diagnostic, root, target) for diagnostic in diagnostics):
        _setup_error(case_id, f"unexpected_native_diagnostics={diagnostics!r}")
