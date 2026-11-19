"""Observação e evidência da mesma invocação nativa da raiz."""

import json
from pathlib import Path
from subprocess import CompletedProcess
from typing import NamedTuple

import pytest

from tests.production.infra._root_native_identities import Node, assert_root_identity_producers

type Identity = tuple[str, str]


class NativeRootReport(NamedTuple):
    result: CompletedProcess[str]
    statuses: dict[str, str]
    observations: Node


def _persist_result(result: CompletedProcess[str], seeds: list[Node], context: Node) -> Path:
    directory = Path(context["directory"])
    directory.mkdir(parents=True, exist_ok=True)
    stem = directory / context["label"]
    Path(f"{stem}.stdout").write_bytes(result.stdout.encode("utf-8"))
    Path(f"{stem}.stderr").write_bytes(result.stderr.encode("utf-8"))
    Path(f"{stem}.suite.tftest.hcl").write_bytes(context["suite"])
    command = {"argv": result.args, "returncode": result.returncode, "seeds": seeds,
               "label": context["label"], "metadata": context["metadata"]}
    Path(f"{stem}.result.json").write_text(json.dumps(command, indent=2) + "\n", encoding="utf-8")
    return stem


def _event_identity(event: Node, payload: Node | None = None) -> Identity:
    file, run = event.get("@testfile"), event.get("@testrun")
    if not isinstance(file, str) or not file or not isinstance(run, str) or not run:
        pytest.fail("setup_error reason=native_event_identity")
    if payload is not None and (payload.get("path") != file or payload.get("run") != run):
        pytest.fail("setup_error reason=native_terminal_identity")
    return file, run


def _record_terminal(event: Node, terminals: dict[Identity, str]) -> None:
    payload = event["test_run"]
    identity = _event_identity(event, payload)
    status = payload["status"]
    if identity in terminals or status not in {"pass", "fail", "error", "skip"}:
        pytest.fail(f"setup_error native_terminal={identity} status={status}")
    terminals[identity] = status


def _record_event(event: Node, terminals: dict[Identity, str], observations: Node) -> None:
    kind = event.get("type")
    if kind == "test_run":
        _record_terminal(event, terminals)
    elif kind == "test_plan":
        identity = _event_identity(event)
        if identity in observations["plans"]:
            pytest.fail(f"setup_error native_plan_duplicate={identity}")
        if not isinstance(event["test_plan"], dict):
            pytest.fail(f"setup_error native_plan_payload={identity}")
        observations["plans"][identity] = event["test_plan"]
    elif kind == "test_summary":
        if observations["summary"] is not None or not isinstance(event["test_summary"], dict):
            pytest.fail("setup_error reason=native_summary_duplicate_or_malformed")
        observations["summary"] = event
    elif kind == "diagnostic":
        if not isinstance(event["diagnostic"], dict):
            pytest.fail("setup_error reason=native_diagnostic_payload")
        observations["diagnostics"].append(event)


def _assert_unambiguous(identities: list[Identity]) -> None:
    files: dict[str, str] = {}
    for file, run in identities:
        if run in files and files[run] != file:
            pytest.fail(f"setup_error native_run_ambiguous={run}")
        files[run] = file


def _parse_result(stdout: str) -> tuple[dict[str, str], Node]:
    terminals: dict[Identity, str] = {}
    observations: Node = {"plans": {}, "summary": None, "diagnostics": []}
    try:
        for line in stdout.splitlines():
            if not line.strip():
                continue
            event = json.loads(line)
            if not isinstance(event, dict):
                pytest.fail("setup_error reason=native_event_object")
            _record_event(event, terminals, observations)
        _assert_unambiguous([*terminals, *observations["plans"]])
    except (ValueError, KeyError, TypeError, AttributeError) as error:
        pytest.fail(f"setup_error native_event_structure={error}")
    return {run: status for (_, run), status in terminals.items()}, observations


def _assert_success_counts(counts: Node, statuses: dict[str, str]) -> None:
    if (
        counts.get("status") != "pass"
        or any(counts.get(name) != 0 for name in ("failed", "errored", "skipped"))
        or counts.get("passed") != len(statuses)
        or any(status != "pass" for status in statuses.values())
    ):
        pytest.fail("setup_error reason=native_positive_status_summary")


def _assert_successful_suite(report: NativeRootReport) -> None:
    summary = report.observations["summary"]
    if report.result.returncode != 0 or summary is None:
        pytest.fail("setup_error reason=native_positive_result_or_summary")
    _assert_success_counts(summary["test_summary"], report.statuses)
    if any(event["diagnostic"].get("severity") == "error"
           for event in report.observations["diagnostics"]):
        pytest.fail("setup_error reason=native_positive_diagnostic")


def _assert_positive_roots(report: NativeRootReport, seeds: list[Node], spec: Node) -> None:
    if not spec["positive_roots"]:
        return
    _assert_successful_suite(report)
    for run in spec["positive_roots"]:
        identity = (spec["test_file"], run)
        plan = report.observations["plans"].get(identity)
        if report.statuses.get(run) != "pass" or not plan or not plan.get("resource_changes"):
            pytest.fail(f"setup_error native_positive_plan={identity}")
        assert_root_identity_producers(plan, seeds)


def observe_root_result(
    result: CompletedProcess[str], seeds: list[Node], observation_spec: Node, evidence_context: Node
) -> NativeRootReport:
    """Args: Resultado original, seeds, escopo positivo e contexto de evidência.
    Returns: Relatório com eventos originais associados a arquivo/run.
    Raises: pytest.fail.Exception em evidência inválida ou produtor divergente.
    """
    stem = _persist_result(result, seeds, evidence_context)
    statuses, observations = _parse_result(result.stdout)
    plans = [{"test_file": file, "run": run, "plan": plan}
             for (file, run), plan in observations["plans"].items()]
    Path(f"{stem}.plans.json").write_text(json.dumps(plans, indent=2) + "\n", encoding="utf-8")
    report = NativeRootReport(result, statuses, observations)
    _assert_positive_roots(report, seeds, observation_spec)
    return report
