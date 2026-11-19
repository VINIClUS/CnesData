"""Workflow manual promote-production: gatilho, pins, host gate e consumo do handoff."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from scripts.promotion._api_static_handoff import HOST_GATE_SHA
from scripts.promotion.build_host_archive import CALLER_REF

WORKFLOW = Path(__file__).resolve().parents[3] / ".github/workflows/promote-production.yml"


def _workflow() -> dict[Any, Any]:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_workflow_so_tem_workflow_dispatch_e_actions_fixadas() -> None:
    workflow = _workflow()
    triggers = workflow["on"] if "on" in workflow else workflow[True]
    assert set(triggers) == {"workflow_dispatch"}
    assert workflow["permissions"] == {}
    jobs = workflow["jobs"]
    assert list(jobs) == ["preamble", "closed-ready", "host-gate", "host-outcome"]
    assert CALLER_REF in jobs["preamble"]["if"]
    for name, job in jobs.items():
        if name != "host-gate":
            assert job["runs-on"] == "ubuntu-latest"
        for step in job.get("steps", []):
            if "uses" in step:
                assert re.fullmatch(r"[\w/-]+@[0-9a-f]{40}", step["uses"]), step["uses"]
            if step.get("uses", "").startswith("actions/checkout@"):
                assert step["with"]["persist-credentials"] is False
            assert "${{" not in step.get("run", ""), step.get("name")


def test_host_gate_fixado_e_closed_ready_recusa_sem_produtor() -> None:
    jobs = _workflow()["jobs"]
    gate = jobs["host-gate"]
    assert gate["uses"] == ("VINIClUS/infra-ansible/.github/workflows/"
                            f"reusable-cnesdata-host-deploy-gate.yml@{HOST_GATE_SHA}")
    assert set(gate["with"]) == {"plan_evidence_id", "plan_evidence_digest", "plan_run_id",
                                 "artifact_id", "artifact_digest"}
    assert gate["secrets"] == {"host_governance": "${{ secrets.CNESDATA_HOST_GOVERNANCE }}"}
    ready = jobs["closed-ready"]
    assert ready["permissions"] == {}
    assert all("uses" not in step for step in ready["steps"])
    assert ready["steps"][-1]["run"].rstrip().endswith("exit 1")
    assert ready["outputs"] == {
        "closed_ready_sha256": "${{ steps.ready.outputs.closed_ready_sha256 }}"}


def test_host_outcome_sincroniza_o_workspace_e_chama_o_handoff_completo() -> None:
    outcome = _workflow()["jobs"]["host-outcome"]
    assert "needs.host-gate.result" in outcome["if"]
    runs = [step for step in outcome["steps"] if "run" in step]
    assert [step["run"].strip() for step in runs[:-1]] == ["uv sync --locked --all-packages"]
    handoff = runs[-1]
    assert "uv run --no-sync python -m scripts.promotion._api_static_handoff" in handoff["run"]
    for flag in ("jobs", "artifacts", "outputs", "caller", "archive"):
        assert f'--{flag} "$WORK/{flag if flag != "archive" else "handoff"}.' in handoff["run"]
    assert '--closed-ready-sha256 "$CLOSED_READY_SHA256"' in handoff["run"]
    assert handoff["env"]["CLOSED_READY_SHA256"] == (
        "${{ needs.closed-ready.outputs.closed_ready_sha256 }}")
