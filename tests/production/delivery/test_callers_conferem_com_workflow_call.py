"""N14 e R-X5: with, secrets e permissions de cada caller conferem com o reusable no pin."""

from __future__ import annotations

import copy
import os
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.production.delivery._caller_contract import caller_jobs, caller_problems
from tests.production.delivery._plan_apply_fixtures import PRODUCER

ROOT = Path(__file__).resolve().parents[3]
REUSABLE = {
    "on": {"workflow_call": {
        "inputs": {"purpose": {"required": True, "type": "string"},
                   "mode": {"required": False, "type": "string"}},
        "secrets": {"GOVERNANCE": {"required": True}}}},
    "jobs": {"execute": {"permissions": {"contents": "read", "id-token": "write"}}},
}
CALLER = {
    "uses": "VINIClUS/infra-ansible/.github/workflows/reusable-cnesdata-x.yml@" + "a" * 40,
    "permissions": {"contents": "read", "id-token": "write"},
    "with": {"purpose": "p"}, "secrets": {"GOVERNANCE": "${{ secrets.G }}"},
}


def _mutated(**changes: Any) -> dict[str, Any]:
    job = copy.deepcopy(CALLER)
    job.update(changes)
    return job


def test_caller_conforme_nao_tem_problemas() -> None:
    assert caller_problems(CALLER, REUSABLE) == []


def test_recusa_input_que_o_reusable_nao_declara() -> None:
    job = _mutated(**{"with": {"purpose": "p", "extra": "x"}})
    assert caller_problems(job, REUSABLE) == ["with_unknown=extra"]


def test_recusa_input_obrigatorio_ausente() -> None:
    assert caller_problems(_mutated(**{"with": {}}), REUSABLE) == ["with_missing=purpose"]


def test_recusa_secret_que_o_reusable_nao_declara_ou_obrigatorio_ausente() -> None:
    assert caller_problems(_mutated(secrets={"GOVERNANCE": "x", "OTHER": "y"}), REUSABLE) == [
        "secret_unknown=OTHER"]
    assert caller_problems(_mutated(secrets={}), REUSABLE) == ["secret_missing=GOVERNANCE"]


def test_recusa_permissao_do_caller_menor_que_a_do_job_do_reusable() -> None:
    job = _mutated(permissions={"contents": "read"})
    assert caller_problems(job, REUSABLE) == ["permission_missing=id-token:write job=execute"]
    weaker = _mutated(permissions={"contents": "none", "id-token": "write"})
    assert caller_problems(weaker, REUSABLE) == ["permission_missing=contents:read job=execute"]


def test_caller_sem_permissions_nao_cobre_o_reusable() -> None:
    job = copy.deepcopy(CALLER)
    del job["permissions"]
    assert caller_problems(job, REUSABLE) == [
        "permission_missing=contents:read job=execute",
        "permission_missing=id-token:write job=execute"]


def test_todo_caller_de_reusable_do_infra_ansible_usa_o_pin_unico() -> None:
    refs = {ref for *_, ref in caller_jobs(ROOT / ".github/workflows")}
    assert refs == {PRODUCER}


def _show(repository: str, pin: str, path: str) -> dict[str, Any]:
    raw = subprocess.run(  # noqa: S603 -- git fixo sobre o checkout do pin.
        ["git", "-C", repository, "show", f"{pin}:{path}"],  # noqa: S607 -- git do dev.
        capture_output=True, check=True).stdout
    return yaml.safe_load(raw)


@pytest.mark.ia_tree
def test_callers_conferem_com_o_workflow_call_do_reusable_no_pin() -> None:
    repository = os.environ.get("INFRA_ANSIBLE_REPO")
    if not repository:
        pytest.skip("INFRA_ANSIBLE_REPO ausente")
    found = caller_jobs(ROOT / ".github/workflows")
    assert found
    for source, job_name, job, reusable_path, ref in found:
        assert re.fullmatch(r"[0-9a-f]{40}", ref)
        problems = caller_problems(job, _show(repository, ref, reusable_path))
        assert problems == [], f"{source} {job_name}"
