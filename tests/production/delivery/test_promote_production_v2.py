"""Caller promote-production v2, fatia a: admit, application e closed-ready real (P27)."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.production.delivery import _ia_lifecycle_steps_catalog as catalog
from tests.production.delivery._plan_apply_fixtures import (
    CONTRACT_PATH,
    CONTRACT_REPOSITORY,
    CONTRACT_SHA256,
    PRODUCER,
    PRODUCER_CLOSURE_SHA256,
)
from tests.production.delivery.conftest import require_tool

ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github/workflows/promote-production.yml"
VECTOR = json.loads(Path(__file__).with_name("application_gate_call_vector.json").read_bytes())
REUSABLE = VECTOR["reusable"]
SECRET = "CNESDATA_DELIVERY_GOVERNANCE"  # noqa: S105 -- nome do secret, não valor.
SHA = re.compile(r"(?<![0-9a-f])[0-9a-f]{40}(?![0-9a-f])")
ACTION_PIN = re.compile(r"uses: [\w./-]+@[0-9a-f]{40} # v[\d.]+")
SOURCE = "a" * 40
PRODUCTION_JOBS = ("application", "host-gate")


def _text() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def _jobs() -> dict[str, Any]:
    return yaml.safe_load(_text())["jobs"]


def _needs(job: dict[str, Any]) -> set[str]:
    needs = job.get("needs", [])
    return {needs} if isinstance(needs, str) else set(needs)


def _closure(name: str, jobs: dict[str, Any]) -> set[str]:
    seen: set[str] = set()
    pending = [name]
    while pending:
        for need in _needs(jobs[pending.pop()]):
            if need not in seen:
                seen.add(need)
                pending.append(need)
    return seen


def test_job_names_do_catalogo_conferem_com_o_yaml_na_fatia_a() -> None:
    jobs = _jobs()
    for name in catalog.SLICE_A_STEPS:
        caller_job, reusable_job = catalog.CATALOG[name]["job_name"].split(" / ")
        assert caller_job in jobs
        assert "uses" in jobs[caller_job]
        assert reusable_job == REUSABLE["job"]
    pending = {n for n in catalog.CATALOG if n not in catalog.SLICE_A_STEPS}
    pending_jobs = {catalog.CATALOG[n]["job_name"].split(" / ")[0] for n in pending}
    assert pending_jobs.isdisjoint(jobs)


def test_placeholder_do_closed_ready_acabou_e_a_sonda_real_esta_ligada() -> None:
    text = _text()
    job = _jobs()["closed-ready"]
    assert "close_drain_capability" not in text
    assert "exit 1" not in json.dumps(job)
    assert job["outputs"] == {
        "closed_ready_sha256": "${{ steps.probe.outputs.closed_ready_sha256 }}"}
    assert "scripts.promotion.closed_ready_probe" in json.dumps(job)
    assert _needs(job) == {"preamble"}


def test_admit_precede_todo_job_com_environment_ou_oidc() -> None:
    jobs = _jobs()
    assert _needs(jobs["admit"]) == set()
    for name in jobs:
        if name != "admit":
            assert "admit" in _closure(name, jobs), name
    assert jobs["admit"]["permissions"] == {}
    assert "environment" not in jobs["admit"]


ADMIT_ENV = {"GITHUB_REPOSITORY": "VINIClUS/CnesData", "GITHUB_EVENT_NAME": "workflow_dispatch",
             "GITHUB_REF": "refs/heads/main", "GITHUB_SHA": SOURCE, "SOURCE_SHA": SOURCE}


def _admit(tmp_path: Path, **change: str) -> subprocess.CompletedProcess[str]:
    step = _jobs()["admit"]["steps"][0]
    output = tmp_path / "output"
    environment = {"PATH": os.environ["PATH"], **ADMIT_ENV | change,
                   "GITHUB_OUTPUT": str(output)}
    return subprocess.run(  # noqa: S603 -- executa o run do admit versionado.
        ["bash", "-c", step["run"]],  # noqa: S607 -- bash do runner.
        env=environment, text=True, capture_output=True, check=False)


def test_admit_aceita_o_contexto_exato_e_publica_o_source_sha(tmp_path: Path) -> None:
    done = _admit(tmp_path)
    assert done.returncode == 0, done.stderr
    assert (tmp_path / "output").read_text() == f"source_sha={SOURCE}\n"


@pytest.mark.parametrize("change", [
    {"GITHUB_REPOSITORY": "VINIClUS/outro"}, {"GITHUB_EVENT_NAME": "push"},
    {"GITHUB_REF": "refs/heads/develop"}, {"GITHUB_SHA": "b" * 40}, {"SOURCE_SHA": "abc"},
], ids=["repositorio", "evento", "ref", "sha", "formato"])
def test_admit_recusa_contexto_fora_do_exato(tmp_path: Path, change: dict[str, str]) -> None:
    assert _admit(tmp_path, **change).returncode != 0


def test_application_chama_o_reusable_no_pin_da_constante_unica() -> None:
    application = _jobs()["application"]
    assert application["uses"] == f"VINIClUS/infra-ansible/{REUSABLE['path']}@{PRODUCER}"
    assert _needs(application) == {"admit", "descriptor"}
    assert "strategy" not in application
    assert "inherit" not in _text()


def test_with_secrets_e_permissoes_da_chamada_conferem_com_o_workflow_call_no_pin() -> None:
    application = _jobs()["application"]
    call = REUSABLE["workflow_call"]
    assert set(application["with"]) == set(call["inputs"])
    assert all(spec["required"] for spec in call["inputs"].values())
    assert set(application["secrets"]) == set(call["secrets"]) == {SECRET}
    assert application["secrets"][SECRET] == "${{ secrets.CNESDATA_DELIVERY_GOVERNANCE }}"
    assert application["permissions"] == REUSABLE["permissions"][REUSABLE["job"]]
    assert yaml.safe_load(_text())["permissions"] == {}


def test_inputs_do_contrato_sao_literais_iguais_as_constantes_do_pin() -> None:
    with_ = _jobs()["application"]["with"]
    assert with_["contract_repository"] == CONTRACT_REPOSITORY
    assert with_["contract_path"] == CONTRACT_PATH
    assert with_["contract_sha256"] == CONTRACT_SHA256
    assert with_["contract_source_sha"] == PRODUCER
    assert with_["purpose"] == catalog.CATALOG[catalog.KEEPER_STEP]["purpose"]
    assert with_["expected_source_sha"] == "${{ needs.admit.outputs.source_sha }}"
    assert with_["request_descriptor"] == "${{ needs.descriptor.outputs.request_descriptor }}"
    assert with_["request_descriptor_sha256"] == (
        "${{ needs.descriptor.outputs.request_descriptor_sha256 }}")


def test_descriptor_le_o_pin_e_o_closure_das_constantes() -> None:
    env = _jobs()["descriptor"]["steps"][-1]["env"]
    assert env["PRODUCER_WORKFLOW_SHA"] == PRODUCER
    assert env["PRODUCER_SOURCE_CLOSURE_SHA256"] == PRODUCER_CLOSURE_SHA256


def test_nenhum_sha_de_40_hex_fora_do_pin_e_dos_pins_de_actions() -> None:
    sem_actions = ACTION_PIN.sub("", _text())
    assert set(SHA.findall(sem_actions)) <= {PRODUCER}


def test_host_gate_e_host_outcome_seguem_o_dd2_com_needs_coerente() -> None:
    jobs = _jobs()
    assert _needs(jobs["host-gate"]) == {"preamble", "closed-ready"}
    assert jobs["host-gate"]["uses"].endswith(f"@{PRODUCER}")
    assert _needs(jobs["host-outcome"]) == {"closed-ready", "host-gate"}
    assert set(jobs) == {"admit", "preamble", "descriptor", "application", "closed-ready",
                         "host-gate", "host-outcome"}


def test_jobs_de_producao_nao_leem_o_workflow_pela_lista_do_referenced_workflows() -> None:
    assert "referenced_workflows" not in _text()


def test_actionlint_aceita_o_workflow() -> None:
    done = subprocess.run(  # noqa: S603 -- actionlint fixo sobre o workflow versionado.
        [require_tool("actionlint"), str(WORKFLOW)], capture_output=True, text=True, check=False)
    assert done.returncode == 0, done.stdout + done.stderr


def _show(repository: str, path: str) -> bytes:
    return subprocess.run(  # noqa: S603 -- git fixo sobre o checkout do pin.
        ["git", "-C", repository, "show", f"{PRODUCER}:{path}"],  # noqa: S607 -- git do dev.
        capture_output=True,
        check=True).stdout


def test_constantes_do_pin_conferem_com_a_arvore_do_infra_ansible_no_producer() -> None:
    repository = os.environ.get("INFRA_ANSIBLE_REPO")
    if not repository:
        pytest.skip("INFRA_ANSIBLE_REPO ausente")
    sha = lambda raw: hashlib.sha256(raw).hexdigest()  # noqa: E731
    files = {path: sha(_show(repository, path)) for path in VECTOR["closure_paths"]}
    canonical = json.dumps({"files": files}, sort_keys=True, separators=(",", ":")) + "\n"
    assert sha(canonical.encode()) == PRODUCER_CLOSURE_SHA256
    assert sha(_show(repository, CONTRACT_PATH)) == CONTRACT_SHA256
    assert sha(_show(repository, REUSABLE["path"])) == REUSABLE["sha256"]
