"""Caller promote-production v2, fatias b a d: catálogo commitado e jobs de gate (P28*)."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tarfile
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts.promotion import lifecycle_descriptor
from tests.production.delivery._plan_apply_fixtures import (
    CONTRACT_PATH,
    CONTRACT_REPOSITORY,
    CONTRACT_SHA256,
    PRODUCER,
)

ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github/workflows/promote-production.yml"
WORKFLOWS = ROOT / ".github/workflows"
CATALOG = ROOT / ".github/delivery/lifecycle-steps.json"
CATALOG_SHA256 = "5c991344a10da55f68fa70e8c52eeff30e47fee6e59ef8f76dc6be3435091bc4"
GENERATOR_PIN = "7adf2071c8444a4aeba7a51637c84ee176c754b8"
VECTOR = json.loads(Path(__file__).with_name("infra_gate_call_vector.json").read_bytes())
APPLICATION = json.loads(Path(__file__).with_name("application_gate_call_vector.json").read_bytes())
SECRET = "CNESDATA_DELIVERY_GOVERNANCE"  # noqa: S105 -- nome do secret, não valor.
WINDOW_NEEDS = {"admit", "preamble", "descriptor"}
ADMISSION = ["Verify complete public source and admission proof",
             "Repeat source gate then request exact OIDC role",
             "Execute under CAS watchdog and verify final publication"]


def _jobs() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]


def _steps() -> dict[str, Any]:
    return json.loads(CATALOG.read_bytes())


def _needs(job: dict[str, Any]) -> set[str]:
    needs = job.get("needs", [])
    return {needs} if isinstance(needs, str) else set(needs)


def _gates() -> dict[str, dict[str, Any]]:
    return {name: step for name, step in _steps().items() if step["role"] != "application"}


def _caller(step: dict[str, Any]) -> str:
    return step["job_name"].split(" / ")[0]


def _reusable_path(role: str) -> str:
    if role == "application":
        return APPLICATION["reusable"]["path"]
    return VECTOR["reusables"][role]["path"]


def test_catalogo_commitado_e_canonico_com_o_sha_declarado() -> None:
    raw = CATALOG.read_bytes()
    value = json.loads(raw)
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    assert raw == (canonical + "\n").encode()
    assert hashlib.sha256(raw).hexdigest() == CATALOG_SHA256
    assert len(value) == 16
    assert lifecycle_descriptor.catalog() == value


def test_job_names_do_catalogo_conferem_com_o_yaml() -> None:
    jobs = _jobs()
    for name, step in _steps().items():
        caller, reusable_job = step["job_name"].split(" / ")
        assert caller in jobs, name
        assert reusable_job == step["producer_job"] == "execute", name
        assert jobs[caller]["uses"] == (
            f"VINIClUS/infra-ansible/{_reusable_path(step['role'])}@{PRODUCER}"), name
    gate_users = {name for name, job in jobs.items()
                  if "reusable-cnesdata-infra-" in str(job.get("uses", ""))}
    assert gate_users == {_caller(step) for step in _gates().values()}


def test_jobs_de_gate_esperam_so_admit_preamble_descriptor_sem_cadeia_entre_gates() -> None:
    jobs = _jobs()
    gates = {_caller(step) for step in _gates().values()}
    assert len(gates) == 15
    for name in gates:
        assert _needs(jobs[name]) == WINDOW_NEEDS, name
        assert "strategy" not in jobs[name]
        assert "if" not in jobs[name]
    for name, job in jobs.items():
        assert not _needs(job) & gates, name


def test_with_secrets_e_permissoes_de_cada_gate_conferem_com_o_workflow_call() -> None:
    jobs = _jobs()
    for name, step in _gates().items():
        job, reusable = jobs[_caller(step)], VECTOR["reusables"][step["role"]]
        call = reusable["workflow_call"]
        key = lifecycle_descriptor.output_key(name)
        assert set(job["with"]) == set(call["inputs"]), name
        assert all(spec["required"] for spec in call["inputs"].values())
        assert job["with"] == {
            "request_descriptor": f"${{{{ needs.descriptor.outputs.{key}_descriptor }}}}",
            "request_descriptor_sha256": (
                f"${{{{ needs.descriptor.outputs.{key}_descriptor_sha256 }}}}"),
            "contract_repository": CONTRACT_REPOSITORY, "contract_source_sha": PRODUCER,
            "contract_path": CONTRACT_PATH, "contract_sha256": CONTRACT_SHA256,
            "expected_source_sha": "${{ needs.admit.outputs.source_sha }}",
            "purpose": step["purpose"]}, name
        assert job["secrets"] == {SECRET: "${{ secrets.CNESDATA_DELIVERY_GOVERNANCE }}"}
        assert set(call["secrets"]) == {SECRET}
        assert job["permissions"] == reusable["permissions"], name


def test_descriptor_expoe_um_par_por_passo_de_gate() -> None:
    outputs = _jobs()["descriptor"]["outputs"]
    keys = {"request_descriptor", "request_descriptor_sha256"} | {
        f"{lifecycle_descriptor.output_key(name)}_descriptor{suffix}"
        for name in _gates() for suffix in ("", "_sha256")}
    assert set(outputs) == keys
    assert all(value == f"${{{{ steps.descriptor.outputs.{key} }}}}"
               for key, value in outputs.items())


def test_admission_steps_do_catalogo_sao_passos_reais_do_execute_do_reusable() -> None:
    for name, step in _steps().items():
        assert step["admission_steps"] == ADMISSION, name
    for role in ("plan", "audit", "deploy", "application"):
        assert set(ADMISSION) <= set(VECTOR["reusables"][role]["steps"]), role


def test_cadeia_de_checkpoint_dos_planos_segue_o_deploy_anterior() -> None:
    steps = _steps()
    chained = {name: step["input_jobs"].get("checkpoint", {}).get("step")
               for name, step in steps.items() if step["role"] == "plan"}
    assert chained == {"observer-bootstrap-plan": None,
                       "application-prepare-operational-plan": "observer-bootstrap",
                       "phase-b-plan": "application-prepare-operational",
                       "phase-c-recovery-plan": "phase-b",
                       "phase-c-audit-plan": "phase-c-recovery"}
    historical = {name for name, step in steps.items() if step.get("historical_entry") is True}
    assert historical == {"observer-bootstrap-plan", "observer-bootstrap"}
    assert all("inputs" not in step["input_jobs"] for step in steps.values())


def _triggers(document: dict[Any, Any]) -> set[str]:
    on = document.get("on", document.get(True))
    if isinstance(on, str):
        return {on}
    return set(on) if isinstance(on, (dict, list)) else set()


def _self_hosted(job: dict[str, Any]) -> bool:
    runs_on = job.get("runs-on", [])
    labels = [runs_on] if isinstance(runs_on, str) else runs_on
    if isinstance(runs_on, dict):
        labels = runs_on.get("labels", [])
    return any("self-hosted" in str(label) for label in labels)


def _called(job: dict[str, Any]) -> dict[str, Any]:
    uses = str(job.get("uses", ""))
    if not uses.startswith("./.github/workflows/"):
        return {}
    return yaml.safe_load((ROOT / uses.removeprefix("./")).read_text(encoding="utf-8"))["jobs"]


def self_hosted_from_pull_request(directory: Path) -> list[str]:
    found: list[str] = []
    pull = {"pull_request", "pull_request_target", "workflow_run"}
    for path in sorted(directory.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not _triggers(document) & pull:
            continue
        for name, job in document["jobs"].items():
            if _self_hosted(job) or any(map(_self_hosted, _called(job).values())):
                found.append(f"{path.name}:{name}")
    return found


def test_nenhum_runs_on_self_hosted_alcancavel_de_pull_request() -> None:
    assert self_hosted_from_pull_request(WORKFLOWS) == []


def test_regra_self_hosted_pega_job_alcancavel_de_pull_request(tmp_path: Path) -> None:
    (tmp_path / "pr.yml").write_text(
        'name: pr\n"on": [pull_request]\njobs:\n  build:\n    runs-on: [self-hosted, x64]\n'
        "    steps: []\n", encoding="utf-8")
    assert self_hosted_from_pull_request(tmp_path) == ["pr.yml:build"]


def _show(repository: str, revision: str, path: str) -> bytes:
    return subprocess.run(  # noqa: S603 -- git fixo sobre o checkout do pin.
        ["git", "-C", repository, "show", f"{revision}:{path}"],  # noqa: S607 -- git do dev.
        capture_output=True, check=True).stdout


def _repository() -> str:
    repository = os.environ.get("INFRA_ANSIBLE_REPO")
    if not repository:
        pytest.skip("INFRA_ANSIBLE_REPO ausente")
    return repository


@pytest.mark.ia_tree
def test_vetor_dos_gates_confere_com_a_arvore_do_infra_ansible_no_pin() -> None:
    repository = _repository()
    for role, reusable in VECTOR["reusables"].items():
        raw = _show(repository, VECTOR["ia_pin"], reusable["path"])
        assert hashlib.sha256(raw).hexdigest() == reusable["sha256"], role


@pytest.mark.ia_tree
def test_catalogo_e_regenerado_byte_a_byte_pelo_gerador_do_infra_ansible(tmp_path: Path) -> None:
    repository = _repository()
    archive = subprocess.run(  # noqa: S603 -- git fixo sobre o checkout do pin.
        ["git", "-C", repository, "archive", GENERATOR_PIN, "tools"],  # noqa: S607
        capture_output=True, check=True).stdout
    with tarfile.open(fileobj=BytesIO(archive)) as tree:
        tree.extractall(tmp_path, filter="data")
    script = ("import json,sys;from tools.cnesdata_delivery.producers import application as a;"
              "raw,_=a.catalog_member(json.load(open(sys.argv[1])));sys.stdout.buffer.write(raw)")
    done = subprocess.run(  # noqa: S603 -- gerador fixo do pin sobre o catálogo versionado.
        [sys.executable, "-c", script, str(CATALOG)], cwd=tmp_path,
        capture_output=True, check=False)
    assert done.returncode == 0, done.stderr.decode()
    assert done.stdout == CATALOG.read_bytes()
