"""P43: builders dos descriptors da janela confrontados com a admissão dos próprios workflows."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import sys
import tarfile
from io import BytesIO
from pathlib import Path
from shutil import which
from typing import Any

import pytest
import yaml

from release.verify_activation import _reference as consumer_reference
from scripts.promotion import verify_plan_artifact as admit
from scripts.promotion import window_descriptors as wd
from tests.production.delivery._plan_apply_fixtures import (
    BASELINES,
    PRODUCER,
    SOURCE,
    reference,
)

ROOT = Path(__file__).resolve().parents[3]
SIGN_WORKFLOW = ROOT / ".github/workflows/sign-activation.yml"
INFRA_REPOSITORY = "VINIClUS/infra-ansible"


def _spec(mode: str = "plan") -> dict[str, Any]:
    path = "infra-plan.yml" if mode == "apply" else "phase-inputs.yml"
    spec: dict[str, Any] = {
        "source_sha": SOURCE,
        "producer_pins": [
            {"repository": INFRA_REPOSITORY, "source_sha": PRODUCER, "content_sha256": "c" * 64},
        ],
        "baseline_artifacts": {name: reference(n) for n, name in enumerate(BASELINES, 1)},
        "payload_artifact": reference(100, "VINIClUS/CnesData", path),
    }
    if mode == "apply":
        spec["phase_inputs"] = reference(101, "VINIClUS/CnesData", "phase-inputs.yml")
    return spec


def _request(mode: str, built: wd.Descriptor, spec: dict[str, Any]) -> dict[str, Any]:
    request: dict[str, Any] = {
        "mode": mode, "purpose": "phase-a", "expected_source_sha": spec["source_sha"],
        "request_descriptor": built.raw.decode(), "request_descriptor_sha256": built.sha256,
    }
    request.update(built.transport)
    return request


@pytest.mark.parametrize("mode", ["plan", "apply"])
def test_descritor_phase_a_passa_na_validacao_do_verificador(mode: str) -> None:
    spec = _spec(mode)
    built = wd.phase_a_descriptor(mode, spec)
    request = _request(mode, built, spec)
    descriptor = admit._descriptor(request)
    assert admit._payload(request, descriptor) == spec["payload_artifact"]
    assert [item["purpose"] for item in descriptor["baseline_artifacts"]] == admit.BASELINES


@pytest.mark.parametrize("mode", ["plan", "apply"])
def test_sha_cobre_os_bytes_canonicos_com_um_lf_final(mode: str) -> None:
    built = wd.phase_a_descriptor(mode, _spec(mode))
    assert built.raw.endswith(b"}\n")
    assert not built.raw.endswith(b"\n\n")
    assert built.sha256 == hashlib.sha256(built.raw).hexdigest()
    assert built.sha256 != hashlib.sha256(built.raw.rstrip(b"\n")).hexdigest()


def test_transporte_do_plan_sao_os_tres_phase_inputs_do_payload() -> None:
    spec = _spec("plan")
    payload = spec["payload_artifact"]
    assert wd.phase_a_descriptor("plan", spec).transport == {
        "phase_inputs_run_id": str(payload["run_id"]),
        "phase_inputs_artifact_id": str(payload["artifact_id"]),
        "phase_inputs_artifact_digest": payload["artifact_digest"],
    }


def test_transporte_do_apply_traz_o_plan_e_os_phase_inputs_originais() -> None:
    spec = _spec("apply")
    payload, inputs = spec["payload_artifact"], spec["phase_inputs"]
    assert wd.phase_a_descriptor("apply", spec).transport == {
        "plan_run_id": str(payload["run_id"]),
        "artifact_id": str(payload["artifact_id"]),
        "artifact_digest": payload["artifact_digest"],
        "phase_inputs_run_id": str(inputs["run_id"]),
        "phase_inputs_artifact_id": str(inputs["artifact_id"]),
        "phase_inputs_artifact_digest": inputs["artifact_digest"],
    }


def _without(spec: dict[str, Any], key: str) -> dict[str, Any]:
    changed = copy.deepcopy(spec)
    del changed[key]
    return changed


def _drop_baseline(spec: dict[str, Any]) -> dict[str, Any]:
    changed = copy.deepcopy(spec)
    del changed["baseline_artifacts"]["cost"]
    return changed


def _extra_baseline(spec: dict[str, Any]) -> dict[str, Any]:
    changed = copy.deepcopy(spec)
    changed["baseline_artifacts"]["extra"] = reference(9)
    return changed


def _foreign_pin(spec: dict[str, Any]) -> dict[str, Any]:
    changed = copy.deepcopy(spec)
    changed["producer_pins"][0]["source_sha"] = "b" * 40
    return changed


def _bad_digest(spec: dict[str, Any]) -> dict[str, Any]:
    changed = copy.deepcopy(spec)
    changed["payload_artifact"]["artifact_digest"] = "sha256:short"
    return changed


def _bad_source(spec: dict[str, Any]) -> dict[str, Any]:
    changed = copy.deepcopy(spec)
    changed["source_sha"] = "not-a-sha"
    return changed


def _unknown_field(spec: dict[str, Any]) -> dict[str, Any]:
    return spec | {"latest": True}


@pytest.mark.parametrize("change", [
    _drop_baseline, _extra_baseline, _foreign_pin, _bad_digest, _bad_source, _unknown_field,
    lambda spec: _without(spec, "payload_artifact"), lambda spec: _without(spec, "producer_pins"),
])
def test_recusa_campo_errado_ausente_ou_extra_no_phase_a(change: Any) -> None:
    with pytest.raises(ValueError):
        wd.phase_a_descriptor("plan", change(_spec("plan")))


def test_recusa_modo_fora_de_plan_e_apply() -> None:
    with pytest.raises(ValueError, match="window_descriptor field=mode"):
        wd.phase_a_descriptor("destroy", _spec("plan"))


def test_apply_recusa_payload_que_nao_e_o_plan() -> None:
    spec = _spec("apply")
    spec["payload_artifact"] = reference(100, "VINIClUS/CnesData", "phase-inputs.yml")
    with pytest.raises(ValueError):
        wd.phase_a_descriptor("apply", spec)


def _activation_spec() -> dict[str, Any]:
    return {
        "source_sha": SOURCE,
        "candidate_reference": reference(1, "VINIClUS/CnesData", "candidate.yml"),
        "phase_a_reference": reference(2, "VINIClUS/CnesData", "infra-apply.yml"),
        "retained_unit_task_definition_arns": ["arn:retained:1", "arn:retained:2"],
    }


def _admit_step_script() -> str:
    jobs = yaml.safe_load(SIGN_WORKFLOW.read_text(encoding="utf-8"))["jobs"]
    return str(jobs["admit"]["steps"][0]["run"])


def _run_admit(
    built: wd.Descriptor, tmp_path: Path, source: str,
) -> subprocess.CompletedProcess[str]:
    output = tmp_path / "output"
    output.touch()
    environment = {
        "PATH": os.environ["PATH"], "GITHUB_REPOSITORY": "VINIClUS/CnesData",
        "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REF": "refs/heads/main",
        "GITHUB_SHA": source, "EXPECTED_SOURCE_SHA": source, "GITHUB_OUTPUT": str(output),
        "REQUEST_DESCRIPTOR": built.raw.decode(), "REQUEST_DESCRIPTOR_SHA256": built.sha256,
    }
    return subprocess.run(  # noqa: S603 -- bash fixo sobre o passo do próprio workflow.
        [which("bash") or "bash", "-c", _admit_step_script()], env=environment,
        capture_output=True, text=True, check=False)


def test_descritor_de_ativacao_passa_no_passo_admit_do_sign_activation(tmp_path: Path) -> None:
    built = wd.activation_descriptor(_activation_spec())
    done = _run_admit(built, tmp_path, SOURCE)
    assert done.returncode == 0, done.stderr
    assert f"request_descriptor_sha256={built.sha256}" in (tmp_path / "output").read_text()


def test_descritor_de_ativacao_tem_os_cinco_campos_fechados_do_consumidor() -> None:
    value = json.loads(wd.activation_descriptor(_activation_spec()).raw)
    assert set(value) == {"request_version", "mode", "source_sha", "inputs",
                          "ext2_post_upload_receipt_v1"}
    assert value["mode"] == "activation"
    assert value["ext2_post_upload_receipt_v1"] is None
    assert set(value["inputs"]) == {"candidate_reference", "phase_a_reference",
                                    "retained_unit_task_definition_arns"}


def test_ids_das_referencias_da_ativacao_sao_strings_decimais_como_o_consumidor_exige() -> None:
    spec = _activation_spec()
    inputs = json.loads(wd.activation_descriptor(spec).raw)["inputs"]
    for name in ("candidate_reference", "phase_a_reference"):
        for key in ("run_id", "run_attempt", "artifact_id"):
            assert inputs[name][key] == str(spec[name][key])


def test_consumidor_do_cnesdata_aceita_as_referencias_do_construtor() -> None:
    inputs = json.loads(wd.activation_descriptor(_activation_spec()).raw)["inputs"]
    for name in ("candidate_reference", "phase_a_reference"):
        assert consumer_reference(inputs[name], SOURCE) == inputs[name]


def test_consumidor_do_cnesdata_recusa_id_inteiro_semeado() -> None:
    inputs = json.loads(wd.activation_descriptor(_activation_spec()).raw)["inputs"]
    seeded = {**inputs["candidate_reference"], "run_id": 1}
    with pytest.raises(ValueError, match=r"^activation=reference_id$"):
        consumer_reference(seeded, SOURCE)


def test_sha_da_ativacao_cobre_os_bytes_com_o_lf_final() -> None:
    built = wd.activation_descriptor(_activation_spec())
    assert built.raw.endswith(b"}\n")
    assert built.sha256 == hashlib.sha256(built.raw).hexdigest()


def _unsorted_retained(spec: dict[str, Any]) -> dict[str, Any]:
    changed = copy.deepcopy(spec)
    changed["retained_unit_task_definition_arns"] = ["arn:b", "arn:a"]
    return changed


def _reference_other_source(spec: dict[str, Any]) -> dict[str, Any]:
    changed = copy.deepcopy(spec)
    changed["phase_a_reference"]["source_sha"] = "b" * 40
    return changed


def _reference_bad_workflow(spec: dict[str, Any]) -> dict[str, Any]:
    changed = copy.deepcopy(spec)
    changed["candidate_reference"]["workflow_path"] = "scripts/x.py"
    return changed


@pytest.mark.parametrize("change", [
    _unsorted_retained, _reference_other_source, _reference_bad_workflow,
    lambda spec: _without(spec, "candidate_reference"),
    lambda spec: spec | {"retained_unit_task_definition_arns": ["a"] * 2},
    lambda spec: spec | {"mode": "historical"},
])
def test_recusa_campo_errado_ausente_ou_extra_na_ativacao(change: Any) -> None:
    with pytest.raises(ValueError):
        wd.activation_descriptor(change(_activation_spec()))


def test_cli_escreve_o_descritor_e_imprime_sha_e_transporte(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(json.dumps(_spec("plan")), encoding="utf-8")
    code = wd.main(["phase-a", "--mode", "plan", "--spec", str(spec_path),
                    "--output", str(tmp_path / "out")])
    written = (tmp_path / "out" / "request_descriptor.json").read_bytes()
    lines = capsys.readouterr().out.splitlines()
    assert code == 0
    assert f"request_descriptor_sha256={hashlib.sha256(written).hexdigest()}" in lines
    assert any(line.startswith("phase_inputs_run_id=") for line in lines)


def test_cli_de_ativacao_escreve_o_descritor(tmp_path: Path) -> None:
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(json.dumps(_activation_spec()), encoding="utf-8")
    assert wd.main(["activation", "--spec", str(spec_path), "--output", str(tmp_path / "o")]) == 0
    written = (tmp_path / "o" / "request_descriptor.json").read_bytes()
    assert written == wd.activation_descriptor(_activation_spec()).raw


IA_SIGNING_PIN = "82e9ff210cbc0767c88f4f761b09e7ccfc38e50c"
DECODE_SCRIPT = (
    "import json,sys;from tools.cnesdata_delivery import signing_gate as g;"
    "raw=sys.stdin.buffer.read();"
    "r=g.decode_request(raw,digest=sys.argv[1],source_sha=sys.argv[2],mode='activation');"
    "sys.stdout.write(json.dumps(r,sort_keys=True))"
)


def _ia_tools(repository: str, target: Path) -> None:
    archive = subprocess.run(  # noqa: S603 -- git fixo sobre o checkout do pin.
        ["git", "-C", repository, "archive", IA_SIGNING_PIN, "tools"],  # noqa: S607 -- git do dev.
        capture_output=True, check=True).stdout
    with tarfile.open(fileobj=BytesIO(archive)) as tree:
        tree.extractall(target, filter="data")


def _ia_decode(target: Path, raw: bytes, sha: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(  # noqa: S603 -- decode_request do pin sobre bytes do builder.
        [sys.executable, "-c", DECODE_SCRIPT, sha, SOURCE], cwd=target, input=raw,
        capture_output=True, check=False)


@pytest.fixture
def ia_tree(tmp_path: Path) -> Path:
    repository = os.environ.get("INFRA_ANSIBLE_REPO")
    if not repository:
        pytest.skip("INFRA_ANSIBLE_REPO ausente")
    target = tmp_path / "ia"
    target.mkdir()
    _ia_tools(repository, target)
    return target


@pytest.mark.ia_tree
def test_decode_request_real_do_signing_gate_aceita_o_descritor_do_builder(ia_tree: Path) -> None:
    built = wd.activation_descriptor(_activation_spec())
    done = _ia_decode(ia_tree, built.raw, built.sha256)
    assert done.returncode == 0, done.stderr.decode()
    assert json.loads(done.stdout) == json.loads(built.raw)


@pytest.mark.ia_tree
def test_decode_request_real_recusa_descritor_com_campo_extra(ia_tree: Path) -> None:
    value = json.loads(wd.activation_descriptor(_activation_spec()).raw)
    value["inputs"]["latest"] = True
    raw = wd.canonical_bytes(value, wd.REQUEST_LIMIT)
    done = _ia_decode(ia_tree, raw, hashlib.sha256(raw).hexdigest())
    assert done.returncode != 0
    assert b"closed" in done.stderr or b"Refused" in done.stderr or b"Error" in done.stderr
