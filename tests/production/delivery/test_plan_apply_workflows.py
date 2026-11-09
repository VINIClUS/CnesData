"""Contratos atuais de admissão e plan/apply manuais exatos."""

import hashlib
import json
import os
import subprocess
from pathlib import Path
from shutil import which
from types import SimpleNamespace

import pytest
import yaml

from release.verify_candidate import timestamp
from scripts.promotion import verify_plan_artifact
from scripts.promotion._plan_contract import verify_application
from tests.production.delivery._plan_apply_fixtures import (
    APPLICATION_NOW,
    NOW,
    application,
    evidence,
    outputs,
    phase_apply,
    replace_payload,
    request,
    transport,
)
from tests.production.infra._cost_manifest_model import unsupported_keywords, validate

REPO_ROOT = Path(__file__).resolve().parents[3]
ROOT = REPO_ROOT
SCHEMA = REPO_ROOT / "release" / "plan-apply-evidence.schema.json"
WORKFLOWS = {
    "plan": REPO_ROOT / ".github" / "workflows" / "infra-plan.yml",
    "apply": REPO_ROOT / ".github" / "workflows" / "infra-apply.yml",
}


def test_schema_tem_um_unico_proprietario_a3() -> None:
    assert SCHEMA.is_file()
    schema = json.loads(SCHEMA.read_bytes())
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert not unsupported_keywords(schema)
    assert not (ROOT / "release" / "infra-plan.schema.json").exists()


@pytest.mark.parametrize(
    ("value", "rule"),
    [
        (3, {"type": "integer", "maximum": 2}),
        ("abc", {"type": "string", "maxLength": 2}),
        ([1, 2], {"type": "array", "items": {}, "maxItems": 1}),
        ([1, 1], {"type": "array", "items": {}, "uniqueItems": True}),
        (True, {"oneOf": [{"type": "integer"}, {"type": "string"}]}),
        (1, {"oneOf": [{"type": "integer"}, {"const": 1}]}),
    ],
)
def test_validador_de_schema_recusa_limites_e_unioes_ambiguas(value, rule) -> None:
    assert unsupported_keywords(rule) == []
    assert validate(value, rule)


def test_guard_segue_branches_e_recusa_keyword_desconhecida() -> None:
    rule = {"oneOf": [{"type": "string", "format": "uri"}]}
    assert unsupported_keywords(rule) == ["$.oneOf[0].format"]


def test_uniao_exige_exatamente_um_braco_sem_coercao() -> None:
    rule = {"oneOf": [{"type": "boolean"}, {"type": "integer", "maximum": 2}]}
    assert validate(True, rule) == []
    assert validate(2, rule) == []
    assert validate(3, rule)


@pytest.mark.parametrize("purpose", ["acm-certificate", "bootstrap", "phase-a"])
@pytest.mark.parametrize("status", ["planned", "applied"])
def test_schema_aceita_os_seis_casos_atuais_fechados(purpose, status) -> None:
    assert SCHEMA.is_file()
    assert validate(evidence(purpose, status), json.loads(SCHEMA.read_bytes())) == []


@pytest.mark.parametrize("case", ["extra", "boolean", "status", "purpose", "outputs", "pre"])
def test_schema_recusa_mistura_de_proposito_status_e_tipo(case) -> None:
    value = evidence()
    if case == "extra":
        value["artifact_id"] = 1
    elif case == "boolean":
        value["plan"]["producer"]["run_id"] = True
    elif case == "status":
        value["status"] = "planned"
    elif case == "purpose":
        value["plan"]["purpose"] = "phase-c"
    elif case == "outputs":
        value["apply"]["outputs"] = outputs("bootstrap")
    else:
        value["plan"]["cost"] = evidence("bootstrap")["plan"]["cost"]
    assert SCHEMA.is_file()
    assert validate(value, json.loads(SCHEMA.read_bytes()))


def admission():
    assert (ROOT / "scripts/promotion/verify_plan_artifact.py").is_file()
    return verify_plan_artifact


@pytest.mark.parametrize(
    ("mode", "purpose"),
    [
        ("plan", "acm-certificate"),
        ("plan", "bootstrap"),
        ("plan", "phase-a"),
        ("apply", "bootstrap"),
    ],
)
def test_admissao_publica_autentica_transporte_sem_credencial_aws(mode, purpose):
    value, api = request(mode, purpose)
    prepared = admission().prepare_request(value, api, NOW)
    assert prepared["expected_source_sha"] == "a" * 40
    assert prepared["purpose"] == purpose
    assert json.loads(prepared["request_descriptor"])["source_sha"] == "a" * 40


@pytest.mark.parametrize("case", ["attempt", "boundary", "digest", "expired", "source"])
def test_artefato_errado_ou_ambiguo_e_recusado_antes_do_reusable(case):
    value, api = request()
    run = api.responses["VINIClUS/infra-ansible", "actions/runs/1/attempts/1"]
    metadata = api.responses["VINIClUS/infra-ansible", "actions/artifacts/1001"]
    if case == "attempt":
        run["run_attempt"] = 2
    elif case == "boundary":
        metadata["created_at"] = run["run_started_at"]
    elif case == "digest":
        metadata["digest"] = "sha256:" + "0" * 64
    elif case == "expired":
        metadata["expired"] = True
    else:
        run["head_sha"] = "b" * 40
    with pytest.raises(ValueError):
        admission().prepare_request(value, api, NOW)


def test_source_sem_um_dos_dez_checks_e_recusado():
    value, api = request()
    checks = api.responses["VINIClUS/CnesData", "commits/" + "a" * 40 + "/check-runs?per_page=100"]
    checks["check_runs"][0]["conclusion"] = "failure"
    with pytest.raises(ValueError):
        admission().prepare_request(value, api, NOW)


def test_apply_liga_os_tres_transportes_ao_payload_real():
    value, api = request("apply", "bootstrap")
    value["artifact_id"] = "2000"
    with pytest.raises(ValueError):
        admission().prepare_request(value, api, NOW)


def workflow(mode):
    assert WORKFLOWS[mode].is_file()
    return yaml.safe_load(WORKFLOWS[mode].read_bytes())


@pytest.mark.parametrize("mode", ["plan", "apply"])
def test_validacao_publica_precede_reusable_e_id_token(mode):
    data = workflow(mode)
    assert set(data.get("on", data.get(True))) == {"workflow_dispatch"}
    assert data["permissions"] == {}
    assert data["concurrency"] == {"group": "cnesdata-prod-infra", "cancel-in-progress": False}
    admit, gate = data["jobs"]["admit"], data["jobs"]["gate"]
    assert admit["runs-on"] == "ubuntu-latest"
    assert "id-token" not in admit["permissions"]
    assert gate["needs"] == "admit"
    assert gate["permissions"]["id-token"] == "write"
    assert set(gate["with"]) == {
        "request_descriptor",
        "request_descriptor_sha256",
        "contract_repository",
        "contract_source_sha",
        "contract_path",
        "contract_sha256",
        "expected_source_sha",
        "purpose",
    }
    assert gate["uses"].endswith("@97f8e7e086e3f107401b5eb35ff4017dde21ff34")
    assert "secrets" not in gate


@pytest.mark.parametrize("mode", ["plan", "apply"])
@pytest.mark.parametrize("case", ["valid", "fork", "branch", "source"])
def test_guard_do_caller_executa_contexto_exato_antes_do_checkout(mode, case):
    guard = workflow(mode)["jobs"]["admit"]["steps"][0]
    env = dict(
        os.environ,
        GITHUB_REPOSITORY="VINIClUS/CnesData",
        GITHUB_REF="refs/heads/develop",
        GITHUB_SHA="a" * 40,
        GITHUB_EVENT_NAME="workflow_dispatch",
        EXPECTED_SOURCE_SHA="a" * 40,
    )
    if case == "fork":
        env["GITHUB_REPOSITORY"] = "other/CnesData"
    elif case == "branch":
        env["GITHUB_REF"] = "refs/heads/main"
    elif case == "source":
        env["EXPECTED_SOURCE_SHA"] = "b" * 40
    bash = which("bash")
    assert bash is not None
    result = subprocess.run(  # noqa: S603
        [bash, "-c", guard["run"]], env=env, capture_output=True, check=False
    )
    assert (result.returncode == 0) == (case == "valid")


def test_cli_sem_produtor_liberado_fecha_sem_output(monkeypatch, tmp_path):
    monkeypatch.setattr(verify_plan_artifact, "PRODUCER_READY", False)
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "outputs"))
    monkeypatch.setattr("sys.argv", ["verify_plan_artifact", "plan"])
    with pytest.raises(SystemExit) as result:
        admission().main()
    assert result.value.code == 2
    assert not (tmp_path / "outputs").exists()


def test_apply_phase_a_confere_transporte_original_dos_inputs():
    value, api = phase_apply()
    assert admission().prepare_request(value, api, NOW)["purpose"] == "phase-a"
    value["phase_inputs_run_id"] = "201"
    with pytest.raises(ValueError):
        admission().prepare_request(value, api, NOW)


def test_apply_nao_estende_plan_alem_da_assinatura_free():
    value, api = request("apply", "phase-a")
    payload = json.loads(value["request_descriptor"])["payload_artifact"]
    index = transport(payload, "phase-a")
    index["cost"]["cloudfront"]["expires_at"] = "2026-11-09T01:00:00Z"
    replace_payload(value, api, index)
    with pytest.raises(ValueError):
        admission().prepare_request(value, api, NOW)


def test_plan_recusa_transportes_de_apply_fora_de_contexto():
    value, api = request()
    value["artifact_id"] = "2000"
    with pytest.raises(ValueError):
        admission().prepare_request(value, api, NOW)


def test_actionlint_fixado_valida_os_dois_callers():
    executable = which("actionlint")
    assert executable is not None, "delivery_tool_missing=actionlint"
    version = subprocess.run(  # noqa: S603
        [executable, "--version"], capture_output=True, text=True, check=True
    )
    assert version.stdout.splitlines()[0] == "1.7.7"
    result = subprocess.run(  # noqa: S603
        [executable, *map(str, WORKFLOWS.values())], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "case",
    [
        "product",
        "aggregate",
        "paid",
        "files",
        "dependencies",
        "retained",
        "fingerprints",
    ],
)
def test_schema_preserva_limites_de_custo_e_colecoes(case):
    value = evidence()
    plan = value["plan"]
    if case in {"product", "aggregate", "paid"}:
        field, amount = {
            "product": ("product_monthly_usd", "8.01"),
            "aggregate": ("aggregate_monthly_usd", "15.00"),
            "paid": ("paid_actions_limit_usd", "0.01"),
        }[case]
        plan["cost"][field] = amount
    elif case == "files":
        plan["package"]["files"] = [
            {"name": f"file{i}", "size": 1, "sha256": "a" * 64} for i in range(33)
        ]
    elif case == "dependencies":
        plan["dependencies"] = [
            {"repository": f"repo{i}", "source_sha": "a" * 40, "content_sha256": "b" * 64}
            for i in range(33)
        ]
    elif case == "retained":
        plan["phase_a_inputs"]["processor_retained_unit_task_definition_arns"] = [
            f"arn:aws:ecs:us-east-2:123456789012:task-definition/unit:{i}" for i in range(129)
        ]
    else:
        value["apply"]["outputs"]["resource_fingerprints"] = [
            {"address": f"resource{i}", "fingerprint_sha256": "a" * 64} for i in range(65)
        ]
    assert validate(value, json.loads(SCHEMA.read_bytes()))


def test_schema_e_consumidor_integrado_concordam_no_a3_phase_a_sintetico():
    bundle, governed = application()
    result = verify_application(bundle, governed, APPLICATION_NOW)
    schema = json.loads(SCHEMA.read_bytes())
    for status in ("planned", "applied"):
        assert validate(json.loads(bundle[status]), schema) == []
    assert result["plan"] == json.loads(bundle["planned"])["plan"]


@pytest.mark.parametrize("mode", ["plan", "apply"])
def test_cli_liberado_autentica_e_emite_os_oito_inputs(mode, monkeypatch, tmp_path):
    value, api = request(mode, "bootstrap")
    for key in ("mode", "run_id", "run_attempt"):
        value.pop(key)
    for key, item in {
        "GITHUB_REPOSITORY": "VINIClUS/CnesData",
        "GITHUB_REF": "refs/heads/develop",
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "GITHUB_RUN_ID": "900",
        "GITHUB_RUN_ATTEMPT": "2",
        "GITHUB_OUTPUT": str(tmp_path / "outputs"),
        "REQUEST_JSON": json.dumps(value),
    }.items():
        monkeypatch.setenv(key, item)
    monkeypatch.setattr("sys.argv", ["verify_plan_artifact", mode])
    monkeypatch.setattr(verify_plan_artifact, "PublicGitHub", lambda: api)
    monkeypatch.setattr(verify_plan_artifact, "datetime", SimpleNamespace(now=lambda _: NOW))
    verify_plan_artifact.main()
    output = (tmp_path / "outputs").read_text()
    assert {line.split("<<")[0] for line in output.splitlines() if "<<" in line} == {
        "request_descriptor",
        "request_descriptor_sha256",
        "expected_source_sha",
        "purpose",
        "contract_repository",
        "contract_source_sha",
        "contract_path",
        "contract_sha256",
    }
    assert value["request_descriptor"] in output
    assert "bootstrap" in output


def test_produtor_antigo_e_recusado_mesmo_com_descriptor_recalculado():
    from tests.production.delivery._plan_apply_fixtures import canonical

    value, api = request()
    descriptor = json.loads(value["request_descriptor"])
    descriptor["producer_pins"][0]["source_sha"] = "99fc0e4805b96fbca7df06440d5ea3ad42e53369"
    value["request_descriptor"] = canonical(descriptor).decode()
    value["request_descriptor_sha256"] = hashlib.sha256(canonical(descriptor)).hexdigest()
    with pytest.raises(ValueError, match="descriptor=producer_pin"):
        verify_plan_artifact.prepare_request(value, api, NOW)


def test_cronologia_do_transporte_novo_sucede_release_real_do_produtor():
    _, api = request()
    run = api.responses["VINIClUS/infra-ansible", "actions/runs/1/attempts/1"]
    artifact = api.responses["VINIClUS/infra-ansible", "actions/artifacts/1001"]
    assert (
        timestamp("2026-11-08T23:47:55Z")
        < timestamp(run["run_started_at"])
        < timestamp(artifact["created_at"])
        < timestamp(run["updated_at"])
        < NOW
        < timestamp(artifact["expires_at"])
    )
