"""Workflow phase-a-prepare: roda a CLI Phase A só em runner hospedado, sob OIDC e sem S3."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any

import yaml

from tests.production.delivery.conftest import require_tool

ROOT = Path(__file__).resolve().parents[3]
PATH = ROOT / ".github/workflows/phase-a-prepare.yml"
ALLOWED_ACTIONS = {
    "actions/checkout",
    "actions/upload-artifact",
    "astral-sh/setup-uv",
    "actions/setup-python",
}


def _text() -> str:
    assert PATH.is_file(), "workflow_missing=phase-a-prepare.yml"
    return PATH.read_text(encoding="utf-8")


def _workflow() -> dict[Any, Any]:
    return yaml.safe_load(_text())


def _job() -> dict[str, Any]:
    jobs = _workflow()["jobs"]
    assert len(jobs) == 1
    return next(iter(jobs.values()))


def _steps() -> list[dict[str, Any]]:
    return _job()["steps"]


def _runs() -> str:
    return "\n".join(step.get("run", "") for step in _steps())


def test_dispara_so_por_workflow_dispatch_com_inputs_fechados():
    trigger = _workflow()["on"]
    assert list(trigger) == ["workflow_dispatch"]
    assert set(trigger["workflow_dispatch"]["inputs"]) == {
        "reference",
        "creator",
        "baseline",
        "source_sha",
        "canary_trusted_principal_arn",
    }


def test_job_usa_environment_production_e_so_roda_em_main():
    assert _job()["environment"] == "production"
    assert _job()["if"] == "github.ref == 'refs/heads/main'"


def test_permissoes_do_job_sao_exatamente_leitura_oidc_e_packages_read():
    assert _workflow()["permissions"] == {}
    assert _job()["permissions"] == {
        "contents": "read",
        "id-token": "write",
        "packages": "read",
        "actions": "read",
        "checks": "read",
    }


def test_runner_e_hospedado_e_nunca_self_hosted():
    assert _job()["runs-on"] == "ubuntu-latest"
    assert "self-hosted" not in _text()


def test_actions_fixadas_por_sha_completo_e_sem_acao_de_credencial_de_terceiros():
    uses = [step["uses"] for step in _steps() if "uses" in step]
    assert uses
    for item in uses:
        assert re.fullmatch(r"[\w/-]+@[0-9a-f]{40}", item), item
        assert item.split("@")[0] in ALLOWED_ACTIONS


def test_inputs_chegam_aos_comandos_por_env_e_nunca_por_interpolacao():
    assert "${{ inputs." not in _runs()
    assert "${{ github." not in _runs()
    assert "secrets." not in _text()


def test_chama_as_duas_clis_pelo_modulo_com_ambiente_travado_por_lock():
    commands = _runs()
    assert "uv sync --locked --all-packages" in commands
    module = "uv run --no-sync python -m scripts.promotion.phase_a_cli"
    order = ("promote-ecr", "phase-inputs", "stage-private", "upload-private")
    for command in order:
        assert f"{module} {command}" in commands
    assert [commands.index(command) for command in order] == sorted(
        commands.index(command) for command in order
    )


def test_assume_so_as_duas_roles_esperadas_em_us_east_2_com_audience_sts():
    commands = _runs()
    assert "role/cnesdata-prod-ecr-promotion" in _text()
    assert "role/cnesdata-prod-phase-inputs-writer" in _text()
    assert sorted(re.findall(r"role/([\w-]+)", _text())) == [
        "cnesdata-prod-ecr-promotion",
        "cnesdata-prod-phase-inputs-writer",
    ]
    assert commands.count("audience=sts.amazonaws.com") == 2
    assert "AWS_WEB_IDENTITY_TOKEN_FILE" in _text()
    assert "AWS_REGION: us-east-2" in _text()


def test_token_do_ghcr_vem_do_job_e_o_token_oidc_e_mascarado():
    text = _text()
    assert "GITHUB_TOKEN: ${{ github.token }}" in text
    assert "::add-mask::" in _runs()


def test_artefato_publico_tem_so_phase_a_public_e_retencao_de_30_dias():
    uploads = [step for step in _steps() if "upload-artifact" in step.get("uses", "")]
    assert len(uploads) == 1
    options = uploads[0]["with"]
    assert options["name"] == "phase-a-prepare-${{ github.run_id }}-${{ github.run_attempt }}"
    assert options["path"].strip() == "${{ runner.temp }}/phase-a/phase-a-public.json"
    assert options["if-no-files-found"] == "error"
    assert options["retention-days"] == 30
    assert uploads[0] == _steps()[-1]


def test_nao_ha_caminho_de_upload_local_nem_cli_do_aws_nem_chave_de_ensaio():
    text = _text()
    assert not re.search(r"\baws s3\b|s3://|s3api|aws-cli", text)
    assert "rehearsal" not in text
    assert "rehearsal_plan" not in text


def test_variaveis_de_evidencia_vem_do_environment_e_o_role_de_escrita_so_no_upload():
    steps = _steps()
    upload = next(s for s in steps if "upload-private" in s.get("run", ""))
    assert upload["env"] == {
        "EVIDENCE_BUCKET": "${{ vars.PRODUCTION_EVIDENCE_BUCKET }}",
        "EVIDENCE_PREFIX": "${{ vars.PRODUCTION_EVIDENCE_PREFIX }}",
        "EVIDENCE_KMS_KEY_ARN": "${{ vars.PRODUCTION_EVIDENCE_KMS_KEY_ARN }}",
        "ACCOUNT_ID": "${{ vars.PRODUCTION_AWS_ACCOUNT_ID }}",
        "SOURCE_SHA": "${{ inputs.source_sha }}",
        "RUN_ID": "${{ github.run_id }}",
        "RUN_ATTEMPT": "${{ github.run_attempt }}",
    }
    writer = next(i for i, s in enumerate(steps) if "phase-inputs-writer" in s.get("run", ""))
    assert steps.index(upload) > writer
    promote = next(i for i, s in enumerate(steps) if "promote-ecr" in s.get("run", ""))
    assert promote < writer
    assert "phase-inputs-writer" not in steps[promote]["run"]


def test_actionlint_valida_o_workflow():
    done = subprocess.run(  # noqa: S603 -- Validates the checked-in workflow with fixed actionlint.
        [require_tool("actionlint"), str(PATH)],
        capture_output=True,
        text=True,
        check=False,
        stdin=subprocess.DEVNULL,
    )
    assert done.returncode == 0, done.stdout


def test_cada_input_alimenta_a_variavel_de_ambiente_do_mesmo_nome():
    expected = {
        "REFERENCE": "reference",
        "CREATOR": "creator",
        "BASELINE": "baseline",
        "SOURCE_SHA": "source_sha",
        "CANARY_PRINCIPAL": "canary_trusted_principal_arn",
    }
    seen = {
        name: value
        for step in _steps()
        for name, value in step.get("env", {}).items()
        if name in expected
    }
    assert seen == {name: "${{ inputs." + key + " }}" for name, key in expected.items()}


def test_argumentos_das_clis_ligam_cada_arquivo_e_variavel_ao_parametro_certo():
    commands = _runs()
    for argument in (
        '--reference "$out/reference.json"',
        '--creator "$out/creator.json"',
        '--account-id "$ACCOUNT_ID"',
        '--region "$AWS_REGION"',
        '--output "$out/promotion.json"',
        '--promotion "$out/promotion.json"',
        '--baseline "$out/baseline.json"',
        '--source-sha "$SOURCE_SHA"',
        '--canary-trusted-principal-arn "$CANARY_PRINCIPAL"',
        '--output "$out/phase-inputs.json"',
    ):
        assert argument in commands
    for variable, name in (
        ("REFERENCE", "reference"),
        ("CREATOR", "creator"),
        ("BASELINE", "baseline"),
    ):
        assert f'printf \'%s\' "${variable}" > "$RUNNER_TEMP/phase-a/{name}.json"' in commands


def test_account_id_vem_so_de_vars_do_ambiente_e_nunca_de_input_ou_texto():
    text = _text()
    assert "inputs.account_id" not in text
    assert not re.search(r"(?<![0-9a-zA-Z])[0-9]{12}(?![0-9a-zA-Z])", text)
    seen = [step["env"]["ACCOUNT_ID"] for step in _steps() if "ACCOUNT_ID" in step.get("env", {})]
    assert len(seen) == 5
    assert set(seen) == {"${{ vars.PRODUCTION_AWS_ACCOUNT_ID }}"}
    assert "vars." not in _runs()
    for step in _steps():
        if "env" in step:
            assert all(
                "vars." not in value or name.startswith(("ACCOUNT", "EVIDENCE"))
                for name, value in step["env"].items()
            )


def test_regex_de_12_digitos_precede_todo_uso_do_account_id():
    commands = _runs()
    check = '[[ "$ACCOUNT_ID" =~ ^[0-9]{12}$ ]]'
    assert check in commands
    assert commands.index(check) < commands.index('"$ACCOUNT_ID"', commands.index(check) + 1)
    first = next(step for step in _steps() if "ACCOUNT_ID" in step.get("env", {}))
    assert check in first["run"]
    assert first["run"].index(check) < first["run"].rindex("$RUNNER_TEMP")
