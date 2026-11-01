import hashlib
import re
import subprocess
from typing import Any, cast

import pytest
import yaml

from tests.production.infra._athena_contracts import (
    BILLED_QUERIES_PER_MONTH,
    OVERSHOOT_QUERIES,
    REPO_ROOT,
)
from tests.production.infra.conftest import require_tool

PROD = REPO_ROOT / "infra" / "opentofu" / "env" / "prod"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "infra-native.yml"
CONFTEST_VERSION = "0.71.1"
CONFTEST_SHA256 = "c3f6b2a753bd56e377a1e51a95ae3e057f926f2e9139f7d5b5dfbbaf810f7985"
AWS_LOCK_BLOCK_SHA256 = "97d8a5b4cc70cce4ae2578d2f8b2c9534ea0e851f328c1ff97f4357c007a00c9"
PLATFORM_HASHES = 15
NEW_PATHS = ("policies/**", "schemas/**", "tests/fixtures/**")
RUNBOOK = REPO_ROOT / "docs" / "runbooks" / "athena-operator.md"
ALARM_STATE = (
    "aws cloudwatch describe-alarms --alarm-names cnesdata-prod-athena-cutoff "
    "--query 'MetricAlarms[0].StateValue' --output text"
)
WORKGROUP_STATE = "aws athena update-work-group --work-group cnesdata-prod-operator --state"
CUTOFF_PATH_ALARMS = (
    "cnesdata-prod-cutoff-lambda-errors", "cnesdata-prod-cutoff-sns-delivery-failures",
)


def _lock_block(provider: str) -> str:
    text = (PROD / ".terraform.lock.hcl").read_text(encoding="utf-8")
    header = f'provider "registry.opentofu.org/hashicorp/{provider}"'
    match = re.search(rf"{re.escape(header)} \{{.*?\n\}}\n", text, re.S)
    assert match is not None, f"lock_entry_missing provider={provider}"
    return match.group(0)


def _runbook() -> str:
    return re.sub(r"\\\n\s*", "", RUNBOOK.read_text(encoding="utf-8"))


def _workflow() -> dict[str, object]:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_lock_preserva_a_entrada_aws_e_acrescenta_archive_nas_mesmas_plataformas() -> None:
    aws = _lock_block("aws")
    assert hashlib.sha256(aws.encode()).hexdigest() == AWS_LOCK_BLOCK_SHA256
    archive = _lock_block("archive")
    assert archive.count('"h1:') == aws.count('"h1:') == PLATFORM_HASHES
    assert archive.count('"zh:') == aws.count('"zh:') == PLATFORM_HASHES
    versions = (PROD / "versions.tf").read_text(encoding="utf-8")
    assert 'source  = "hashicorp/archive"' in versions


def test_ci_instala_conftest_fixado_so_no_job_hospedado_nativo() -> None:
    workflow = _workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    native = jobs["opentofu-native"]
    assert native["runs-on"] == "ubuntu-latest"
    steps = [step for step in native["steps"] if "conftest" in step.get("name", "").lower()]
    assert [step["name"] for step in steps] == ["Install conftest"]
    step = steps[0]
    assert step["env"]["CONFTEST_VERSION"] == CONFTEST_VERSION
    assert step["env"]["CONFTEST_SHA256"] == CONFTEST_SHA256
    assert "sha256sum -c" in step["run"]
    assert "conftest_${CONFTEST_VERSION}_Linux_x86_64.tar.gz" in step["run"]
    assert 'tar -xzf "${archive}" -C "${RUNNER_TEMP}/conftest-bin" conftest' in step["run"]
    for name, job in jobs.items():
        if name != "opentofu-native":
            assert "conftest" not in yaml.safe_dump(job).lower(), f"job={name}"
        assert "self-hosted" not in str(job["runs-on"]), f"job={name}"


def test_ci_dispara_nos_novos_caminhos_de_politica_schema_e_fixtures() -> None:
    workflow = cast("dict[Any, Any]", _workflow())
    triggers = workflow.get("on", workflow.get(True))
    assert isinstance(triggers, dict)
    for event in ("pull_request", "push"):
        for path in NEW_PATHS:
            assert path in triggers[event]["paths"], f"event={event} path={path}"


def test_conftest_local_e_a_versao_fixada() -> None:
    executable = require_tool("conftest")
    done = subprocess.run(  # noqa: S603
        [executable, "--version"], check=False, capture_output=True, text=True
    )
    assert f"Conftest: {CONFTEST_VERSION}" in done.stdout


def test_guia_de_desenvolvimento_documenta_o_conftest() -> None:
    text = (REPO_ROOT / "docs" / "development.md").read_text(encoding="utf-8")
    assert "conftest" in text
    assert CONFTEST_VERSION in text


@pytest.mark.parametrize("path", ["policies/cnesdata.rego", "schemas/cost-manifest.schema.json"])
def test_artefatos_de_politica_existem(path: str) -> None:
    assert (REPO_ROOT / path).is_file()


def test_runbook_so_reativa_com_o_alarme_de_cutoff_em_ok() -> None:
    text = _runbook()
    enable = text.index(f"{WORKGROUP_STATE} ENABLED")
    gate = text.index(ALARM_STATE)
    assert gate < enable
    assert "`OK`" in text[gate:enable]


def test_runbook_desabilita_a_mao_quando_o_caminho_do_cutoff_falha() -> None:
    text = _runbook()
    assert f"{WORKGROUP_STATE} DISABLED" in text
    for alarm in CUTOFF_PATH_ALARMS:
        assert f"`{alarm}`" in text, f"alarm={alarm}"


def test_runbook_declara_as_premissas_do_teto_mensal() -> None:
    text = _runbook()
    assert f"k = {OVERSHOOT_QUERIES}" in text
    assert f"{BILLED_QUERIES_PER_MONTH} queries cobradas" in text
