"""Contrato do workflow de CI que roda a suíte nativa OpenTofu e o piso de versão."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW = _ROOT / ".github/workflows/infra-native.yml"
_VERSIONS_TF = _ROOT / "infra/opentofu/env/prod/versions.tf"
_SETUP_OPENTOFU = "opentofu/setup-opentofu@9d84900f3238fab8cd84ce47d658d25dd008be2f"
_NATIVE = "opentofu-native"
_FLOOR = "opentofu-floor"
_NATIVE_VERSION = "1.13.2"
_FLOOR_VERSION = "1.10.0"
_TRIVY_VERSION = "0.74.0"
_TRIVY_SHA256 = "2ae6fe3ee734b7fdf11335663e18c75ea12dccc76062f09f164a3b0f8be4371a"
_SUITE = "Run OpenTofu native suite"
_SUITE_COMMAND = (
    "timeout --signal=INT --kill-after=15s 1800 "
    "uv run --no-build pytest -q tests/production/infra --junitxml=infra-native.xml"
)
_WARM = "Warm provider cache"
_PROD_INIT = (
    "tofu -chdir=infra/opentofu/env/prod init -backend=false -input=false -lockfile=readonly"
)
_LOCK = "infra/opentofu/env/prod/.terraform.lock.hcl"
_TRIGGER_PATHS = [
    "infra/**",
    "tests/production/infra/**",
    "pyproject.toml",
    "uv.lock",
    ".github/workflows/infra-native.yml",
]
_MAX_JOB_MINUTES = 40
_BIND = "Bind processing sources"
_BINDINGS_ENV = {
    "PROCESSING_SOURCE_BINDINGS": "${{ runner.temp }}/processing-source-bindings.json"
}
_SOURCE_ROOTS = (
    ("apps/data_processor/src", "data_processor"),
    ("packages/cnes_contracts/src", "cnes_contracts"),
    ("packages/cnes_domain/src", "cnes_domain"),
    ("packages/cnes_infra/src", "cnes_infra"),
)


def _workflow() -> dict[Any, Any]:
    return yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))


def _events() -> dict[str, Any]:
    workflow = _workflow()
    return cast("dict[str, Any]", workflow.get("on", workflow.get(True)))


def _job(name: str) -> dict[str, Any]:
    return _workflow()["jobs"][name]


def _step(job: str, name: str) -> dict[str, Any]:
    return next(step for step in _job(job)["steps"] if step.get("name") == name)


def _names(job: str) -> list[str]:
    return [step.get("name", "") for step in _job(job)["steps"]]


def _setup_opentofu(job: str) -> dict[str, Any]:
    steps = [step for step in _job(job)["steps"] if "setup-opentofu" in step.get("uses", "")]
    assert len(steps) == 1, f"setup_opentofu_steps={len(steps)} job={job}"
    return steps[0]


def _command(step: dict[str, Any]) -> str:
    return " ".join(step["run"].split())


def test_workflow_dispara_em_pr_e_push_somente_nos_caminhos_de_infra() -> None:
    events = _events()

    assert events["pull_request"] == {"paths": _TRIGGER_PATHS}
    assert events["push"] == {"branches": ["develop", "main"], "paths": _TRIGGER_PATHS}
    assert "workflow_dispatch" in events
    assert set(events) == {"pull_request", "push", "workflow_dispatch"}


def test_workflow_cancela_execucao_anterior_do_mesmo_ref() -> None:
    concurrency = _workflow()["concurrency"]

    assert concurrency == {
        "group": "infra-native-${{ github.ref }}",
        "cancel-in-progress": True,
    }


def test_jobs_rodam_em_runner_hospedado_com_permissao_somente_leitura() -> None:
    workflow = _workflow()

    assert workflow["permissions"] == {"contents": "read"}
    assert set(workflow["jobs"]) == {_NATIVE, _FLOOR}
    for name, job in workflow["jobs"].items():
        assert job["runs-on"] == "ubuntu-latest", name
        assert job["permissions"] == {"contents": "read"}, name
        assert "self-hosted" not in str(job), name


def test_jobs_nao_usam_secrets_credenciais_aws_nem_environment() -> None:
    text = _WORKFLOW.read_text(encoding="utf-8")

    assert "secrets." not in text
    assert "id-token" not in text
    assert "AWS_" not in text
    for job in _workflow()["jobs"].values():
        assert "environment" not in job


def test_jobs_e_passos_tem_timeout_finito() -> None:
    for name, job in _workflow()["jobs"].items():
        minutes = job["timeout-minutes"]
        assert isinstance(minutes, int), name
        assert 0 < minutes <= _MAX_JOB_MINUTES, name
    suite = _step(_NATIVE, _SUITE)
    assert 0 < suite["timeout-minutes"] < _job(_NATIVE)["timeout-minutes"]
    assert suite["run"].startswith("timeout --signal=INT --kill-after=15s 1800 ")
    for name in ("Install Trivy", _WARM):
        assert 0 < _step(_NATIVE, name)["timeout-minutes"] <= 5, name


def test_falhas_dos_jobs_nao_sao_mascaradas_por_continue_on_error() -> None:
    assert "continue-on-error" not in _WORKFLOW.read_text(encoding="utf-8")


def test_setup_opentofu_fixado_por_sha_de_release_nos_dois_jobs() -> None:
    for job in (_NATIVE, _FLOOR):
        assert _setup_opentofu(job)["uses"] == _SETUP_OPENTOFU


def test_job_nativo_instala_opentofu_1_13_2_sem_wrapper() -> None:
    assert _setup_opentofu(_NATIVE)["with"] == {
        "tofu_version": _NATIVE_VERSION,
        "tofu_wrapper": False,
    }


def test_job_piso_instala_opentofu_1_10_0_sem_wrapper() -> None:
    assert _setup_opentofu(_FLOOR)["with"] == {
        "tofu_version": _FLOOR_VERSION,
        "tofu_wrapper": False,
    }


def test_piso_do_ci_acompanha_required_version_do_ambiente_prod() -> None:
    match = re.search(
        r'required_version\s*=\s*">=\s*([0-9.]+)"', _VERSIONS_TF.read_text(encoding="utf-8")
    )

    assert match is not None
    assert match[1] == _FLOOR_VERSION


def test_job_piso_roda_somente_fmt_init_e_validate() -> None:
    runs = [_command(step) for step in _job(_FLOOR)["steps"] if "run" in step]

    assert runs == [
        "tofu -chdir=infra/opentofu fmt -check -recursive",
        f"timeout --signal=INT --kill-after=15s 240 {_PROD_INIT}",
        "timeout --signal=INT --kill-after=15s 240 tofu -chdir=infra/opentofu/env/prod validate",
    ]
    assert all(step.get("timeout-minutes") for step in _job(_FLOOR)["steps"] if "run" in step)


def test_job_nativo_mantem_cache_de_providers_no_workspace_chaveado_pelo_lock() -> None:
    job = _job(_NATIVE)
    cache = _step(_NATIVE, "Cache OpenTofu providers")

    assert job["env"]["TF_PLUGIN_CACHE_DIR"] == "${{ github.workspace }}/.tofu-plugin-cache"
    assert cache["uses"] == "actions/cache@v4"
    assert cache["with"]["path"] == "${{ env.TF_PLUGIN_CACHE_DIR }}"
    assert f"hashFiles('{_LOCK}')" in cache["with"]["key"]


def test_job_nativo_cria_e_aquece_cache_antes_da_suite() -> None:
    names = _names(_NATIVE)
    warm = _step(_NATIVE, _WARM)

    assert _step(_NATIVE, "Create provider cache directory")["run"] == (
        'mkdir -p "${TF_PLUGIN_CACHE_DIR}"'
    )
    assert _command(warm) == f"timeout --signal=INT --kill-after=15s 240 {_PROD_INIT}"
    assert warm["env"] == {"TF_DATA_DIR": "${{ runner.temp }}/tofu-warm"}
    assert names.index("Create provider cache directory") < names.index(
        "Cache OpenTofu providers"
    ) < names.index(_WARM) < names.index(_SUITE)


def test_job_nativo_instala_trivy_fixado_por_versao_e_checksum() -> None:
    trivy = _step(_NATIVE, "Install Trivy")

    assert trivy["env"] == {"TRIVY_VERSION": _TRIVY_VERSION, "TRIVY_SHA256": _TRIVY_SHA256}
    assert "sha256sum -c -" in trivy["run"]
    assert "curl -fsSL --proto '=https'" in trivy["run"]
    assert _names(_NATIVE).index("Install Trivy") < _names(_NATIVE).index(_SUITE)


def _bind_script() -> str:
    run = _step(_NATIVE, _BIND)["run"]
    assert run.startswith("python -I - <<'EOF'\n"), "bind_heredoc"
    return run.split("<<'EOF'\n", 1)[1].rsplit("EOF", 1)[0]


def test_job_nativo_vincula_fontes_de_processamento_antes_da_suite() -> None:
    names = _names(_NATIVE)

    assert _step(_NATIVE, _BIND)["env"] == _BINDINGS_ENV
    assert _step(_NATIVE, _SUITE)["env"] == _BINDINGS_ENV
    assert names.index("Install dependencies") < names.index(_BIND) < names.index(_SUITE)


def test_vinculo_cobre_raizes_na_ordem_dos_namespaces_e_todo_fonte_python(
    tmp_path: Path,
) -> None:
    target = tmp_path / "bindings.json"

    subprocess.run(  # noqa: S603
        [sys.executable, "-I", "-c", _bind_script()],
        check=True,
        cwd=_ROOT,
        env=os.environ | {"PROCESSING_SOURCE_BINDINGS": str(target)},
    )
    binding = json.loads(target.read_text(encoding="utf-8"))
    expected = {
        str(path.resolve()): sha256(path.read_bytes()).hexdigest()
        for root, name in _SOURCE_ROOTS
        for path in (_ROOT / root / name).rglob("*.py")
    }

    assert binding["roots"] == [str((_ROOT / root).resolve()) for root, _ in _SOURCE_ROOTS]
    assert binding["files"] == expected
    manifests = _ROOT / "packages/cnes_contracts/src/cnes_contracts/manifests/processing.py"
    assert str(manifests.resolve()) in binding["files"]


def test_job_nativo_roda_suite_infra_completa_com_junit() -> None:
    suite = _step(_NATIVE, _SUITE)

    assert _job(_NATIVE)["env"]["CI"] is True
    assert "uv sync --locked --all-packages" in [step.get("run") for step in _job(_NATIVE)["steps"]]
    assert _command(suite) == _SUITE_COMMAND
    assert "--ignore" not in suite["run"]
    assert " -m " not in suite["run"]


def test_job_nativo_publica_e_valida_junit_completo() -> None:
    names = _names(_NATIVE)
    upload = _step(_NATIVE, "Upload JUnit report")
    validate = _step(_NATIVE, "Validate native report")

    assert upload["if"] == "always()"
    assert upload["with"]["path"] == "infra-native.xml"
    assert "require_complete_junit" in validate["run"]
    assert "Path('infra-native.xml')" in validate["run"]
    assert names.index(_SUITE) < names.index("Upload JUnit report")
    assert names.index(_SUITE) < names.index("Validate native report")


def test_job_nativo_valida_o_proprio_contrato_antes_da_suite() -> None:
    names = _names(_NATIVE)
    contract = _step(_NATIVE, "OpenTofu runner contract")

    assert "pytest -q tests/scripts/test_ci_opentofu_runner.py" in contract["run"]
    assert names.index("OpenTofu runner contract") < names.index(_SUITE)
