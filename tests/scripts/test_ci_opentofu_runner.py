"""Contrato do workflow de CI que roda a suíte nativa OpenTofu e o piso de versão."""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest
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
    "policies/**",
    "schemas/**",
    "scripts/promotion/**",
    "release/verify_candidate.py",
    "release/verify_activation.py",
    "release/build_candidate.py",
    "release/plan-apply-evidence.schema.json",
    ".github/workflows/infra-plan.yml",
    ".github/workflows/infra-apply.yml",
    "tests/production/delivery/test_plan_apply_workflows.py",
    "tests/production/delivery/_plan_apply_fixtures.py",
    "tests/production/delivery/_phase_a_application_fixtures.py",
    "tests/production/delivery/_phase_a_fixtures.py",
    "apps/web_dashboard/bun.lock",
    "tests/fixtures/**",
    "tests/production/infra/**",
    "tests/production/test_runtime_acceptance.py",
    "tests/scripts/test_ci_opentofu_runner.py",
    "tests/integration/aws_capability/_fargate_mirror.py",
    "tests/integration/aws_capability/_matrix_policies.py",
    "apps/data_processor/Dockerfile",
    "apps/data_processor/src/**",
    "apps/web_dashboard/src/auth/oidcSettings.ts",
    "apps/web_dashboard/src/lib/env.ts",
    "packages/cnes_contracts/src/**",
    "packages/cnes_domain/src/**",
    "packages/cnes_infra/src/**",
    "packages/cnes_infra/tests/aws/production_values.py",
    "packages/cnes_infra/tests/fixtures/step_functions/**",
    "docs/development.md",
    "docs/runbooks/athena-operator.md",
    "docs/superpowers/specs/2026-11-01-athena-observability-cost-amendment.md",
    "conftest.py",
    "pyproject.toml",
    "pytest.ini",
    "uv.lock",
    ".gitignore",
    ".github/CODEOWNERS",
    ".github/workflows/deploy-main.yml",
    ".github/workflows/infra-native.yml",
]
_DETECT = "Detect infrastructure changes"
_GATE = "steps.changes.outputs.infra == 'true'"
_MAX_JOB_MINUTES = 40
_BIND = "Bind processing sources"
_PRODUCER = "tests/production/infra/bind_processing_sources.py"
_BIND_COMMAND = "python -I " + _PRODUCER + ' "${PROCESSING_SOURCE_BINDINGS}"'
_DOCS = _ROOT / "docs/development.md"
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


def _gated(step: dict[str, Any]) -> bool:
    return step.get("name") != _DETECT


def test_workflow_dispara_em_todo_pr_e_filtra_somente_o_push() -> None:
    events = _events()

    assert events["pull_request"] is None
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
    runs = [_command(step) for step in _job(_FLOOR)["steps"] if "run" in step and _gated(step)]

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


def _produce(producer: Path, cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        [sys.executable, "-I", str(producer), *args],
        check=False,
        capture_output=True,
        cwd=cwd,
        text=True,
    )


def test_job_nativo_vincula_fontes_de_processamento_antes_da_suite() -> None:
    names = _names(_NATIVE)
    bind = _step(_NATIVE, _BIND)

    assert _command(bind) == _BIND_COMMAND
    assert "<<" not in bind["run"]
    assert bind["env"] == _BINDINGS_ENV
    assert _step(_NATIVE, _SUITE)["env"] == _BINDINGS_ENV
    assert names.index("Install dependencies") < names.index(_BIND) < names.index(_SUITE)


def test_documentacao_local_gera_vinculo_com_o_mesmo_produtor() -> None:
    docs = _DOCS.read_text(encoding="utf-8")

    assert f"uv run {_BIND_COMMAND}\n" in docs
    assert "_bind_script" not in docs


@pytest.mark.parametrize("origem", ["raiz", "fora_do_repositorio"])
def test_vinculo_cobre_raizes_na_ordem_dos_namespaces_e_todo_fonte_python(
    tmp_path: Path, origem: str
) -> None:
    target = tmp_path / "bindings.json"
    cwd = _ROOT if origem == "raiz" else tmp_path

    result = _produce(_ROOT / _PRODUCER, cwd, str(target))

    assert result.returncode == 0, result.stderr
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


def test_produtor_exige_destino_explicito(tmp_path: Path) -> None:
    result = _produce(_ROOT / _PRODUCER, tmp_path)

    assert result.returncode != 0
    assert "usage=bind_processing_sources.py" in result.stderr
    assert not list(tmp_path.iterdir())


def test_produtor_recusa_raiz_de_fontes_ausente(tmp_path: Path) -> None:
    producer = tmp_path / _PRODUCER
    producer.parent.mkdir(parents=True)
    shutil.copyfile(_ROOT / _PRODUCER, producer)
    target = tmp_path / "bindings.json"

    result = _produce(producer, tmp_path, str(target))

    assert result.returncode != 0
    assert "processing_source_root=missing" in result.stderr
    assert not target.exists()


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

    assert upload["if"] == f"always() && {_GATE}"
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


def _detection(job: str) -> dict[str, Any]:
    return _step(job, _DETECT)


def _patterns(job: str) -> list[str]:
    body = re.search(r"patterns=\(\n(.*?)\n\s*\)", _detection(job)["run"], re.DOTALL)
    assert body is not None
    return re.findall(r"'([^']+)'", body[1])


@pytest.mark.parametrize("job", [_NATIVE, _FLOOR])
def test_deteccao_de_infra_e_o_primeiro_passo_apos_o_checkout_raso(job: str) -> None:
    steps = _job(job)["steps"]
    detection = _detection(job)

    assert steps[0]["uses"].startswith("actions/checkout@")
    assert steps[0]["with"] == {"persist-credentials": False, "fetch-depth": 2}
    assert steps[1] == detection
    assert 0 < detection["timeout-minutes"] <= 2
    assert detection["id"] == "changes"
    assert detection["env"] == {"EVENT_NAME": "${{ github.event_name }}"}
    assert "${{" not in detection["run"]
    assert "name" not in _job(job)
    assert "token" not in detection["run"].lower()


def test_deteccao_e_identica_nos_dois_jobs() -> None:
    assert _detection(_FLOOR) == _detection(_NATIVE)


@pytest.mark.parametrize("job", [_NATIVE, _FLOOR])
def test_deteccao_usa_a_mesma_lista_de_caminhos_do_filtro_de_push(job: str) -> None:
    assert _patterns(job) == _TRIGGER_PATHS == _events()["push"]["paths"]


@pytest.mark.parametrize("job", [_NATIVE, _FLOOR])
def test_todo_passo_apos_a_deteccao_so_roda_com_infra_alterada(job: str) -> None:
    after = _job(job)["steps"][2:]

    assert after
    for step in after:
        condition = step.get("if", "")
        assert _GATE in condition, step.get("name", step.get("uses"))
        if "always()" in condition:
            assert condition == f"always() && {_GATE}"


def _git(cwd: Path, *arguments: str) -> None:
    subprocess.run(  # noqa: S603
        [str(shutil.which("git")), "-c", "user.name=teste",
         "-c", "user.email=teste@example.invalid", "-c", "commit.gpgsign=false",
         "-c", "init.defaultBranch=main", *arguments],
        check=True, capture_output=True, cwd=cwd, text=True,
    )


def _start_pr(repo: Path, *seeded: str) -> None:
    repo.mkdir()
    _git(repo, "init")
    for path in ("LEIAME.txt", *seeded):
        target = repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"conteudo estavel de {path}\n" * 8, encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "base")
    _git(repo, "checkout", "-b", "pr")


def _merge_pr(repo: Path) -> None:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "pr")
    _git(repo, "checkout", "main")
    _git(repo, "merge", "--no-ff", "-m", "merge", "pr")


def _merge_commit(repo: Path, changed: str) -> None:
    _start_pr(repo)
    target = repo / changed
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("alterado", encoding="utf-8")
    _merge_pr(repo)


def _detected(repo: Path, event: str, output: Path) -> str:
    script = _detection(_NATIVE)["run"]
    environment = {"PATH": os.environ["PATH"], "EVENT_NAME": event, "GITHUB_OUTPUT": str(output)}
    subprocess.run(  # noqa: S603
        [str(shutil.which("bash")), "-e", "-c", script],
        check=True, capture_output=True, cwd=repo, env=environment,
    )
    return output.read_text(encoding="utf-8").strip()


@pytest.mark.parametrize(
    ("changed", "expected"),
    [
        *((path.replace("**", "a/x.py"), "infra=true") for path in _TRIGGER_PATHS),
        ("packages/cnes_domain/tests/test_x.py", "infra=false"),
        ("docs/roadmap.md", "infra=false"),
        ("apps/data_processor/tests/test_x.py", "infra=false"),
        ("apps/web_dashboard/src/App.tsx", "infra=false"),
        ("tests/integration/aws_capability/_cleanup.py", "infra=false"),
        ("infra/opentofu/env/prod/configuração.tf", "infra=true"),
        ('policies/aspas"e\\barra.rego', "infra=true"),
        ("docs/runbooks/x.md", "infra=false"),
        ("tests/production/test_outro.py", "infra=false"),
        ("packages/cnes_infra/tests/aws/test_outro.py", "infra=false"),
        (".github/workflows/outro.yml", "infra=false"),
        ("apps/central_api/main.py", "infra=false"),
    ],
)
def test_deteccao_em_pull_request_olha_so_o_diff_do_merge(
    tmp_path: Path, changed: str, expected: str
) -> None:
    _merge_commit(tmp_path / "repo", changed)

    assert _detected(tmp_path / "repo", "pull_request", tmp_path / "saida") == expected


@pytest.mark.parametrize("seeded", ["policies/cnesdata.rego", "tests/production/infra/test_x.py"])
@pytest.mark.parametrize("operation", ["rm", "mv"])
def test_deteccao_ve_infra_removida_ou_renomeada_para_fora(
    tmp_path: Path, seeded: str, operation: str
) -> None:
    repo = tmp_path / "repo"
    _start_pr(repo, seeded)
    (repo / "docs").mkdir()
    _git(repo, operation, seeded, *(["docs/fora.txt"] if operation == "mv" else []))
    _merge_pr(repo)

    assert _detected(repo, "pull_request", tmp_path / "saida") == "infra=true"


@pytest.mark.parametrize("event", ["push", "workflow_dispatch"])
def test_deteccao_fora_de_pull_request_sempre_roda(tmp_path: Path, event: str) -> None:
    _merge_commit(tmp_path / "repo", "docs/runbooks/x.md")

    assert _detected(tmp_path / "repo", event, tmp_path / "saida") == "infra=true"
