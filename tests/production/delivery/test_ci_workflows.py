"""Contratos de CI e dos bytes compilados da entrega de produção."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from shlex import split
from typing import Any

import pytest
import yaml

from tests.production.delivery.conftest import require_tool

ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS = ROOT / ".github/workflows"
CHANGED = ("verify-production.yml", "production-runtime.yml")
API = "https://api.cnesdata.com.br/api/v1"


def _workflow(name: str = "verify-production.yml") -> dict[Any, Any]:
    path = WORKFLOWS / name
    assert path.is_file(), f"workflow_missing={name}"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _commands(job: dict[str, Any]) -> str:
    return "\n".join(step.get("run", "") for step in job["steps"])


def test_actions_dos_workflows_de_delivery_usam_sha_completo() -> None:
    for name in CHANGED:
        for job in _workflow(name)["jobs"].values():
            for step in job["steps"]:
                if "uses" in step:
                    assert re.fullmatch(r"[\w/-]+@[0-9a-f]{40}", step["uses"]), step["uses"]


def test_jobs_de_delivery_usam_runner_hospedado() -> None:
    for name in CHANGED:
        for job in _workflow(name)["jobs"].values():
            assert job["runs-on"] == "ubuntu-latest"


def test_dependencias_instaladas_com_lock() -> None:
    jobs = _workflow()["jobs"]
    assert "uv sync --locked --all-packages" in _commands(jobs["delivery-contracts"])
    assert "bun install --frozen-lockfile" in _commands(jobs["dashboard"])
    assert "-lockfile=readonly" in _commands(jobs["opentofu-policy-cost"])


def test_pull_request_nao_recebe_segredo_id_token_nem_packages_write() -> None:
    workflow = _workflow()
    assert workflow["permissions"] == {"contents": "read"}
    assert "secrets." not in yaml.safe_dump(workflow)
    assert "id-token" not in yaml.safe_dump(workflow)
    for name, job in workflow["jobs"].items():
        if name != "oci-publish":
            assert job.get("permissions", workflow["permissions"]) == {"contents": "read"}
    assert workflow["jobs"]["oci-publish"]["permissions"] == {
        "contents": "read",
        "packages": "write",
    }


def test_oci_publish_so_roda_em_push_e_nao_builda() -> None:
    job = _workflow()["jobs"]["oci-publish"]
    assert job["if"] == "github.event_name == 'push' && github.ref == 'refs/heads/develop'"
    assert "oci-build" in job["needs"]
    commands = _commands(job)
    assert "docker load --input image.tar" in commands
    assert "sha256sum -c SHA256SUMS" in commands
    assert "docker push" in commands
    assert "build-push-action" not in yaml.safe_dump(job)
    assert "docker buildx imagetools inspect" in commands
    assert "image.id" in commands
    assert "manifest.json" in commands


def test_push_em_develop_sem_filtro_de_path() -> None:
    events = _workflow()[True]
    assert set(events) == {"push", "pull_request", "workflow_dispatch"}
    assert events["push"] == {"branches": ["develop"]}


def test_imagem_salva_e_escaneada_antes_de_publicar_artifact() -> None:
    job = _workflow()["jobs"]["oci-build"]
    assert job["strategy"]["matrix"]["app"] == ["central_api", "data_processor"]
    builds = [s for s in job["steps"] if "docker/build-push-action@" in s.get("uses", "")]
    assert len(builds) == 1
    assert builds[0]["with"]["push"] is False
    assert builds[0]["with"]["load"] is True
    commands = _commands(job)
    assert "docker save --output image.tar" in commands
    assert "trivy image --input image.tar" in commands
    assert "--format cyclonedx" in commands
    assert "--severity CRITICAL,HIGH --ignore-unfixed --exit-code 1" in commands
    assert commands.index("docker save") < commands.index("--input image.tar")
    assert "sbom.cdx.json" in commands
    assert "SHA256SUMS" in commands
    upload = job["steps"][-1]
    assert upload["with"]["name"] == "production-oci-${{ matrix.app }}"
    assert upload["with"]["if-no-files-found"] == "error"


def test_artifacts_tem_contrato_duravel_para_candidato() -> None:
    text = (WORKFLOWS / "verify-production.yml").read_text(encoding="utf-8")
    for token in (
        "production-dashboard",
        "production-config",
        "production-published-${{ matrix.app }}",
    ):
        assert token in text, f"artifact_contract_missing={token}"


def test_infra_reusa_politicas_nativas_e_scan_revisado() -> None:
    commands = _commands(_workflow()["jobs"]["opentofu-policy-cost"])
    for command in (
        "tofu -chdir=infra/opentofu fmt -check -recursive",
        "-backend=false",
        "tests/production/infra/test_opa_policy.py",
        "tests/production/infra/test_acceptance.py",
        "tests/production/infra/test_cost_manifest.py",
        "trivy config",
        "trivy fs",
    ):
        assert command in commands


def test_dashboard_de_producao_recusa_api_relativa() -> None:
    job = _workflow()["jobs"]["dashboard"]
    assert job["env"]["VITE_API_BASE_URL"] == API
    assert job["env"]["VITE_RUNTIME_POLICY"] == "production"
    assert "bun run build" in _commands(job)
    assert "-k test_dist_" in _commands(job)
    assert job["env"]["PRODUCTION_DIST"] == "apps/web_dashboard/dist"


def test_dashboard_preserva_browser_do_lock_em_sistema_suportado() -> None:
    job = _workflow()["jobs"]["dashboard"]
    lock = (ROOT / "apps/web_dashboard/bun.lock").read_text()
    versions = set(re.findall(r"@playwright/test@([\d.]+)", lock))
    assert len(versions) == 1
    version = versions.pop()
    assert re.fullmatch(
        rf"mcr\.microsoft\.com/playwright:v{re.escape(version)}-noble@sha256:[a-f0-9]{{64}}",
        job["container"]["image"],
    )
    assert "playwright install" not in _commands(job)
    assert "PLAYWRIGHT_CHANNEL" not in yaml.safe_dump(job) + DIST_ORACLE
    assert {"--only-dev", "--managed-python", "--no-sync"} <= set(split(_commands(job)))
    steps = yaml.safe_dump(job["steps"])
    assert steps.index("install -y --no-install-recommends unzip") < steps.index("oven-sh/")


def test_emuladores_derrubados_com_always() -> None:
    job = _workflow("production-runtime.yml")["jobs"]["runtime-emulators"]
    teardown = [s for s in job["steps"] if "down -v" in s.get("run", "")]
    assert len(teardown) == 2
    assert all(step["if"] == "always()" for step in teardown)


def test_delivery_tem_casa_propria_sem_afrouxar_runtime() -> None:
    runtime = _workflow("production-runtime.yml")["jobs"]
    assert "--ignore=tests/production/delivery" in _commands(runtime["runtime-python"])
    own = _commands(_workflow()["jobs"]["delivery-contracts"])
    assert "pytest -q tests/production/delivery" in own
    assert "require_complete_junit" in own
    assert runtime["production-runtime-acceptance"]["needs"] == [
        "runtime-python",
        "runtime-dashboard",
        "runtime-emulators",
    ]


def test_actionlint_fixado_e_documentado() -> None:
    job = _workflow()["jobs"]["delivery-contracts"]
    step = next(s for s in job["steps"] if s.get("name") == "Install actionlint")
    assert "sha256sum -c -" in step["run"]
    docs = (ROOT / "docs/development.md").read_text(encoding="utf-8")
    for value in step["env"].values():
        assert str(value) in docs
    version = subprocess.run(  # noqa: S603 -- Executes the fixed local actionlint binary.
        [require_tool("actionlint"), "-version"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert version.stdout.splitlines()[0] == step["env"]["ACTIONLINT_VERSION"]


def test_actionlint_valida_workflows_alterados() -> None:
    subprocess.run(  # noqa: S603 -- Validates checked-in workflows with fixed actionlint.
        [require_tool("actionlint"), *(str(WORKFLOWS / n) for n in CHANGED)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )


def test_ci_falha_sem_ferramenta(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("PATH", "")
    with pytest.raises(pytest.fail.Exception, match="delivery_tool_missing=actionlint"):
        require_tool("actionlint")


DIST_ORACLE = r"""const {chromium} = require('@playwright/test');
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
(async () => {
  const root = process.env.PRODUCTION_DIST;
  const files = fs.readdirSync(path.join(root, 'assets')).filter(p => p.endsWith('.js'));
  const clients = files.filter(p => fs.readFileSync(path.join(root, 'assets', p), 'utf8')
    .includes('function createApiFetch('));
  assert.equal(clients.length, 1, 'single_compiled_api_client');
  const vendor = /^(oidc|tanstack|react|d3|recharts|tremor|datepicker)-/;
  const application = files.filter(p => !vendor.test(p))
    .map(p => fs.readFileSync(path.join(root, 'assets', p), 'utf8')).join('\n');
  const transports = application.match(/(?<![.\w])fetch\(/g) || [];
  assert.equal(transports.length, 3, 'only_api_client_signed_s3_and_vite_preload');
  assert(files.some(p => fs.readFileSync(path.join(root, 'assets', p), 'utf8')
    .includes('"/activate/confirm"')), 'compiled_activation_required');
  const browser = await chromium.launch({headless: true});
  try {
    const page = await browser.newPage();
    await page.route('**/*', async route => {
      const url = new URL(route.request().url());
      if (url.origin !== 'http://dashboard.test') return route.abort();
      let name = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
      const file = path.join(root, name);
      if (!fs.existsSync(file)) return route.abort();
      let body = fs.readFileSync(file);
      if (name === 'assets/' + clients[0]) body = Buffer.from(body.toString() +
        '\n;globalThis.__delivery = {apiFetch, createApiFetch};');
      const type = name.endsWith('.js') ? 'text/javascript' :
        name.endsWith('.css') ? 'text/css' : 'text/html';
      await route.fulfill({body, contentType: type});
    });
    await page.goto('http://dashboard.test/');
    await page.waitForFunction(() => globalThis.__delivery);
    const seen = await page.evaluate(async () => {
      const calls = [];
      globalThis.fetch = async (url, options) => {
        calls.push({url, credentials: options.credentials,
          auth: options.headers.get('Authorization'), tenant: options.headers.get('X-Tenant-Id')});
        return new Response('{}');
      };
      const api = globalThis.__delivery.apiFetch;
      await api('/activate/confirm', {
        method: 'POST', tenantId: '354130', accessToken: 'synthetic'});
      await api('/dashboard/auth/me', {accessToken: null,
        headers: {'X-Tenant-Id': 'forged', Authorization: 'Bearer forged'}});
      for (const endpoint of ['https://foreign.test/x', '//foreign.test/x', '/../x']) {
        try { await api(endpoint, {accessToken: 'synthetic'}); throw Error('accepted_foreign'); }
        catch (e) { if (!['api_origin_mismatch', 'api_path_invalid'].includes(e.code)) throw e; }
      }
      return calls;
    });
    assert.deepEqual(seen, [
      {url: 'https://api.cnesdata.com.br/api/v1/activate/confirm', credentials: 'omit',
       auth: 'Bearer synthetic', tenant: '354130'},
      {url: 'https://api.cnesdata.com.br/api/v1/dashboard/auth/me', credentials: 'omit',
       auth: null, tenant: null}
    ]);
  } finally { await browser.close(); }
})().catch(e => { console.error(e.message); process.exit(1); });"""


def _dist_path() -> Path:
    dist = os.environ.get("PRODUCTION_DIST")
    if not dist:
        pytest.skip("production_dist=not_requested")
    return (ROOT / dist).resolve()


def _run_dist(dist: Path) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ, PRODUCTION_DIST=str(dist))
    return subprocess.run(  # noqa: S603 -- Executes the checked-in dist oracle.
        [require_tool("node"), "-e", DIST_ORACLE],
        cwd=ROOT / "apps/web_dashboard",
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def test_dist_usa_api_absoluta_client_unico_e_headers_escopados() -> None:
    result = _run_dist(_dist_path())
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "base", ["/api/v1", "https://foreign.test/api/v1"], ids=["relativa", "outra-origem"]
)
def test_dist_recusa_bytes_compilados_com_api_errada(tmp_path: Path, base: str) -> None:
    copied = tmp_path / "dist"
    shutil.copytree(_dist_path(), copied)
    for path in copied.rglob("*.js"):
        path.write_text(path.read_text().replace(API, base))
    result = _run_dist(copied)
    assert result.returncode != 0


DOCKER_BOUNDARY = r"""#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> "$DOCKER_CALLS"
case "$1 $2" in
  'login ghcr.io') cat > /dev/null ;;
  'image inspect') printf '%s\n' "$LOADED_ID" ;;
  'buildx imagetools')
    if [[ "$5" == --raw ]]; then
      printf '{"config":{"digest":"%s"}}\n' "$REMOTE_CONFIG"
    else
      printf 'sha256:%064d\n' 1
    fi ;;
esac
"""


def _shell(cwd: Path, env: dict[str, str], body: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 -- Executes checked-in workflow at executable boundaries.
        [require_tool("bash"), "-euo", "pipefail", "-c", body],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )


def _publisher_inputs(directory: Path) -> dict[str, str]:
    config = "sha256:" + "c" * 64
    files = {
        "image.tar": "scanned_image",
        "image.id": "sha256:" + "d" * 64,
        "scan.json": json.dumps({"Metadata": {"ImageID": config}}),
        "sbom.cdx.json": "{}",
        "source.sha": "a" * 40,
        "run.id": "123",
        "run.attempt": "1",
    }
    for name, value in files.items():
        (directory / name).write_text(value + "\n")
    _checksum_inputs(directory)
    docker = directory / "docker"
    docker.write_text(DOCKER_BOUNDARY)
    docker.chmod(0o755)
    return dict(
        os.environ,
        PATH=str(directory) + os.pathsep + os.environ["PATH"],
        GITHUB_SHA="a" * 40,
        GITHUB_RUN_ID="123",
        GITHUB_RUN_ATTEMPT="1",
        GITHUB_REPOSITORY="VINIClUS/CnesData",
        APP="central_api",
        GHCR_TOKEN="synthetic",  # noqa: S106 -- Synthetic Docker boundary credential.
        GHCR_ACTOR="synthetic",
        LOADED_ID=files["image.id"],
        REMOTE_CONFIG=config,
        DOCKER_CALLS=str(directory / "docker.calls"),
    )


def _checksum_inputs(directory: Path) -> None:
    files = sorted(p for p in directory.iterdir() if p.name not in {"SHA256SUMS", "docker"})
    content = "".join(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n" for p in files)
    (directory / "SHA256SUMS").write_text(content)


@pytest.mark.parametrize(
    "case",
    [
        "valido",
        "tar_corrompido",
        "outro_sha",
        "load_errado",
        "remoto_errado",
        "config_ausente",
        "config_ambiguo",
    ],
)
def test_publisher_so_publica_os_bytes_vinculados_ao_run(tmp_path: Path, case: str) -> None:
    environment = _publisher_inputs(tmp_path)
    if case == "tar_corrompido":
        (tmp_path / "image.tar").write_text("different_image")
    if case == "outro_sha":
        environment["GITHUB_SHA"] = "b" * 40
    if case == "load_errado":
        environment["LOADED_ID"] = "sha256:" + "e" * 64
    if case == "remoto_errado":
        environment["REMOTE_CONFIG"] = "sha256:" + "d" * 64
    if case.startswith("config_"):
        value = None if case == "config_ausente" else [environment["REMOTE_CONFIG"]] * 2
        (tmp_path / "scan.json").write_text(json.dumps({"Metadata": {"ImageID": value}}))
        _checksum_inputs(tmp_path)
    job = _workflow()["jobs"]["oci-publish"]
    body = next(
        s["run"] for s in job["steps"] if s.get("name") == "Load verified bytes and publish digest"
    )
    result = _shell(tmp_path, environment, body)
    assert (result.returncode == 0) is (case == "valido"), result.stderr
    calls = (tmp_path / "docker.calls").read_text() if (tmp_path / "docker.calls").exists() else ""
    if case in {"tar_corrompido", "outro_sha", "load_errado", "config_ausente", "config_ambiguo"}:
        assert "push " not in calls
    _assert_no_image_build(calls)
    if case == "valido":
        manifest = json.loads((tmp_path / "published/manifest.json").read_text())
        assert manifest["schema_version"] == 1
        assert manifest["source_sha"] == environment["GITHUB_SHA"]
        assert manifest["run_id"] == "123"
        assert manifest["run_attempt"] == "1"
        assert manifest["config_digest"] == environment["REMOTE_CONFIG"]
        assert manifest["image"] == "ghcr.io/viniclus/cnesdata/central_api"
        files = {"image.tar": "image_archive_sha256", "sbom.cdx.json": "sbom_sha256"}
        for name, field in files.items():
            assert manifest[field] == hashlib.sha256((tmp_path / name).read_bytes()).hexdigest()
        assert re.fullmatch(r"sha256:[0-9a-f]{64}", manifest["digest"])


def _assert_no_image_build(calls: str) -> None:
    for line in calls.splitlines():
        arguments = split(line)
        while arguments and arguments[0].startswith("-"):
            option = arguments.pop(0)
            if option in {"--context", "-c", "--host", "-H", "--config", "--log-level", "-l"}:
                arguments = arguments[1:]
        namespace = arguments[0] if arguments else ""
        operation = arguments[:2] if namespace in {"buildx", "image", "compose"} else arguments[:1]
        assert not set(operation) & {"build", "bake"}, "image_rebuild_forbidden"


@pytest.mark.parametrize(
    "command",
    [
        "build .",
        "buildx build .",
        "image build .",
        "buildx bake .",
        "--context=ci build .",
        "--context ci image build .",
    ],
)
def test_contrato_de_bytes_rejeita_comandos_efetivos_de_rebuild(
    tmp_path: Path,
    command: str,
) -> None:
    environment = _publisher_inputs(tmp_path)
    result = _shell(tmp_path, environment, "docker " + command)
    assert result.returncode == 0, result.stderr
    with pytest.raises(AssertionError, match="image_rebuild_forbidden"):
        _assert_no_image_build((tmp_path / "docker.calls").read_text())
