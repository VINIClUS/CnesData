"""Vínculo imutável dos candidatos aos artefatos verificados de produção."""

from __future__ import annotations

import base64
import copy
import hashlib
import io
import json
import tarfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
SHA = "a" * 40
PR_SHA = "b" * 40
NOW = datetime(2026, 11, 7, 21, tzinfo=UTC)
CHECKS = (
    "lint-test-coverage", "sonar", "dependencies", "config", "image (central_api)",
    "image (data_processor)", "image (cnes_db_migrator)", "image (web_dashboard)",
    "production-runtime-acceptance", "opentofu-native",
)
JOBS = (
    "delivery-contracts", "dashboard", "opentofu-policy-cost", "oci-build (central_api)",
    "oci-build (data_processor)", "oci-publish (central_api)", "oci-publish (data_processor)",
)
REPOSITORY = "example/CnesData"
SOURCES = {
    "infra/opentofu/env/prod/providers.tf": b'locals { name_prefix = "cnesdata-prod" }',
    "infra/opentofu/env/prod/main.tf": (
        b'module "processor_registry" { source = "../../modules/processor-registry" '
        b'name_prefix = local.name_prefix }'
    ),
    "infra/opentofu/modules/processor-registry/main.tf": (
        b'resource "aws_ecr_repository" "this" { name = "${var.name_prefix}-processor" }'
    ),
}


def _hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json(data: Any) -> bytes:
    text = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return (text + "\n").encode()


def _tar(files: dict[str, bytes]) -> bytes:
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return stream.getvalue()


def _payload(files: dict[str, bytes]) -> dict[str, bytes]:
    result = {**files, "source.sha": (SHA + "\n").encode(), "run.id": b"42\n",
              "run.attempt": b"2\n"}
    result["SHA256SUMS"] = "".join(
        f"{_hash(data)}  {name}\n" for name, data in result.items()
    ).encode()
    return result


def _zip(files: dict[str, bytes]) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            info = zipfile.ZipInfo(name, date_time=(2026, 11, 7, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, data)
    return stream.getvalue()


class GitHubBoundary:
    def __init__(self) -> None:
        self.routes: dict[str, Any] = {}
        self.payloads: dict[str, dict[str, bytes]] = {}
        self.metadata: list[dict[str, Any]] = []
        self.downloads: list[str] = []

    def get(self, path: str) -> Any:
        return copy.deepcopy(self.routes[path.split("?", 1)[0]])

    def download(self, artifact: dict[str, Any], target: Path) -> None:
        self.downloads.append(artifact["name"])
        target.write_bytes(_zip(self.payloads[artifact["name"]]))

    def add(self, name: str, files: dict[str, bytes]) -> None:
        self.payloads[name] = files
        self.metadata.append({
            "id": len(self.metadata) + 1, "name": name, "expired": False,
            "expires_at": "2026-12-01T21:00:00Z", "digest": "sha256:" + _hash(_zip(files)),
            "size_in_bytes": len(_zip(files)),
            "workflow_run": {"id": 42, "head_sha": SHA, "head_branch": "develop"},
        })

    def refresh(self, name: str) -> None:
        data = _zip(self.payloads[name])
        for artifact in self.metadata:
            if artifact["name"] == name:
                artifact.update(digest="sha256:" + _hash(data), size_in_bytes=len(data))


def _add_image_artifacts(api: GitHubBoundary) -> None:
    for app in ("central_api", "data_processor"):
        image = (app + "-image").encode()
        sbom = _json({"bomFormat": "CycloneDX", "components": [{"name": app}]})
        config_digest = "sha256:" + _hash((app + "-config").encode())
        api.add("production-oci-" + app, _payload({
            "image.tar": image, "image.id": ("sha256:" + _hash(image) + "\n").encode(),
            "sbom.cdx.json": sbom, "scan.json": _json({"Metadata": {"ImageID": config_digest}}),
        }))
        published = {"schema_version": 1, "source_sha": SHA, "run_id": "42", "run_attempt": "2",
                     "image": "ghcr.io/example/cnesdata/" + app,
                     "digest": "sha256:" + _hash(image), "config_digest": config_digest,
                     "image_archive_sha256": _hash(image), "sbom_sha256": _hash(sbom)}
        data = _json(published)
        api.add("production-published-" + app,
                {"manifest.json": data,
                 "manifest.sha256": f"{_hash(data)}  manifest.json\n".encode()})


@pytest.fixture
def github() -> GitHubBoundary:
    api = GitHubBoundary()
    run = {"id": 42, "run_attempt": 2, "head_sha": SHA, "head_branch": "develop",
           "event": "push", "status": "completed", "conclusion": "success",
           "path": ".github/workflows/verify-production.yml", "check_suite_id": 91,
           "updated_at": "2026-11-07T20:00:00Z"}
    pr = {"number": 9, "merged_at": "2026-11-07T19:00:00Z", "merge_commit_sha": SHA,
          "base": {"ref": "develop", "repo": {"full_name": REPOSITORY}},
          "head": {"sha": PR_SHA}}
    checks = [{"name": name, "id": i + 1, "head_sha": PR_SHA, "status": "completed",
               "conclusion": "success", "completed_at": "2026-11-07T18:00:00Z",
               "app": {"id": 15368}}
              for i, name in enumerate(CHECKS)]
    suite = [{"name": name, "id": i + 20, "head_sha": SHA, "status": "completed",
              "conclusion": "success", "check_suite": {"id": 91}}
             for i, name in enumerate(JOBS)]
    api.routes = {
        "compare/" + SHA + "...develop": {"status": "ahead"},
        "actions/workflows/verify-production.yml/runs": {"workflow_runs": [run]},
        "actions/runs/42": run, "commits/" + SHA + "/check-runs": {"check_runs": suite},
        "commits/" + SHA + "/pulls": [pr], "pulls/9": pr,
        "commits/" + PR_SHA + "/check-runs": {"check_runs": checks},
        "rules/branches/develop": [{"type": "required_status_checks", "parameters": {
            "required_status_checks": [{"context": name, "integration_id": 15368}
                                       for name in CHECKS]}}],
        "actions/runs/42/artifacts": {"artifacts": api.metadata},
    }
    for path, source in SOURCES.items():
        api.routes["contents/" + path] = {"encoding": "base64", "type": "file",
                                          "content": base64.b64encode(source).decode()}
    config = {"ops/deploy/aws/compose.production.yaml": b"services: {}\n",
              "uv.lock": b"python-lock",
              "apps/web_dashboard/bun.lock": b"dashboard-lock",
              "infra/opentofu/env/prod/.terraform.lock.hcl": b"provider-lock"}
    api.add("production-config", _payload({"config.tar": _tar(config)}))
    api.add("production-dashboard", _payload({"dist.tar": _tar({"index.html": b"tested"})}))
    _add_image_artifacts(api)
    return api


def _collect(github: GitHubBoundary) -> dict[str, Any]:
    from release.build_candidate import collect_candidate

    return collect_candidate(REPOSITORY, SHA, github, NOW)


def _verify(manifest: dict[str, Any]) -> None:
    from release.verify_candidate import verify_candidate

    data = _json(manifest)
    verify_candidate(data, _hash(data), NOW)


def test_cria_bytes_e_sidecar_deterministicos_dos_artefatos_testados(github, tmp_path) -> None:
    from release.build_candidate import build_candidate
    from release.verify_candidate import verify_candidate

    manifest = _collect(github)
    first, second = tmp_path / "first.json", tmp_path / "second.json"
    build_candidate(manifest, first, NOW)
    github.metadata.reverse()
    build_candidate(_collect(github), second, NOW)
    assert first.read_bytes() == second.read_bytes() == _json(manifest)
    assert first.with_suffix(".json.sha256").read_text() == _hash(first.read_bytes()) + "\n"
    verify_candidate(first.read_bytes(), _hash(first.read_bytes()), NOW)
    assert manifest["expected_ecr_repository"] == "cnesdata-prod-processor"
    dashboard = github.payloads["production-dashboard"]["dist.tar"]
    assert manifest["dashboard_sha256"] == _hash(dashboard)
    assert manifest["locks"]["opentofu"] == _hash(b"provider-lock")
    for app in ("central_api", "data_processor"):
        assert manifest["images"][app]["sbom_sha256"] == _hash(
            github.payloads["production-oci-" + app]["sbom.cdx.json"]
        )


@pytest.mark.parametrize(("field", "value"), [
    ("taskDefinitionArn", "arn:aws:ecs:us-east-2:123456789012:task-definition/unit:1"),
    ("ecr_digest", "sha256:" + "c" * 64),
    ("expected_ecr_repository", "123456789012.dkr.ecr.us-east-2.amazonaws.com/p@sha256:"
     + "c" * 64),
])
def test_recusa_task_definition_arn_e_digest_ecr(github, field, value) -> None:
    manifest = _collect(github)
    manifest[field] = value
    with pytest.raises(ValueError):
        _verify(manifest)


@pytest.mark.parametrize("app", ["central_api", "data_processor"])
@pytest.mark.parametrize("mutation", ["tag", "sha", "run", "attempt", "config", "sbom", "archive"])
def test_recusa_tag_mutavel_e_run_de_outro_sha(github, app, mutation) -> None:
    name = "production-published-" + app
    published = json.loads(github.payloads[name]["manifest.json"])
    field, value = {
        "tag": ("image", published["image"] + ":latest"), "sha": ("source_sha", PR_SHA),
        "run": ("run_id", "99"), "attempt": ("run_attempt", "1"),
        "config": ("config_digest", "sha256:" + "e" * 64),
        "sbom": ("sbom_sha256", "e" * 64), "archive": ("image_archive_sha256", "e" * 64),
    }[mutation]
    published[field] = value
    data = _json(published)
    github.payloads[name] = {"manifest.json": data,
                             "manifest.sha256": f"{_hash(data)}  manifest.json\n".encode()}
    github.refresh(name)
    with pytest.raises(ValueError):
        _collect(github)


@pytest.mark.parametrize(("name", "file"), [
    ("production-oci-central_api", "sbom.cdx.json"),
    ("production-oci-data_processor", "SHA256SUMS"),
    ("production-dashboard", "run.attempt"), ("production-config", "source.sha"),
    ("production-published-central_api", "manifest.sha256"),
])
def test_recusa_sbom_ou_checksum_ausente(github, name, file) -> None:
    del github.payloads[name][file]
    github.refresh(name)
    with pytest.raises(ValueError):
        _collect(github)


@pytest.mark.parametrize(("field", "value"), [
    ("expires_at", "2026-11-07T20:00:00Z"), ("api_token", "do-not-persist"),
    ("release_id", "other-release"), ("source_sha", PR_SHA),
])
def test_recusa_manifest_expirado_e_chave_parecida_com_segredo(github, field, value) -> None:
    manifest = _collect(github)
    manifest[field] = value
    with pytest.raises(ValueError):
        _verify(manifest)


def test_recusa_json_nao_canonico(github) -> None:
    from release.verify_candidate import verify_candidate

    data = json.dumps(_collect(github), indent=2).encode()
    with pytest.raises(ValueError, match="canonical"):
        verify_candidate(data, _hash(data), NOW)


@pytest.mark.parametrize("check", CHECKS)
@pytest.mark.parametrize("failure", ["missing", "failure", "after_merge"])
def test_exige_os_dez_checks_obrigatorios_no_pr_mergeado(github, check, failure) -> None:
    checks = github.routes["commits/" + PR_SHA + "/check-runs"]["check_runs"]
    record = next(item for item in checks if item["name"] == check)
    if failure == "missing":
        checks.remove(record)
    elif failure == "failure":
        record["conclusion"] = "failure"
    else:
        record["completed_at"] = "2026-11-07T20:00:00Z"
    with pytest.raises(ValueError):
        _collect(github)
    assert github.downloads == []


@pytest.mark.parametrize("failure", ["run", "suite", "develop", "pr", "rules"])
def test_recusa_gate_invalido_antes_de_baixar_artefatos(github, failure) -> None:
    if failure == "run":
        github.routes["actions/runs/42"]["head_sha"] = PR_SHA
    elif failure == "suite":
        github.routes["commits/" + SHA + "/check-runs"]["check_runs"][0]["conclusion"] = "failure"
    elif failure == "develop":
        github.routes["compare/" + SHA + "...develop"]["status"] = "diverged"
    elif failure == "pr":
        github.routes["pulls/9"]["merged_at"] = None
    else:
        github.routes["rules/branches/develop"][0]["parameters"]["required_status_checks"].pop()
    with pytest.raises(ValueError):
        _collect(github)
    assert github.downloads == []


@pytest.mark.parametrize("name", [
    "production-config", "production-dashboard", "production-oci-central_api",
    "production-oci-data_processor", "production-published-central_api",
    "production-published-data_processor",
])
@pytest.mark.parametrize("failure", ["missing", "duplicate", "expired", "digest", "sha", "run"])
def test_recusa_proveniencia_incompleta_dos_artifacts(github, name, failure) -> None:
    artifact = next(item for item in github.metadata if item["name"] == name)
    if failure == "missing":
        github.metadata.remove(artifact)
    elif failure == "duplicate":
        github.metadata.append(copy.deepcopy(artifact))
    elif failure == "expired":
        artifact["expired"] = True
    elif failure == "digest":
        artifact["digest"] = "sha256:" + "f" * 64
    else:
        key, value = ("head_sha", PR_SHA) if failure == "sha" else ("id", 99)
        artifact["workflow_run"][key] = value
    with pytest.raises(ValueError):
        _collect(github)


@pytest.mark.parametrize("name", [
    "production-config", "production-dashboard", "production-oci-central_api",
    "production-oci-data_processor",
])
def test_recusa_checksum_recalculado_com_payload_de_outro_attempt(github, name) -> None:
    files = github.payloads[name]
    files["run.attempt"] = b"1\n"
    files["SHA256SUMS"] = "".join(
        f"{_hash(data)}  {key}\n" for key, data in files.items() if key != "SHA256SUMS"
    ).encode()
    github.refresh(name)
    with pytest.raises(ValueError):
        _collect(github)


def test_recusa_alteracao_do_contrato_de_repositorio_ecr(github) -> None:
    path = "contents/infra/opentofu/modules/processor-registry/main.tf"
    github.routes[path]["content"] = base64.b64encode(b'name = "other-repository"').decode()
    with pytest.raises(ValueError):
        _collect(github)


@pytest.mark.parametrize("failure", ["integration", "newer_failure", "other_head"])
def test_recusa_evidencia_de_check_sem_autoridade(github, failure) -> None:
    checks = github.routes["commits/" + PR_SHA + "/check-runs"]["check_runs"]
    if failure == "integration":
        checks[0]["app"]["id"] = 999
    elif failure == "other_head":
        checks[0]["head_sha"] = "c" * 40
    else:
        newer = copy.deepcopy(checks[0])
        newer.update(id=999, conclusion="failure", completed_at="2026-11-07T18:30:00Z")
        checks.append(newer)
    with pytest.raises(ValueError):
        _collect(github)
    assert github.downloads == []


@pytest.mark.parametrize("check", CHECKS)
@pytest.mark.parametrize("outcome", ["pending", "failed_after_merge", "unknown_start"])
def test_recusa_execucao_mais_recente_nao_verde_no_merge(github, check, outcome) -> None:
    checks = github.routes["commits/" + PR_SHA + "/check-runs"]["check_runs"]
    newer = copy.deepcopy(next(item for item in checks if item["name"] == check))
    newer.update(id=999, status="in_progress", conclusion=None,
                 started_at="2026-11-07T18:30:00Z", completed_at=None)
    if outcome == "failed_after_merge":
        newer.update(status="completed", conclusion="failure", completed_at="2026-11-07T19:30:00Z")
    elif outcome == "unknown_start":
        newer.update(status="queued", started_at=None)
    checks.append(newer)
    with pytest.raises(ValueError, match="pull=not_green"):
        _collect(github)
    assert github.downloads == []


@pytest.mark.parametrize("check", CHECKS)
@pytest.mark.parametrize("outcome", ["pending", "failed"])
def test_preserva_evidencia_historica_para_execucao_posterior_ao_merge(
    github, check, outcome,
) -> None:
    checks = github.routes["commits/" + PR_SHA + "/check-runs"]["check_runs"]
    original = next(item for item in checks if item["name"] == check)
    newer = copy.deepcopy(original)
    newer.update(id=999, status="in_progress", conclusion=None,
                 started_at="2026-11-07T19:30:00Z", completed_at=None)
    if outcome == "failed":
        newer.update(status="completed", conclusion="failure", completed_at="2026-11-07T20:00:00Z")
    checks.append(newer)
    assert _collect(github)["pull_request"]["checks"][check]["id"] == original["id"]


@pytest.mark.parametrize("failure", [
    "payload", "unchecked", "path", "symlink", "lock", "directory",
])
def test_recusa_payload_ou_archive_adulterado(github, failure) -> None:
    name = "production-config"
    files = github.payloads[name]
    if failure == "payload":
        files["config.tar"] += b"modified"
    elif failure == "unchecked":
        files["api_token"] = b"forbidden"
    else:
        contents: dict[str, bytes] = dict.fromkeys((
            "ops/deploy/aws/compose.production.yaml", "uv.lock", "apps/web_dashboard/bun.lock",
            "infra/opentofu/env/prod/.terraform.lock.hcl"), b"lock")
        if failure == "path":
            contents["../outside"] = b"escape"
        elif failure == "lock":
            del contents["infra/opentofu/env/prod/.terraform.lock.hcl"]
        elif failure == "directory":
            del contents["uv.lock"]
        data = _tar(contents)
        if failure in {"symlink", "directory"}:
            stream = io.BytesIO(data)
            with tarfile.open(fileobj=stream, mode="a") as archive:
                member = tarfile.TarInfo("unsafe" if failure == "symlink" else "uv.lock")
                member.type = tarfile.SYMTYPE if failure == "symlink" else tarfile.DIRTYPE
                member.linkname = "/etc/passwd"
                archive.addfile(member)
            data = stream.getvalue()
        github.payloads[name] = _payload({"config.tar": data})
    github.refresh(name)
    reason = "config=missing_content" if failure == "directory" else None
    with pytest.raises(ValueError, match=reason):
        _collect(github)


def test_recusa_candidato_acima_do_limite_e_checksum_incorreto(github) -> None:
    from release.verify_candidate import verify_candidate

    data = _json(_collect(github))
    with pytest.raises(ValueError, match="checksum"):
        verify_candidate(data, "0" * 64, NOW)
    oversized = data + b" " * 65536
    with pytest.raises(ValueError, match="size"):
        verify_candidate(oversized, _hash(oversized), NOW)


def test_nao_escreve_saida_para_manifest_invalido(github, tmp_path) -> None:
    from release.build_candidate import build_candidate

    manifest = _collect(github)
    manifest["images"]["data_processor"]["image"] = "ghcr.io/example/cnesdata/data_processor:latest"
    with pytest.raises(ValueError):
        build_candidate(manifest, tmp_path / "candidate.json", NOW)
    assert list(tmp_path.iterdir()) == []


def test_workflow_manual_hospedado_somente_leitura() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/release-candidate.yml").read_text())
    assert set(workflow[True]) == {"workflow_dispatch"}
    assert workflow["permissions"] == {"contents": "read", "actions": "read", "checks": "read",
                                        "pull-requests": "read"}
    for job in workflow["jobs"].values():
        assert job["runs-on"] == "ubuntu-latest"
        assert "github.ref == 'refs/heads/develop'" in job["if"]
        assert "permissions" not in job
    serialized = yaml.safe_dump(workflow)
    for forbidden in ("packages: write", "id-token", "docker build", "secrets.", "self-hosted"):
        assert forbidden not in serialized
