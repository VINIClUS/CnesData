"""Coleta evidências do GitHub e cria candidato sem reconstruir artefatos."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import re
import tarfile
import tempfile
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import IO, Any
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen

from release.verify_candidate import canonical, require, timestamp, verify_candidate

REQUIRED_CHECKS = frozenset({
    "lint-test-coverage", "sonar", "dependencies", "config", "image (central_api)",
    "image (data_processor)", "image (cnes_db_migrator)", "image (web_dashboard)",
    "production-runtime-acceptance", "opentofu-native",
})
VERIFY_JOBS = frozenset({
    "delivery-contracts", "dashboard", "opentofu-policy-cost", "oci-build (central_api)",
    "oci-build (data_processor)", "oci-publish (central_api)", "oci-publish (data_processor)",
})
APPS = ("central_api", "data_processor")
ARTIFACTS = {"production-dashboard", "production-config"} | {
    f"production-{kind}-{app}" for kind in ("oci", "published") for app in APPS
}
SOURCE_PATHS = (
    "infra/opentofu/env/prod/providers.tf", "infra/opentofu/env/prod/main.tf",
    "infra/opentofu/modules/processor-registry/main.tf",
)
CONFIG_PATHS = {
    "compose": "deploy/compose.production.yaml", "python": "uv.lock",
    "dashboard": "apps/web_dashboard/bun.lock",
    "opentofu": "infra/opentofu/env/prod/.terraform.lock.hcl",
}
MAX_ARCHIVE_BYTES = 2 * 1024**3
MAX_METADATA_BYTES = 4 * 1024**2


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class GitHub:
    """Args: repositório GitHub; Raises: ValueError sem token efêmero."""

    def __init__(self, repository: str) -> None:
        self.base = f"https://api.github.com/repos/{repository}/"
        token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
        require(token, "github=token_missing")
        self.headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                        "X-GitHub-Api-Version": "2022-11-28"}

    def get(self, path: str) -> Any:
        """Returns: JSON da API oficial com limite de tamanho por página."""
        request = Request(self.base + path, headers=self.headers)
        with urlopen(request, timeout=60) as response:
            data = response.read(MAX_METADATA_BYTES + 1)
        require(len(data) <= MAX_METADATA_BYTES, "github=metadata_size")
        return json.loads(data)

    def download(self, artifact: dict[str, Any], target: Path) -> None:
        """Args: artifact da API e destino ZIP temporário; nenhum token em redirects."""
        path = self.base + f"actions/artifacts/{artifact['id']}/zip"
        request = Request(path, headers=self.headers)
        try:
            response = build_opener(_NoRedirect()).open(request, timeout=60)
        except HTTPError as error:
            require(error.code == 302, "artifact=download_status")
            location = error.headers.get("Location", "")
            require(urlparse(location).scheme == "https", "artifact=redirect_scheme")
            response = urlopen(location, timeout=120)
        with response, target.open("wb") as output:
            remaining = MAX_ARCHIVE_BYTES + 1
            while chunk := response.read(min(1024**2, remaining)):
                remaining -= len(chunk)
                require(remaining > 0, "artifact=download_size")
                output.write(chunk)


def _pages(api: Any, path: str, key: str | None = None) -> list[dict[str, Any]]:
    result = []
    for page in range(1, 101):
        separator = "&" if "?" in path else "?"
        response = api.get(f"{path}{separator}per_page=100&page={page}")
        items = response[key] if key else response
        require(isinstance(items, list), "github=collection")
        result.extend(items)
        if len(items) < 100:
            return result
    raise ValueError("github=pagination_limit")


def _green(check: dict[str, Any], sha: str) -> bool:
    return (check.get("head_sha") == sha and check.get("status") == "completed"
            and check.get("conclusion") == "success")


def _verification(api: Any, sha: str) -> dict[str, Any]:
    require(api.get(f"compare/{sha}...develop").get("status") in {"ahead", "identical"},
            "source=outside_develop")
    runs = _pages(api, "actions/workflows/verify-production.yml/runs"
                  f"?head_sha={sha}&branch=develop&event=push&status=success", "workflow_runs")
    require(runs, "verification=missing")
    run = api.get(f"actions/runs/{max(runs, key=lambda item: item['id'])['id']}")
    require(_green(run, sha) and run.get("event") == "push"
            and run.get("head_branch") == "develop"
            and run.get("path") == ".github/workflows/verify-production.yml",
            "verification=identity")
    checks = _pages(api, f"commits/{sha}/check-runs?filter=all", "check_runs")
    suite = [item for item in checks
             if item.get("check_suite", {}).get("id") == run["check_suite_id"]]
    latest = {item["name"]: item for item in sorted(suite, key=lambda item: item["id"])}
    require(latest.keys() >= VERIFY_JOBS, "verification=checks_missing")
    require(all(_green(latest[name], sha) for name in VERIFY_JOBS), "verification=checks_not_green")
    return run


def _pull_request(api: Any, sha: str, repository: str) -> dict[str, Any]:
    rules = api.get("rules/branches/develop")
    required = {check["context"]: check.get("integration_id") for rule in rules for check in
                rule.get("parameters", {}).get("required_status_checks", [])}
    require(required.keys() == REQUIRED_CHECKS, "rules=contexts_changed")
    associated = _pages(api, f"commits/{sha}/pulls")
    matching = [item for item in associated if item.get("merge_commit_sha") == sha
                and item.get("merged_at") and item["base"]["ref"] == "develop"]
    require(len(matching) == 1, "pull=association")
    pr = api.get(f"pulls/{matching[0]['number']}")
    require(pr.get("merged_at") and pr.get("merge_commit_sha") == sha
            and pr["base"]["ref"] == "develop"
            and pr["base"]["repo"]["full_name"].lower() == repository.lower(), "pull=identity")
    checks = _pages(api, f"commits/{pr['head']['sha']}/check-runs?filter=all", "check_runs")
    merged = timestamp(pr["merged_at"])
    eligible = [item for item in checks
                if (not item.get("started_at") or timestamp(item["started_at"]) <= merged)
                and (not required.get(item["name"])
                     or item.get("app", {}).get("id") == required[item["name"]])]
    latest = {item["name"]: item for item in sorted(eligible, key=lambda item: item["id"])}
    require(latest.keys() >= REQUIRED_CHECKS, "pull=checks_missing")
    require(all(_green(latest[name], pr["head"]["sha"]) and latest[name].get("completed_at")
                and timestamp(latest[name]["completed_at"]) <= merged for name in REQUIRED_CHECKS),
            "pull=not_green")
    return {"number": pr["number"], "head_sha": pr["head"]["sha"], "merged_at": pr["merged_at"],
            "checks": {name: {"id": latest[name]["id"],
                              "completed_at": latest[name]["completed_at"]}
                       for name in sorted(REQUIRED_CHECKS)}}


def _hash(stream: IO[bytes]) -> str:
    digest = hashlib.sha256()
    while chunk := stream.read(1024**2):
        digest.update(chunk)
    return digest.hexdigest()


def _small(archive: zipfile.ZipFile, name: str) -> bytes:
    require(name in archive.namelist(), "artifact=file_missing")
    require(archive.getinfo(name).file_size <= MAX_METADATA_BYTES, "artifact=metadata_size")
    return archive.read(name)


def _checksum_files(archive: zipfile.ZipFile, published: bool) -> dict[str, str]:
    sidecar = "manifest.sha256" if published else "SHA256SUMS"
    entries = _small(archive, sidecar).decode("ascii").splitlines()
    checksums = {}
    for line in entries:
        match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9_.-]+)", line)
        if match is None:
            raise ValueError("artifact=checksum_format")
        digest, name = match.groups()
        require(name not in checksums and name != sidecar, "artifact=checksum_duplicate")
        checksums[name] = digest
    require(set(checksums) == set(archive.namelist()) - {sidecar}, "artifact=unchecked_files")
    for name, digest in checksums.items():
        with archive.open(name) as stream:
            require(_hash(stream) == digest, "artifact=checksum_mismatch")
    return checksums


def _artifact_metadata(api: Any, run: dict[str, Any], now: datetime) -> dict[str, Any]:
    items = _pages(api, f"actions/runs/{run['id']}/artifacts", "artifacts")
    selected = [item for item in items if item["name"] in ARTIFACTS]
    require(len(selected) == len(ARTIFACTS) and {item["name"] for item in selected} == ARTIFACTS,
            "artifacts=missing_or_duplicate")
    for item in selected:
        origin = item.get("workflow_run", {})
        require(origin.get("id") == run["id"] and origin.get("head_sha") == run["head_sha"]
                and origin.get("head_branch") == "develop", "artifact=origin")
        require(item.get("expired") is False and timestamp(item["expires_at"]) > now,
                "artifact=expired")
        require(re.fullmatch(r"sha256:[0-9a-f]{64}", item.get("digest", "")), "artifact=digest")
        require(0 < item["size_in_bytes"] <= MAX_ARCHIVE_BYTES, "artifact=size")
    return {item["name"]: item for item in selected}


def _read_artifact(api: Any, item: dict[str, Any], run: dict[str, Any]) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="candidate-") as directory:
        target = Path(directory) / "artifact.zip"
        api.download(item, target)
        require(target.stat().st_size == item["size_in_bytes"], "artifact=zip_size")
        with target.open("rb") as stream:
            require("sha256:" + _hash(stream) == item["digest"], "artifact=zip_digest")
        with zipfile.ZipFile(target) as archive:
            names = archive.namelist()
            require(len(names) == len(set(names)) and len(names) <= 10, "artifact=zip_entries")
            require(sum(info.file_size for info in archive.infolist()) <= MAX_ARCHIVE_BYTES,
                    "artifact=uncompressed_size")
            published = item["name"].startswith("production-published-")
            hashes = _checksum_files(archive, published)
            if published:
                require(set(names) == {"manifest.json", "manifest.sha256"}, "published=files")
                return {"published": json.loads(_small(archive, "manifest.json"))}
            _payload_identity(archive, run)
            return _payload_content(archive, item["name"], hashes)


def _payload_identity(archive: zipfile.ZipFile, run: dict[str, Any]) -> None:
    expected = {"source.sha": run["head_sha"], "run.id": str(run["id"]),
                "run.attempt": str(run["run_attempt"])}
    for name, value in expected.items():
        require(_small(archive, name) == (value + "\n").encode("ascii"),
                "artifact=payload_identity")


def _payload_content(archive: zipfile.ZipFile, name: str, hashes: dict[str, str]) -> dict[str, Any]:
    identity = {"source.sha", "run.id", "run.attempt"}
    if name == "production-dashboard":
        require(set(hashes) == identity | {"dist.tar"}, "dashboard=files")
        return {"dashboard_sha256": hashes["dist.tar"]}
    if name == "production-config":
        require(set(hashes) == identity | {"config.tar"}, "config=files")
        with archive.open("config.tar") as stream:
            return {"config_sha256": hashes["config.tar"], "config": _config_hashes(stream)}
    require(set(hashes) == identity | {"image.tar", "image.id", "sbom.cdx.json", "scan.json"},
            "oci=files")
    require(re.fullmatch(rb"sha256:[0-9a-f]{64}\n", _small(archive, "image.id")), "oci=image_id")
    return {"hashes": hashes, "scan_config": json.loads(_small(archive, "scan.json"))
            .get("Metadata", {}).get("ImageID")}


def _config_hashes(stream: IO[bytes]) -> dict[str, str]:
    result, names = {}, set()
    with tarfile.open(fileobj=stream, mode="r|*") as archive:
        for member in archive:
            require(member.name not in names and not member.name.startswith("/")
                    and ".." not in Path(member.name).parts, "config=unsafe_path")
            names.add(member.name)
            require(member.isdir() or member.isfile(), "config=unsafe_type")
            require(len(names) <= 1000 and member.size <= MAX_METADATA_BYTES, "config=member_size")
            if member.name not in CONFIG_PATHS.values():
                continue
            payload = archive.extractfile(member)
            if payload is None:
                raise ValueError("config=missing_content")
            with payload:
                result[member.name] = _hash(payload)
    require(set(result) == set(CONFIG_PATHS.values()), "config=lock_missing")
    return result


def _images(payloads: dict[str, Any], run: dict[str, Any], repository: str) -> dict[str, Any]:
    result = {}
    fields = {"schema_version", "source_sha", "run_id", "run_attempt", "image", "digest",
              "config_digest", "image_archive_sha256", "sbom_sha256"}
    for app in APPS:
        published = payloads[f"production-published-{app}"]["published"]
        oci = payloads[f"production-oci-{app}"]
        require(set(published) == fields and published["schema_version"] == 1, "published=fields")
        require((published["source_sha"], published["run_id"], published["run_attempt"])
                == (run["head_sha"], str(run["id"]), str(run["run_attempt"])), "published=identity")
        require(published["image"] == f"ghcr.io/{repository.lower()}/{app}", "published=repository")
        require(re.fullmatch(r"sha256:[0-9a-f]{64}", published["digest"]), "published=digest")
        require(published["config_digest"] == oci["scan_config"], "published=config")
        require(published["image_archive_sha256"] == oci["hashes"]["image.tar"],
                "published=archive")
        require(published["sbom_sha256"] == oci["hashes"]["sbom.cdx.json"], "published=sbom")
        result[app] = {key: published[key] for key in
                       ("config_digest", "image_archive_sha256", "sbom_sha256")}
        result[app]["image"] = published["image"] + "@" + published["digest"]
    return result


def _ecr_repository(api: Any, sha: str) -> tuple[str, dict[str, str]]:
    sources = {}
    for path in SOURCE_PATHS:
        item = api.get(f"contents/{path}?ref={sha}")
        require(item.get("type") == "file" and item.get("encoding") == "base64", "ecr=source")
        sources[path] = base64.b64decode(item["content"].replace("\n", ""), validate=True)
    prefix = re.findall(rb'\bname_prefix\s*=\s*"([a-z0-9-]+)"', sources[SOURCE_PATHS[0]])
    module = re.findall(rb'module\s+"processor_registry"\s*\{([^}]+)\}', sources[SOURCE_PATHS[1]])
    require(len(prefix) == 1 and len(module) == 1, "ecr=source_contract")
    require(re.search(rb'source\s*=\s*"../../modules/processor-registry"', module[0])
            and re.search(rb'name_prefix\s*=\s*local\.name_prefix\b', module[0]), "ecr=module")
    require(re.search(rb'\bname\s*=\s*"\$\{var.name_prefix\}-processor"', sources[SOURCE_PATHS[2]]),
            "ecr=repository_contract")
    return prefix[0].decode() + "-processor", {
        path: hashlib.sha256(value).hexdigest() for path, value in sources.items()
    }


def collect_candidate(repository: str, sha: str, api: Any,
                      now: datetime | None = None) -> dict[str, Any]:
    """Returns: candidato validado; Raises: ValueError antes de qualquer saída persistente."""
    require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository), "repository=format")
    require(re.fullmatch(r"[0-9a-f]{40}", sha), "source=format")
    current = now or datetime.now(UTC)
    run = _verification(api, sha)
    pull = _pull_request(api, sha, repository)
    artifacts = _artifact_metadata(api, run, current)
    ecr_name, source_hashes = _ecr_repository(api, sha)
    payloads = {name: _read_artifact(api, item, run) for name, item in sorted(artifacts.items())}
    config = payloads["production-config"]["config"]
    expiry = min([timestamp(run["updated_at"]) + timedelta(days=30)]
                 + [timestamp(item["expires_at"]) for item in artifacts.values()])
    manifest = {"schema_version": 1, "repository": repository, "source_sha": sha,
                "release_id": f"candidate-{sha}-{run['id']}-{run['run_attempt']}",
                "created_at": run["updated_at"],
                "expires_at": expiry.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "verification": {"id": run["id"], "attempt": run["run_attempt"],
                                 "check_suite_id": run["check_suite_id"],
                                 "completed_at": run["updated_at"]}, "pull_request": pull,
                "expected_ecr_repository": ecr_name, "ecr_source_sha256": source_hashes,
                "images": _images(payloads, run, repository),
                "dashboard_sha256": payloads["production-dashboard"]["dashboard_sha256"],
                "config_sha256": payloads["production-config"]["config_sha256"],
                "compose_sha256": config[CONFIG_PATHS["compose"]],
                "locks": {key: config[CONFIG_PATHS[key]]
                          for key in ("opentofu", "python", "dashboard")},
                "artifacts": {name: {"id": item["id"], "sha256": item["digest"][7:],
                                     "expires_at": item["expires_at"], "source_sha": sha,
                                     "run_id": run["id"], "run_attempt": run["run_attempt"]}
                              for name, item in artifacts.items()}}
    require(api.get(f"actions/runs/{run['id']}") == run, "verification=changed_during_collection")
    data = canonical(manifest)
    verify_candidate(data, hashlib.sha256(data).hexdigest(), current)
    return manifest


def build_candidate(manifest: dict[str, Any], output: Path, now: datetime | None = None) -> None:
    """Args: candidato, destino e relógio opcional; valida tudo antes de escrever."""
    data = canonical(manifest)
    checksum = hashlib.sha256(data).hexdigest()
    verify_candidate(data, checksum, now)
    require(not output.exists() and not output.with_suffix(output.suffix + ".sha256").exists(),
            "output=already_exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(data)
    output.with_suffix(output.suffix + ".sha256").write_text(checksum + "\n", encoding="ascii")


def main() -> None:
    """Args: repositório, SHA de origem e arquivo de saída pela linha de comando."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    manifest = collect_candidate(args.repository, args.source_sha, GitHub(args.repository))
    build_candidate(manifest, args.output)
    logging.info("action=build_candidate result=success")


if __name__ == "__main__":
    main()
