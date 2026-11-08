"""Autentica candidatos e promove manifests e blobs sem reconstrução."""

from __future__ import annotations

import base64
import hashlib
import json
import re
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from release.build_candidate import collect_candidate
from release.verify_candidate import canonical, require, timestamp, verify_candidate

type Node = dict[str, Any]
MANIFEST = "application/vnd.docker.distribution.manifest.v2+json"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
MEDIA = {
    MANIFEST: (
        "application/vnd.docker.container.image.v1+json",
        "application/vnd.docker.image.rootfs.diff.tar.gzip",
    ),
    OCI_MANIFEST: (
        "application/vnd.oci.image.config.v1+json",
        "application/vnd.oci.image.layer.v1.tar+gzip",
    ),
}
DOMAIN = b"cnesdata:prod:canary:v1\0"


def sha256(data: bytes) -> str:
    """Returns: SHA-256 hexadecimal dos bytes exatos."""
    return hashlib.sha256(data).hexdigest()


def canary_binding(candidate: Node, candidate_sha256: str) -> Node:
    """Returns: identidade A2 sem alterar o release_id original."""
    require(bool(re.fullmatch(r"[0-9a-f]{64}", candidate_sha256)), "candidate=hash")
    value = {key: candidate[key] for key in ("repository", "release_id", "source_sha")}
    value["candidate_sha256"] = candidate_sha256
    require(all(type(item) is str for item in value.values()), "canary=string_values")
    data = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    digest = sha256(DOMAIN + data)
    return value | {
        "derivation": "cnesdata:prod:canary:v1",
        "binding_sha256": digest,
        "canary_id": digest[:32],
    }


def _run(api: Any, reference: Node, creator: Node) -> Node:
    run = api.get(f"actions/runs/{reference['run_id']}")
    require(
        (run.get("id"), run.get("run_attempt")) == (reference["run_id"], reference["run_attempt"]),
        "candidate=run_attempt",
    )
    require(
        run.get("status") == "completed"
        and run.get("conclusion") == "success"
        and run.get("head_branch") == "develop"
        and run.get("event") == "workflow_dispatch"
        and run.get("path") == ".github/workflows/release-candidate.yml",
        "candidate=producer",
    )
    require(
        run.get("repository", {}).get("full_name") == reference["repository"],
        "candidate=repository",
    )
    require(
        set(creator) == {"workflow_path", "workflow_sha"}
        and creator["workflow_path"] == run["path"]
        and re.fullmatch(r"[0-9a-f]{40}", creator["workflow_sha"])
        and creator["workflow_sha"] == run["head_sha"],
        "candidate=creator_code",
    )
    return run


def _candidate_artifact(
    api: Any, reference: Node, now: datetime, creator: Node
) -> tuple[Node, Node]:
    run = _run(api, reference, creator)
    artifact = api.get(f"actions/artifacts/{reference['artifact_id']}")
    require(
        artifact.get("id") == reference["artifact_id"]
        and artifact.get("digest") == reference["artifact_digest"],
        "candidate=artifact",
    )
    origin = artifact.get("workflow_run", {})
    require(
        origin.get("id") == run["id"]
        and origin.get("head_sha") == run["head_sha"]
        and origin.get("head_branch") == "develop",
        "candidate=artifact_origin",
    )
    require(
        artifact.get("expired") is False and timestamp(artifact["expires_at"]) > now,
        "candidate=artifact_expired",
    )
    require(
        type(artifact.get("size_in_bytes")) is int and 0 < artifact["size_in_bytes"] <= 131072,
        "candidate=artifact_size",
    )
    return run, artifact


def _candidate_bytes(api: Any, artifact: Node) -> tuple[bytes, str]:
    with tempfile.TemporaryDirectory(prefix="phase-a-candidate-") as directory:
        target = Path(directory) / "candidate.zip"
        api.download(artifact, target)
        data = target.read_bytes()
        require(
            len(data) == artifact["size_in_bytes"]
            and "sha256:" + sha256(data) == artifact["digest"],
            "candidate=archive_digest",
        )
        with zipfile.ZipFile(target) as archive:
            require(
                sorted(archive.namelist()) == ["candidate.json", "candidate.json.sha256"],
                "candidate=archive_files",
            )
            require(
                all(
                    item.file_size <= 65536
                    and not item.is_dir()
                    and (item.external_attr >> 16) & 0o170000 != 0o120000
                    for item in archive.infolist()
                ),
                "candidate=archive_entries",
            )
            candidate = archive.read("candidate.json")
            sidecar = archive.read("candidate.json.sha256")
    require(bool(re.fullmatch(rb"[0-9a-f]{64}\n", sidecar)), "candidate=sidecar")
    return candidate, sidecar.decode("ascii")


def authenticate_candidate(
    reference: Node, api: Any, creator: Node, now: datetime | None = None
) -> Node:
    """Returns: R2 e R1 autenticados; Raises: ValueError antes de qualquer promoção."""
    require(
        set(reference) == {"repository", "run_id", "run_attempt", "artifact_id", "artifact_digest"},
        "candidate=reference_fields",
    )
    require(reference["repository"] == "VINIClUS/CnesData", "candidate=repository")
    require(
        all(
            type(reference[key]) is int and reference[key] > 0
            for key in ("run_id", "run_attempt", "artifact_id")
        ),
        "candidate=reference_ids",
    )
    current = now or datetime.now(UTC)
    run, artifact = _candidate_artifact(api, reference, current, creator)
    data, checksum = _candidate_bytes(api, artifact)
    candidate = verify_candidate(data, checksum, current)
    require(candidate["repository"] == reference["repository"], "candidate=repository")
    expected_name = (
        f"production-candidate-{candidate['source_sha']}-{run['id']}-{run['run_attempt']}"
    )
    require(artifact.get("name") == expected_name, "candidate=artifact_name")
    rebuilt = collect_candidate(candidate["repository"], candidate["source_sha"], api, current)
    require(canonical(rebuilt) == data, "candidate=r1_bytes")
    require(_run(api, reference, creator) == run, "candidate=run_changed")
    return {
        "candidate": candidate,
        "candidate_sha256": sha256(data),
        "binding": canary_binding(candidate, sha256(data)),
        "provenance": {
            "reference": reference,
            "run": run,
            "artifact": artifact,
            "verification": candidate["verification"],
            "artifacts": candidate["artifacts"],
        },
    }


def _repository(context: Node, registry: Any) -> None:
    target = registry.describe_repository()
    expected = {
        "account_id": context["account_id"],
        "region": context["region"],
        "name": context["expected_ecr_repository"],
        "image_tag_mutability": "IMMUTABLE",
    }
    require(
        expected["name"] == "cnesdata-prod-processor"
        and expected["region"] == "us-east-2"
        and bool(re.fullmatch(r"[0-9]{12}", expected["account_id"])),
        "ecr=context",
    )
    require(all(target.get(key) == value for key, value in expected.items()), "ecr=repository")
    uri = f"{expected['account_id']}.dkr.ecr.{expected['region']}.amazonaws.com/{expected['name']}"
    require(target.get("uri") == uri, "ecr=uri")


def _scan(context: Node) -> None:
    scan = context["scan"]
    require(
        scan["source_sha"] == context["source_sha"]
        and scan["config_digest"] == context["config_digest"]
        and scan["job"] == "oci-build (data_processor)"
        and scan["conclusion"] == "success"
        and scan["threshold"] == ["CRITICAL", "HIGH"],
        "scan=origin",
    )
    require(
        context["release_id"]
        == (f"candidate-{context['source_sha']}-{scan['run_id']}-{scan['run_attempt']}"),
        "scan=run",
    )
    require(
        all(
            bool(re.fullmatch(r"[0-9a-f]{64}", scan[field])) for field in ("sha256", "sbom_sha256")
        ),
        "scan=digest",
    )
    require(
        all(item.get("Severity") not in scan["threshold"] for item in scan["vulnerabilities"]),
        "scan=unreviewed_severity",
    )


def _descriptor(value: Node, media: str) -> None:
    require(
        set(value) == {"mediaType", "digest", "size"} and value["mediaType"] == media,
        "oci=descriptor_media",
    )
    require(
        bool(re.fullmatch(r"sha256:[0-9a-f]{64}", value["digest"]))
        and type(value["size"]) is int
        and 0 < value["size"] <= 2 * 1024**3,
        "oci=descriptor",
    )


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024**2):
            digest.update(chunk)
    return digest.hexdigest()


def _oci(context: Node, registry: Any, directory: Path) -> tuple[bytes, list[tuple[str, Path]]]:
    require(
        context["image"].startswith(
            "ghcr.io/" + context["repository"].lower() + "/data_processor@sha256:"
        ),
        "oci=repository",
    )
    data = registry.read_manifest(context["image"])
    require(
        len(data) <= 4 * 1024**2 and "sha256:" + sha256(data) == context["image"].split("@")[1],
        "oci=manifest_digest",
    )
    manifest = json.loads(data)
    require(
        set(manifest) == {"schemaVersion", "mediaType", "config", "layers"}
        and manifest["schemaVersion"] == 2
        and manifest["mediaType"] in MEDIA,
        "oci=manifest_media",
    )
    config_media, layer_media = MEDIA[manifest["mediaType"]]
    _descriptor(manifest["config"], config_media)
    require(manifest["config"]["digest"] == context["config_digest"], "oci=config_digest")
    require(type(manifest["layers"]) is list and 0 < len(manifest["layers"]) <= 128, "oci=layers")
    for layer in manifest["layers"]:
        _descriptor(layer, layer_media)
    blobs = []
    for item in [manifest["config"], *manifest["layers"]]:
        target = directory / item["digest"].split(":")[1]
        registry.download_blob(context["image"], item["digest"], target)
        require(
            target.is_file()
            and target.stat().st_size == item["size"]
            and "sha256:" + _file_hash(target) == item["digest"],
            "oci=blob_digest",
        )
        blobs.append((item["digest"], target))
    require(blobs[0][1].stat().st_size <= 4 * 1024**2, "oci=config_size")
    config = json.loads(blobs[0][1].read_bytes())
    require(config.get("os") == "linux" and config.get("architecture") == "amd64", "oci=platform")
    return data, blobs


def promote_image(context: Node, registry: Any) -> Node:
    """Returns: cópia exata conferida; contexto externo exige autenticação prévia de R1/R2."""
    _repository(context, registry)
    _scan(context)
    with tempfile.TemporaryDirectory(prefix="phase-a-oci-") as directory:
        manifest, blobs = _oci(context, registry, Path(directory))
        for digest, path in blobs:
            registry.copy_blob(digest, path)
        digest = "sha256:" + sha256(manifest)
        registry.put_manifest(digest, manifest)
        _destination_blobs(registry, blobs, Path(directory))
    observed = registry.describe_image(digest)
    require(observed == {"digest": digest, "manifest": manifest}, "ecr=readback")
    _repository(context, registry)
    return {
        "image_digest": digest,
        "config_digest": context["config_digest"],
        "ecr_image": registry.describe_repository()["uri"] + "@" + digest,
        "manifest_sha256": sha256(manifest),
        "blob_digests": [item[0] for item in blobs],
    }


def _scan_producer(candidate: Node, api: Any) -> Node:
    sha, run = candidate["source_sha"], candidate["verification"]
    response = api.get(f"commits/{sha}/check-runs?filter=all&per_page=100")
    jobs = [
        item
        for item in response["check_runs"]
        if item.get("check_suite", {}).get("id") == run["check_suite_id"]
        and item.get("name") == "oci-build (data_processor)"
    ]
    require(jobs, "scan=job_missing")
    job = max(jobs, key=lambda item: item["id"])
    require(
        job.get("head_sha") == sha
        and job.get("conclusion") == "success"
        and job.get("status") == "completed",
        "scan=job_not_green",
    )
    source = api.get(f"contents/.github/workflows/verify-production.yml?ref={sha}")
    require(source.get("type") == "file" and source.get("encoding") == "base64", "scan=workflow")
    data = base64.b64decode(source["content"].replace("\n", ""), validate=True)
    command = (
        rb"trivy image --input image.tar --scanners vuln\s*\\\n\s*"
        rb"--severity CRITICAL,HIGH --ignore-unfixed --exit-code 1\s*\\\n\s*"
        rb"--ignorefile \.trivyignore --format json --output scan.json"
    )
    require(bool(re.search(command, data)), "scan=threshold_contract")
    return {"job_id": job["id"], "workflow_sha256": sha256(data)}


def authenticated_scan(verified: Node, api: Any) -> Node:
    """Returns: revisão de severidade vinculada ao job R1 e ao artifact exato."""
    candidate = verified["candidate"]
    reference = candidate["artifacts"]["production-oci-data_processor"]
    artifact = api.get(f"actions/artifacts/{reference['id']}")
    require(
        artifact["digest"] == "sha256:" + reference["sha256"]
        and artifact["id"] == reference["id"]
        and artifact["expired"] is False
        and artifact["workflow_run"]["id"] == reference["run_id"]
        and artifact["workflow_run"]["head_sha"] == candidate["source_sha"],
        "scan=artifact",
    )
    with tempfile.TemporaryDirectory(prefix="phase-a-scan-") as directory:
        path = Path(directory) / "artifact.zip"
        api.download(artifact, path)
        require(_file_hash(path) == reference["sha256"], "scan=archive_digest")
        with zipfile.ZipFile(path) as archive:
            require(archive.getinfo("scan.json").file_size <= 4 * 1024**2, "scan=size")
            data = archive.read("scan.json")
    scan = json.loads(data)
    image = candidate["images"]["data_processor"]
    require(scan.get("Metadata", {}).get("ImageID") == image["config_digest"], "scan=config")
    vulnerabilities = [
        item for result in scan.get("Results", []) for item in result.get("Vulnerabilities", [])
    ]
    return _scan_producer(candidate, api) | {
        "sha256": sha256(data),
        "source_sha": candidate["source_sha"],
        "run_id": reference["run_id"],
        "run_attempt": reference["run_attempt"],
        "artifact_id": reference["id"],
        "artifact_sha256": reference["sha256"],
        "config_digest": image["config_digest"],
        "sbom_sha256": image["sbom_sha256"],
        "job": "oci-build (data_processor)",
        "threshold": ["CRITICAL", "HIGH"],
        "conclusion": "success",
        "vulnerabilities": vulnerabilities,
    }


def promote_candidate(reference: Node, api: Any, registry: Any, governed: Node) -> Node:
    """Returns: promoção autenticada completa; nenhum receipt sintético é criado."""
    verified = authenticate_candidate(
        reference, api, governed["candidate_creator"], governed["now"]
    )
    candidate = verified["candidate"]
    image = candidate["images"]["data_processor"]
    context = {
        key: candidate[key]
        for key in ("repository", "source_sha", "release_id", "expected_ecr_repository")
    }
    context |= {key: governed[key] for key in ("account_id", "region")}
    context |= {
        "candidate_sha256": verified["candidate_sha256"],
        "image": image["image"],
        "config_digest": image["config_digest"],
        "scan": authenticated_scan(verified, api),
    }
    result = promote_image(context, registry)
    require(
        _run(api, reference, governed["candidate_creator"]) == verified["provenance"]["run"],
        "candidate=run_changed",
    )
    return result | {
        "binding": verified["binding"],
        "provenance": verified["provenance"],
        "scan": context["scan"],
        "expires_at": candidate["expires_at"],
        "candidate": candidate,
    }


def _destination_blobs(registry: Any, blobs: list[tuple[str, Path]], directory: Path) -> None:
    for index, (digest, source) in enumerate(blobs):
        target = directory / f"ecr-{index}"
        registry.download_destination_blob(digest, target)
        require(
            target.is_file()
            and target.stat().st_size == source.stat().st_size
            and "sha256:" + _file_hash(target) == digest,
            "ecr=blob_readback",
        )


def verify_promoted_image(context: Node, registry: Any) -> None:
    """Raises: ValueError no readback real; não copia, registra ou publica bytes."""
    _repository(context, registry)
    _scan(context)
    with tempfile.TemporaryDirectory(prefix="phase-a-readback-") as directory:
        manifest, blobs = _oci(context, registry, Path(directory))
        digest = "sha256:" + sha256(manifest)
        require(
            registry.describe_image(digest) == {"digest": digest, "manifest": manifest},
            "ecr=readback",
        )
        _destination_blobs(registry, blobs, Path(directory))
