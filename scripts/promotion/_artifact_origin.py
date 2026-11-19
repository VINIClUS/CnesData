"""Autentica bytes, metadados e fontes de artifacts governados."""

from __future__ import annotations

import base64
import re
import tempfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from release.verify_candidate import require, timestamp
from scripts.promotion._plan_contract import validate_reference
from scripts.promotion.promote_ecr import _file_hash, authenticate_candidate, sha256

if TYPE_CHECKING:
    from datetime import datetime

type Node = dict[str, Any]


def _artifact_origin(reference: Node, api: Any, governed: Node) -> tuple[Node, Node]:
    validate_reference(reference, governed["repository"])
    require(reference["source_sha"] == governed["source_sha"], "artifact=source")
    run = api.get(f"actions/runs/{reference['run_id']}")
    require(
        run.get("repository", {}).get("full_name") == reference["repository"]
        and run.get("id") == reference["run_id"]
        and run.get("run_attempt") == reference["run_attempt"]
        and run.get("head_sha") == reference["source_sha"] == reference["workflow_sha"]
        and run.get("path") == reference["workflow_path"]
        and run.get("head_branch") == governed["branch"]
        and run.get("event") == governed["event"]
        and run.get("status") == "completed"
        and run.get("conclusion") == "success",
        "artifact=producer_identity",
    )
    caller = {key: reference[key] for key in ("workflow_path", "workflow_sha")}
    require(caller in governed["callers"], "artifact=caller_not_governed")
    expected = governed["reusables"][reference["workflow_path"]]
    if expected:
        require(
            re.fullmatch(r"[0-9a-f]{40}", expected["sha"])
            and expected["path"].endswith("@" + expected["sha"]),
            "artifact=immutable_reusable",
        )
    require(
        run.get("referenced_workflows", []) == ([expected] if expected else []),
        "artifact=reusable_pin",
    )
    artifact = api.get(f"actions/artifacts/{reference['artifact_id']}")
    origin = artifact.get("workflow_run", {})
    require(
        artifact.get("id") == reference["artifact_id"]
        and artifact.get("digest") == reference["artifact_digest"]
        and origin.get("id") == reference["run_id"]
        and origin.get("head_sha") == reference["source_sha"]
        and origin.get("head_branch") == governed["branch"]
        and artifact.get("expired") is False,
        "artifact=origin",
    )
    require(
        type(artifact.get("size_in_bytes")) is int
        and 0 < artifact["size_in_bytes"] <= 192 * 1024**2,
        "artifact=size",
    )
    return run, artifact


def _artifact_members(api: Any, artifact: Node) -> dict[str, bytes]:
    with tempfile.TemporaryDirectory(prefix="phase-a-receipt-") as directory:
        path = Path(directory) / "artifact.zip"
        api.download(artifact, path)
        require(
            path.stat().st_size == artifact["size_in_bytes"]
            and "sha256:" + _file_hash(path) == artifact["digest"],
            "artifact=external_digest",
        )
        with zipfile.ZipFile(path) as archive:
            items = archive.infolist()
            names = archive.namelist()
            require(len(names) == len(set(names)) and 0 < len(names) <= 65, "artifact=members")
            require(
                sum(item.file_size for item in items) <= 192 * 1024**2, "artifact=inflated_size"
            )
            require(
                all(
                    re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", name) and ".." not in name
                    for name in names
                ),
                "artifact=path",
            )
            require(
                all(
                    not item.is_dir() and (item.external_attr >> 16) & 0o170000 != 0o120000
                    for item in items
                ),
                "artifact=links",
            )
            return {name: archive.read(name) for name in names}


def authenticate_artifacts(references: Node, api: Any, governed: Node, now: datetime) -> Node:
    """Returns: metadados e bytes públicos autenticados, sem confiar em digest autodeclarado."""
    require(
        len({item["artifact_id"] for item in references.values()}) == len(references),
        "artifact=duplicate_reference",
    )
    result = {}
    for name, reference in references.items():
        run, artifact = _artifact_origin(reference, api, governed)
        require(
            timestamp(artifact["created_at"]) <= now < timestamp(artifact["expires_at"]),
            "artifact=expired",
        )
        members = _artifact_members(api, artifact)
        require(api.get(f"actions/runs/{reference['run_id']}") == run, "artifact=run_changed")
        result[name] = {
            "reference": reference,
            "run": run,
            "artifact": artifact,
            "members": members,
        }
    return result


def authenticate_sources(entries: list[Node], api: Any, source_sha: str) -> Node:
    """Returns: bytes da fonte imutável autenticada para a prova do grafo original."""
    require(re.fullmatch(r"[0-9a-f]{40}", source_sha), "source=sha")
    result = {}
    for item in entries:
        path = item["path"]
        require(path.startswith("infra/opentofu/") and ".." not in path.split("/"), "source=path")
        response = api.get(f"contents/{path}?ref={source_sha}")
        require(
            response.get("type") == "file" and response.get("encoding") == "base64", "source=type"
        )
        data = base64.b64decode(response["content"].replace("\n", ""), validate=True)
        require(len(data) <= 4 * 1024**2 and sha256(data) == item["sha256"], "source=hash")
        result[path] = data
    return result


def artifact_content_hash(members: Node) -> str:
    """Returns: hash canônico de TODOS os membros do artifact anterior, separado do ZIP."""
    from scripts.promotion._canonical import object_hash

    manifest = [
        {"name": name, "size": len(data), "sha256": sha256(data)}
        for name, data in sorted(members.items())
    ]
    return object_hash(manifest)


def authenticate_r2_artifact(reference: Node, api: Any, creator: Node, now: datetime) -> Node:
    """Returns: apenas o R2 nomeado pode separar creator HEAD e fonte histórica."""
    validate_reference(reference)
    keys = ("repository", "run_id", "run_attempt", "artifact_id", "artifact_digest")
    verified = authenticate_candidate({key: reference[key] for key in keys}, api, creator, now)
    source = verified["provenance"]
    require(
        reference["source_sha"] == verified["candidate"]["source_sha"]
        and reference["workflow_path"] == source["run"]["path"]
        and reference["workflow_sha"] == source["run"]["head_sha"],
        "candidate=prior_creator_binding",
    )
    return {"members": _artifact_members(api, source["artifact"]), **source}
