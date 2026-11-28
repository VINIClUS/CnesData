"""Publica a evidência Phase A só depois de autenticar cada campo no envelope e na promoção."""

from __future__ import annotations

import argparse
import base64
import binascii
import gzip
import json
import logging
import os
import re
import zlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen

from release.build_activation import _artifact
from release.verify_activation import loads, sha, timestamp
from release.verify_candidate import require
from scripts.promotion._artifact_origin import artifact_content_hash
from scripts.promotion._canonical import object_hash, parse_document
from scripts.promotion._phase_a_evidence import EVIDENCE_SCHEMA, validate_evidence, validate_schema

type Node = dict[str, Any]
LIMIT = 262144
ENCODED_LIMIT = 65535
DOCUMENTS = ("evidence", "envelope", "promotion")
OUTPUTS = frozenset({
    "processor_task_definition_arns", "image_digest", "active_before", "retained_before",
    "retained_after", "canary_id", "binding_sha256", "resource_fingerprints",
    "grant_fingerprints", "route_fingerprints",
})
RELEASE = ("release_id", "source_sha", "candidate_sha256", "canary_id", "binding_sha256")
logger = logging.getLogger(__name__)


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class GitHubReader:
    """Args: token efêmero do run; Raises: ValueError sem token."""

    def __init__(self, token: str) -> None:
        require(token, "publish=github_token")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def read(self, path: str, *, raw: bool = False, maximum: int = LIMIT) -> Any:
        """Returns: JSON ou bytes da API; nenhum token segue o redirect do ZIP."""
        request = Request("https://api.github.com/" + path, headers=self.headers)
        try:
            response = build_opener(_NoRedirect()).open(request, timeout=60)
        except HTTPError as error:
            require(raw and error.code == 302, "publish=github_status")
            location = error.headers.get("Location", "")
            require(urlparse(location).scheme == "https", "publish=redirect_scheme")
            response = urlopen(location, timeout=120)
        with response:
            data = response.read(maximum + 1)
        require(len(data) <= maximum, "publish=github_size")
        return data if raw else json.loads(data)


def encode_document(raw: bytes) -> str:
    """Returns: transporte gzip+base64 de um documento para input de dispatch."""
    return base64.b64encode(gzip.compress(raw, 9, mtime=0)).decode("ascii")


def decode_document(encoded: str, digest: str) -> bytes:
    """Returns: bytes originais; Raises: ValueError para tamanho, codificação ou SHA-256."""
    require(type(encoded) is str and 0 < len(encoded) <= ENCODED_LIMIT, "publish=encoded_size")
    require(re.fullmatch(r"[0-9a-f]{64}", digest or ""), "publish=document_sha256_format")
    try:
        compressed = base64.b64decode(encoded, validate=True)
        inflater = zlib.decompressobj(wbits=31)
        raw = inflater.decompress(compressed, LIMIT + 1)
    except (binascii.Error, ValueError, zlib.error):
        raise ValueError("publish=document_encoding") from None
    require(len(raw) <= LIMIT, "publish=document_size")
    require(inflater.eof and not inflater.unused_data, "publish=document_encoding")
    require(sha(raw) == digest, "publish=document_sha256")
    return raw


def _match(evidence: Node, expected: Node, reason: str) -> None:
    for key, value in expected.items():
        require(evidence.get(key) == value, f"publish={reason} field={key}")


def _candidate(reference: Node, api: Any, now: datetime) -> Node:
    members = _artifact(reference, api, now)
    require(set(members) == {"candidate.json", "candidate.json.sha256"}, "publish=candidate")
    raw = members["candidate.json"]
    require(members["candidate.json.sha256"] == (sha(raw) + "\n").encode(), "publish=candidate")
    return loads(raw)


def _envelope(evidence: Node, raw: bytes, artifacts: Node, receipt: Node) -> None:
    index = parse_document(artifacts["applied"]["index.json"])
    applied = evidence["references"]["applied"]
    require(
        sha(raw) == index["envelope_sha256"] == receipt["envelope_sha256"]
        and receipt["index_artifact_id"] == applied["artifact_id"]
        and receipt["index_artifact_digest"] == applied["artifact_digest"],
        "publish=envelope_binding",
    )
    envelope = parse_document(raw)
    plan, apply = envelope["plan"], envelope["apply"]
    require(
        envelope["status"] == "applied" and set(apply["outputs"]) == OUTPUTS
        and apply["state_after"]["outputs_sha256"] == object_hash(apply["outputs"]),
        "publish=envelope_shape",
    )
    for name, producer in (("planned", plan["producer"]), ("applied", apply["producer"])):
        reference = evidence["references"][name]
        require(
            (producer["run_id"], producer["run_attempt"])
            == (reference["run_id"], reference["run_attempt"]),
            "publish=envelope_producer",
        )
    expected = dict(apply["outputs"]) | {
        "plan_binding_sha256": envelope["plan_binding_sha256"],
        "state_sha256": apply["state_after"]["state_sha256"],
        "outputs_sha256": apply["state_after"]["outputs_sha256"],
        "expires_at": plan["expires_at"],
        "cost": plan["cost"],
    }
    _match(evidence, expected | {key: plan["release"][key] for key in RELEASE}, "envelope_field")


def _promotion_entry(planned: Node, api: Any, now: datetime) -> Node:
    index = parse_document(planned["index.json"])
    entries = [
        item for item in index["admission_proof"]["artifacts"] if item["purpose"] == "phase-inputs"
    ]
    require(len(entries) == 1, "publish=promotion_manifest")
    members = _artifact(entries[0]["reference"], api, now)
    require(
        set(members) == {"phase-a-public.json"}
        and artifact_content_hash(members) == entries[0]["content_sha256"],
        "publish=promotion_manifest",
    )
    manifest = parse_document(members["phase-a-public.json"])
    objects = [item for item in manifest["objects"] if item["name"] == "promotion.json"]
    require(len(objects) == 1, "publish=promotion_manifest")
    return objects[0] | {"source_sha": manifest["source_sha"]}


def _promotion(evidence: Node, raw: bytes, candidate: Node, entry: Node) -> None:
    require(
        entry["source_sha"] == evidence["source_sha"]
        and entry["sha256"] == sha(raw)
        and entry["size"] == len(raw),
        "publish=promotion_binding",
    )
    promotion = parse_document(raw)
    require(promotion["candidate"] == candidate, "publish=promotion_candidate")
    provenance, scan = promotion["provenance"], promotion["scan"]
    reference = provenance["reference"] | {
        "workflow_path": provenance["run"]["path"],
        "workflow_sha": provenance["run"]["head_sha"],
        "source_sha": candidate["source_sha"],
    }
    require(evidence["references"]["candidate"] == reference, "publish=promotion_field field=ref")
    expected = dict(promotion["binding"]) | {
        "ecr_image": promotion["ecr_image"],
        "image_digest": promotion["image_digest"],
        "source_provenance_sha256": object_hash(provenance),
        "r1_verification": candidate["verification"],
        "r1_artifacts": [
            {"name": name, **item} for name, item in sorted(candidate["artifacts"].items())
        ],
        "promotion": {
            "config_digest": promotion["config_digest"],
            "manifest_sha256": promotion["manifest_sha256"],
            "blob_digests": promotion["blob_digests"],
            "scan_sha256": scan["sha256"],
            "scan_artifact_id": scan["artifact_id"],
            "scan_artifact_sha256": scan["artifact_sha256"],
            "scan_job_id": scan["job_id"],
            "scan_workflow_sha256": scan["workflow_sha256"],
            "sbom_sha256": scan["sbom_sha256"],
        },
    }
    _match(evidence, expected, "promotion_field")


def publish(documents: dict[str, bytes], api: Any, now: datetime) -> bytes:
    """Returns: bytes exatos da evidência; Raises: ValueError se algum campo não autentica."""
    require(set(documents) == set(DOCUMENTS), "publish=documents")
    raw = documents["evidence"]
    evidence = loads(raw)
    validate_schema(evidence, json.loads(EVIDENCE_SCHEMA.read_bytes()))
    references = evidence["references"]
    candidate = _candidate(references["candidate"], api, now)
    artifacts = {name: _artifact(references[name], api, now) for name in ("planned", "applied")}
    publication = _artifact(references["publication"], api, now)
    require(set(publication) == {"receipt.json"}, "publish=receipt")
    receipt = parse_document(publication["receipt.json"])
    require(timestamp(receipt["verified_at"]) <= now, "publish=anchor_after_now")
    require(evidence["created_at"] == receipt["verified_at"], "publish=created_at")
    validate_evidence(evidence, timestamp(receipt["verified_at"]))
    _envelope(evidence, documents["envelope"], artifacts, receipt)
    entry = _promotion_entry(artifacts["planned"], api, now)
    _promotion(evidence, documents["promotion"], candidate, entry)
    return raw


def _now() -> datetime:
    return datetime.now(UTC)


def main(argv: list[str] | None = None) -> None:
    """Args: --output; lê documentos e token do ambiente do job."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    arguments = parser.parse_args(argv)
    documents = {
        name: decode_document(
            os.environ.get(name.upper(), ""), os.environ.get(name.upper() + "_SHA256", "")
        )
        for name in DOCUMENTS
    }
    data = publish(documents, GitHubReader(os.environ.get("GITHUB_TOKEN", "")), _now())
    output = Path(arguments.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(data)
    logger.info("phase_a_evidence=published sha256=%s size=%d", sha(data), len(data))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
