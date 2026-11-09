"""Constrói activation pós-Phase A e renderiza somente quatro inputs aprovados."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import io
import json
import re
import stat
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

LIMIT = 192 * 1024**2
REPOSITORY = "VINIClUS/CnesData"


def _require(condition: Any, reason: str) -> None:
    if not condition:
        raise ValueError("activation=" + reason)


def match_fields(observed: dict[str, Any], expected: dict[str, Any], reason: str) -> None:
    """Raises: ValueError se valores ou tipos de metadados divergem."""
    _require(
        all(type(observed.get(k)) is type(v) and observed[k] == v for k, v in expected.items()),
        reason,
    )


def _canonical(value: Any) -> bytes:
    return (
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        + "\n"
    ).encode()


def _verifier(governance: dict[str, Any]) -> Any:
    root = Path(__file__).parent
    path = root / "verify_activation.py"
    entries = governance.get("consumer", {}).get("files", [])
    expected = [item for item in entries if item.get("path") == "release/verify_activation.py"]
    _require(len(expected) == 1 and path.is_file() and not path.is_symlink(), "closure_file")
    raw = path.read_bytes()
    _require(hashlib.sha256(raw).hexdigest() == expected[0]["sha256"], "closure_digest")
    spec = importlib.util.spec_from_file_location("activation_verifier", path)
    if spec is None or spec.loader is None:
        raise ValueError("activation=closure_module")
    module = importlib.util.module_from_spec(spec)
    exec(compile(raw, str(path), "exec"), module.__dict__)  # noqa: S102
    module.sibling("verify_activation", governance)
    return module


def read_metadata(api: Any, route: str, *, raw: bool = False) -> Any:
    """Returns: bytes/metadados da API autenticada com limites de transporte."""
    try:
        path = route if route.startswith("repos/") else "repos/" + REPOSITORY + "/" + route
        value = api.read(path, raw=raw, maximum=LIMIT)
    except Exception:
        raise ValueError("activation=artifact_unavailable") from None
    _require(type(value) is bytes if raw else type(value) is dict, "artifact_response")
    if raw:
        _require(0 < len(value) <= LIMIT, "artifact_size")
    return value


def _json(raw: bytes, *, require_canonical: bool = True) -> Any:
    def pairs(items):
        value = {}
        for key, item in items:
            _require(key not in value, "duplicate_json_key")
            value[key] = item
        return value

    def invalid(_):
        raise ValueError("activation=json_number")

    _require(0 < len(raw) <= 262144, "json_size")
    try:
        value = json.loads(
            raw, object_pairs_hook=pairs, parse_float=invalid, parse_constant=invalid
        )
        expected = _canonical(value)
        _require(not require_canonical or raw == expected, "noncanonical_json")
        return value
    except (ValueError, RecursionError, UnicodeError):
        raise ValueError("activation=json_invalid") from None


def _members(raw: bytes) -> dict[str, bytes]:
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            entries = archive.infolist()
            names = archive.namelist()
            _require(0 < len(entries) <= 65 and len(names) == len(set(names)), "artifact_members")
            _require(sum(item.file_size for item in entries) <= LIMIT, "artifact_inflated")
            _require(
                all(
                    re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", item.filename)
                    and ".." not in item.filename
                    and not item.is_dir()
                    and stat.S_IFMT(item.external_attr >> 16) != stat.S_IFLNK
                    for item in entries
                ),
                "artifact_path",
            )
            return {item.filename: archive.read(item) for item in entries}
    except (zipfile.BadZipFile, RuntimeError):
        raise ValueError("activation=artifact_zip") from None


def _date(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    _require(parsed.strftime("%Y-%m-%dT%H:%M:%SZ") == value, "timestamp")
    return parsed


def _artifact(ref: dict[str, Any], api: Any, now: datetime) -> dict[str, bytes]:
    repository = ref.get("repository", REPOSITORY)
    _require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository), "artifact_repository")
    prefix = "repos/" + repository + "/"
    run_route = prefix + f"actions/runs/{ref['run_id']}/attempts/{ref['run_attempt']}"
    run = read_metadata(api, run_route)
    match_fields(
        run,
        {
            "id": int(ref["run_id"]),
            "run_attempt": int(ref["run_attempt"]),
            "head_sha": ref["workflow_sha"],
            "path": ref["workflow_path"],
            "head_branch": "develop" if repository == REPOSITORY else "main",
            "status": "completed",
            "conclusion": "success",
        },
        "artifact_producer",
    )
    _require(
        run.get("repository", {}).get("full_name") == repository
        and run.get("event") in {"workflow_dispatch", "push"},
        "artifact_producer",
    )
    route = prefix + f"actions/artifacts/{ref['artifact_id']}"
    artifact = read_metadata(api, route)
    origin = artifact.get("workflow_run", {})
    match_fields(
        artifact,
        {"id": int(ref["artifact_id"]), "digest": ref["artifact_digest"], "expired": False},
        "artifact_origin",
    )
    expected_origin = {key: run[key] for key in ("id", "head_sha", "head_branch")}
    match_fields(origin, expected_origin, "artifact_origin")
    _require(_date(artifact["created_at"]) <= now < _date(artifact["expires_at"]), "expiry")
    _require(
        type(artifact["size_in_bytes"]) is int and 0 < artifact["size_in_bytes"] <= LIMIT,
        "artifact_size",
    )
    raw = read_metadata(api, route + "/zip", raw=True)
    _require(
        len(raw) == artifact["size_in_bytes"]
        and "sha256:" + hashlib.sha256(raw).hexdigest() == ref["artifact_digest"],
        "artifact_digest",
    )
    members = _members(raw)
    _require(read_metadata(api, run_route) == run, "artifact_run_changed")
    return members


def authenticate_inputs(inputs: dict[str, Any], api: Any, now: datetime) -> tuple[bytes, bytes]:
    """Returns: bytes originais de candidato/Phase A autenticados na API e ZIP."""
    candidate = _artifact(inputs["candidate_reference"], api, now)
    _require(set(candidate) == {"candidate.json", "candidate.json.sha256"}, "candidate_members")
    raw = candidate["candidate.json"]
    _require(
        candidate["candidate.json.sha256"] == (hashlib.sha256(raw).hexdigest() + "\n").encode(),
        "candidate_sidecar",
    )
    phase_a = _artifact(inputs["phase_a_reference"], api, now)
    _require(set(phase_a) == {"phase-a-evidence.json"}, "phase_a_members")
    return raw, phase_a["phase-a-evidence.json"]


def _checksums(members: dict[str, bytes]) -> None:
    sidecar = "manifest.sha256" if "manifest.sha256" in members else "SHA256SUMS"
    _require(sidecar in members, "r1_sidecar")
    entries = {}
    for line in members[sidecar].decode("ascii").splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9_.-]+)", line)
        if match is None:
            raise ValueError("activation=r1_checksum")
        checksum, name = match.groups()
        _require(name not in entries, "r1_duplicate")
        entries[name] = checksum
    _require(set(entries) == set(members) - {sidecar}, "r1_members")
    _require(
        all(
            hashlib.sha256(members[name]).hexdigest() == checksum
            for name, checksum in entries.items()
        ),
        "r1_digest",
    )


def _r1(candidate: dict[str, Any], api: Any, now: datetime) -> bytes:
    verification = candidate["verification"]
    _require(len(candidate["artifacts"]) == 6, "r1_artifacts")
    source = candidate["source_sha"]
    run = read_metadata(
        api, f"actions/runs/{verification['id']}/attempts/{verification['attempt']}"
    )
    _require(
        run.get("head_sha") == source
        and run.get("conclusion") == "success"
        and run.get("check_suite_id") == verification["check_suite_id"]
        and run.get("event") == "push"
        and run.get("head_branch") == "develop"
        and run.get("path") == ".github/workflows/verify-production.yml",
        "r1_run",
    )
    scans = {}
    for name, item in candidate["artifacts"].items():
        _require(
            item["source_sha"] == source
            and item["run_id"] == verification["id"]
            and item["run_attempt"] == verification["attempt"]
            and _date(item["expires_at"]) > now,
            "r1_binding",
        )
        ref = {
            "run_id": item["run_id"],
            "run_attempt": item["run_attempt"],
            "artifact_id": item["id"],
            "artifact_digest": "sha256:" + item["sha256"],
            "workflow_sha": source,
            "workflow_path": run["path"],
        }
        metadata = read_metadata(api, f"actions/artifacts/{item['id']}")
        _require(metadata.get("name") == name, "r1_name")
        members = _artifact(ref, api, now)
        _checksums(members)
        _r1_payload(candidate, name, members)
        scans[name] = members.get("scan.json", b"")
    return scans["production-oci-data_processor"]


def _r1_payload(candidate: dict[str, Any], name: str, members: dict[str, bytes]) -> None:
    verification = candidate["verification"]
    if name.startswith("production-published-"):
        app = name.removeprefix("production-published-")
        image = candidate["images"][app]
        published = _json(members["manifest.json"], require_canonical=False)
        expected = {k: image[k] for k in ("config_digest", "image_archive_sha256", "sbom_sha256")}
        expected.update(
            source_sha=candidate["source_sha"],
            run_id=str(verification["id"]),
            run_attempt=str(verification["attempt"]),
        )
        match_fields(published, expected, "published_binding")
        _require(
            published["image"] + "@" + published["digest"] == image["image"], "published_binding"
        )
        return
    identity = {
        "source.sha": candidate["source_sha"],
        "run.id": str(verification["id"]),
        "run.attempt": str(verification["attempt"]),
    }
    _require(
        all(members[key] == (value + "\n").encode() for key, value in identity.items()),
        "r1_payload_identity",
    )
    hashes = {key: hashlib.sha256(raw).hexdigest() for key, raw in members.items()}
    if name in {"production-dashboard", "production-config"}:
        key, filename = (
            ("dashboard_sha256", "dist.tar")
            if name == "production-dashboard"
            else ("config_sha256", "config.tar")
        )
        _require(candidate[key] == hashes[filename], "r1_payload_digest")
        return
    image = candidate["images"][name.removeprefix("production-oci-")]
    _require(
        image["image_archive_sha256"] == hashes["image.tar"]
        and image["sbom_sha256"] == hashes["sbom.cdx.json"],
        "r1_payload_digest",
    )


def _index(index: dict[str, Any], proof: dict[str, Any], status: str) -> None:
    admission = index["admission_proof"]
    _require(
        admission["candidate_sha256"] == proof["candidate_sha256"]
        and admission["plan_binding_sha256"] == proof["plan_binding_sha256"]
        and admission["source"]
        == {
            "repository": REPOSITORY,
            "ref": "refs/heads/develop",
            "source_sha": proof["source_sha"],
        },
        "admission_binding",
    )
    match_fields(
        index,
        {
            "kind": "cnesdata-private-plan-transport",
            "index_version": 1,
            "status": status,
            "purpose": "phase-a",
            "source_sha": proof["source_sha"],
            "repository": proof["repository"],
            "cost": proof["cost"],
            "plan_binding_sha256": proof["plan_binding_sha256"],
        },
        "phase_a_index",
    )
    _require(
        index["policy"]["verdict"] == "pass"
        and index["drift"]["verdict"] == ("pass" if status == "planned" else "clean"),
        "phase_a_index",
    )
    _require(_date(index["expires_at"]) >= _date(proof["expires_at"]), "expiry")


def authenticate_evidence(
    proof: dict[str, Any], candidate: dict[str, Any], api: Any, now: datetime
) -> bytes:
    """Raises: ValueError se alguma referência R1/A3 pública falha; nunca reconstrói OCI."""
    _fingerprints(proof)
    scan = _r1(candidate, api, now)
    references = proof["references"]
    _require(set(references) == {"candidate", "planned", "applied", "publication"}, "references")
    artifacts = {}
    for name in ("planned", "applied", "publication"):
        ref = references[name]
        _require(
            ref["repository"] == REPOSITORY and ref["source_sha"] == proof["source_sha"],
            "phase_a_source",
        )
        artifacts[name] = _artifact(ref, api, now)
    indexes = {name: _json(artifacts[name]["index.json"]) for name in ("planned", "applied")}
    for name, index in indexes.items():
        _index(index, proof, name)
        _reports(index, artifacts[name], now)
        _dependencies(index["admission_proof"]["artifacts"], api, now)
    _require(
        artifacts["planned"]["redacted-plan.json"] == artifacts["applied"]["redacted-plan.json"],
        "phase_a_plan_changed",
    )
    receipt = _json(artifacts["publication"]["receipt.json"])
    applied = references["applied"]
    match_fields(
        receipt,
        {
            "receipt_version": 1,
            "result": "success",
            "repository": REPOSITORY,
            "run_id": applied["run_id"],
            "run_attempt": applied["run_attempt"],
            "index_artifact_id": applied["artifact_id"],
            "index_artifact_digest": applied["artifact_digest"],
            "envelope_sha256": indexes["applied"]["envelope_sha256"],
        },
        "phase_a_checkpoint",
    )
    _require(_date(receipt["verified_at"]) <= now, "phase_a_checkpoint")
    for key in ("lease_evidence_sha256", "checkpoint_sha256"):
        _require(re.fullmatch(r"[0-9a-f]{64}", receipt[key]), "phase_a_checkpoint")
    return scan


def _fingerprints(proof: dict[str, Any]) -> None:
    for key in ("resource_fingerprints", "grant_fingerprints", "route_fingerprints"):
        addresses = [item["address"] for item in proof[key]]
        _require(addresses == sorted(set(addresses)), "phase_a_fingerprints")


def _reports(index: dict[str, Any], members: dict[str, bytes], now: datetime) -> None:
    proof = index["admission_proof"]
    reports, checks = proof["reports"], proof["checks"]
    _require(len(reports) == len(checks) and 0 < len(reports) <= 64, "admission_reports")
    _require(
        set(members)
        == {"index.json", "redacted-plan.json"} | {report["name"] for report in reports},
        "admission_members",
    )
    for report, check in zip(reports, checks, strict=True):
        raw = members[report["name"]]
        value = _json(raw)
        _require(
            len(raw) == report["size"]
            and hashlib.sha256(raw).hexdigest() == report["sha256"]
            and report["check_id"] == value["check_id"] == check["check_id"]
            and value["source_sha"] == index["source_sha"]
            and check["result"] == "pass"
            and check["expected_sha256"] == check["observed_sha256"]
            and _date(check["observed_at"]) <= now < _date(check["expires_at"]),
            "admission_report",
        )
        measurement = _canonical(value["measurement"])
        _require(
            hashlib.sha256(measurement).hexdigest() == check["observed_sha256"],
            "admission_measurement",
        )


def _dependencies(items: list[dict[str, Any]], api: Any, now: datetime) -> None:
    _require(type(items) is list and 0 < len(items) <= 32, "admission_dependencies")
    purposes = [item["purpose"] for item in items]
    _require(purposes == sorted(set(purposes)), "admission_dependencies")
    for item in items:
        _require(_date(item["expires_at"]) > now, "expiry")
        members = _artifact(item["reference"], api, now)
        manifest = [
            {"name": name, "size": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
            for name, raw in sorted(members.items())
        ]
        raw = _canonical(manifest)
        _require(hashlib.sha256(raw).hexdigest() == item["content_sha256"], "admission_dependency")


def prepare_ext2(
    request: dict[str, Any], *, github: Any, governance: dict[str, Any], now: datetime
) -> bytes:
    """Returns: payload pós-Phase A na ABI privada ratificada EXT2."""
    return _verifier(governance).prepare_payload(
        request, github=github, governance=governance, now=now
    )


def render_inputs(fields: dict[str, Any], retained: list[str]) -> dict[str, Any]:
    """Returns: somente as quatro variáveis aprovadas; Raises: ValueError para ARN inválido."""
    revisions = fields["revisions"]
    arn = (
        r"arn:aws:ecs:us-east-2:([0-9]{{12}}):task-definition/"
        r"cnesdata-prod-processor-{}:[1-9][0-9]*"
    )
    _require(
        type(revisions) is dict and set(revisions) == {"unit", "recovery", "audit"}, "revisions"
    )
    for name, value in revisions.items():
        _require(type(value) is str and re.fullmatch(arn.format(name), value), "revision_arn")
    _require(
        type(retained) is list
        and len(retained) <= 128
        and all(
            type(value) is str and re.fullmatch(arn.format("unit"), value) for value in retained
        ),
        "retained_unit_only",
    )
    unit = revisions["unit"]
    _require(unit in retained, "active_must_be_retained")
    account = str(unit).split(":")[4]
    _require(
        all(str(value).split(":")[4] == account for value in [*revisions.values(), *retained]),
        "revision_account",
    )
    return {
        "processor_active_unit_task_definition_arn": unit,
        "processor_retained_unit_task_definition_arns": sorted(set(retained)),
        "scheduler_recovery_task_definition_arn": revisions["recovery"],
        "scheduler_audit_task_definition_arn": revisions["audit"],
    }


def render_ext2(
    verified: Any, *, request: dict[str, Any], governance: dict[str, Any]
) -> dict[str, Any]:
    """Returns: inputs após VerifiedActivation autenticada; preserva payload original."""
    gate = importlib.import_module("tools.cnesdata_delivery.signing_gate")
    _require(type(verified) is gate.VerifiedActivation, "verified_activation_required")
    verifier = _verifier(governance)
    _require(governance["provenance"]["workflow_sha"] == verifier.EXT2_SHA, "producer_pin")
    _require(
        hashlib.sha256(verified.signing_spki_der).hexdigest()
        == governance["signing"]["spki_sha256"],
        "signing_spki",
    )
    gate.verify_signature(
        verified.payload_bytes,
        verified.signature_der,
        verified.signing_spki_der,
        governance=governance,
    )
    value = verifier.loads(verified.payload_bytes)
    _require(
        verified.authenticated_reference == request["ext2_post_upload_receipt_v1"]
        or request["mode"] == "activation",
        "receipt_binding",
    )
    if request["inputs"] is not None:
        _require(value["inputs"] == request["inputs"], "input_binding")
    return render_inputs(value, value["inputs"]["retained_unit_task_definition_arns"])
