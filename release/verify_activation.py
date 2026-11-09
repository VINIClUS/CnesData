"""Admissão hermética da activation e verificação do carrier EXT2 autenticado."""

from __future__ import annotations

import base64
import hashlib
import importlib
import importlib.util
import json
import re
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

EXT2_SHA = "d6c2fe101f5a5c2287aabc83ed73dd9f9b95f41f"
REPOSITORY = "VINIClUS/CnesData"
FIELD_NAMES = (
    "project environment schema_version repository release_id source_sha "
    "candidate_sha256 binding_sha256 canary_id derivation revisions ecr_uri "
    "phase_a_evidence_digest"
)
FIELDS = frozenset(FIELD_NAMES.split())
INPUTS = frozenset(
    ["candidate_reference", "phase_a_reference", "retained_unit_task_definition_arns"]
)
REFERENCE_NAMES = (
    "repository run_id run_attempt artifact_id artifact_digest workflow_path "
    "workflow_sha source_sha"
)
REFERENCES = frozenset(REFERENCE_NAMES.split())


CANDIDATE_FIELD_NAMES = (
    "schema_version repository source_sha release_id created_at expires_at "
    "verification pull_request expected_ecr_repository images dashboard_sha256 "
    "config_sha256 compose_sha256 locks ecr_source_sha256 artifacts"
)
PHASE_A_SCHEMA_SHA256 = "3cb6b4f012d96493ecafdabcc4e8923453b2f43bb9d1a37f38cc2e039797d017"


def require(condition: Any, reason: str) -> None:
    """Raises: ValueError quando um vínculo obrigatório falha."""
    if not condition:
        raise ValueError("activation=" + reason)


def canonical(value: Any) -> bytes:
    """Returns: bytes JSON UTF-8 canônicos com newline."""
    return (
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        + "\n"
    ).encode()


def sha(data: bytes) -> str:
    """Returns: SHA-256 hexadecimal dos bytes originais."""
    return hashlib.sha256(data).hexdigest()


def closed(value: Any, fields: Any) -> dict[str, Any]:
    """Returns: objeto fechado; Raises: ValueError para campos extras/ausentes."""
    require(type(value) is dict and set(value) == set(fields), "fields")
    return value


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in items:
        require(key not in result, "duplicate_json_key")
        result[key] = value
    return result


def _invalid(_: str) -> None:
    raise ValueError("activation=json_number")


def _bounded(value: Any, depth: int = 0) -> int:
    require(depth <= 24, "json_depth")
    if type(value) is int:
        require(abs(value) <= 9007199254740991, "json_integer")
    children = value.values() if type(value) is dict else value if type(value) is list else []
    count = 1 + sum(_bounded(child, depth + 1) for child in children)
    require(count <= 4096, "json_nodes")
    return count


def loads(raw: bytes) -> Any:
    """Returns: JSON limitado e canônico; Raises: ValueError para ambiguidades."""
    require(type(raw) is bytes and 0 < len(raw) <= 262144, "json_size")
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_pairs,
            parse_constant=_invalid,
            parse_float=_invalid,
        )
        _bounded(value)
        require(canonical(value) == raw, "noncanonical_json")
        return value
    except (UnicodeError, json.JSONDecodeError, RecursionError, OverflowError):
        raise ValueError("activation=json_encoding") from None


def timestamp(value: Any) -> datetime:
    """Returns: instante UTC explícito; Raises: ValueError para formato inválido."""
    require(
        type(value) is str and re.fullmatch(r"20\d{2}-\d\d-\d\dT\d\d:\d\d:\d\dZ", value),
        "timestamp",
    )
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def valid_time(value: dict[str, Any], now: datetime) -> None:
    """Raises: ValueError se os limites de validade não incluem now."""
    require(timestamp(value["created_at"]) <= now < timestamp(value["expires_at"]), "expiry")


def sibling(name: str, governance: dict[str, Any]) -> Any:
    """Returns: módulo irmão autenticado da closure exata, sem cache Python."""
    config = closed(governance.get("consumer"), {"repository", "source_sha", "files"})
    names = {
        "release/" + n
        for n in ("activation.schema.json", "build_activation.py", "verify_activation.py")
    }
    require(
        config["repository"] == REPOSITORY
        and config["source_sha"] == governance["provenance"]["source_sha"],
        "closure_source",
    )
    entries: Any = config["files"]
    require(
        type(entries) is list
        and len(entries) == 3
        and {closed(item, {"path", "sha256"})["path"] for item in entries} == names,
        "closure",
    )
    root = Path(__file__).parent
    for raw_entry in entries:
        entry = closed(raw_entry, {"path", "sha256"})
        path = root / Path(entry["path"]).name
        require(path.is_file() and not path.is_symlink(), "closure_file")
        require(sha(path.read_bytes()) == entry["sha256"], "closure_digest")
    target = root / (name + ".py")
    require("release/" + target.name in names, "closure_module")
    spec = importlib.util.spec_from_file_location("activation_" + name, target)
    if spec is None or spec.loader is None:
        raise ValueError("activation=closure_module")
    module = importlib.util.module_from_spec(spec)
    exec(compile(target.read_bytes(), str(target), "exec"), module.__dict__)  # noqa: S102
    return module


def _schema_scalar(value: Any, rule: dict[str, Any]) -> None:
    types = rule["type"] if type(rule["type"]) is list else [rule["type"]]
    mapping = {"object": dict, "array": list, "string": str, "integer": int, "null": type(None)}
    require(type(value) in [mapping[item] for item in types], "schema_type")
    if "const" in rule:
        require(value == rule["const"], "schema_context")
    if type(value) is str:
        require(
            rule.get("minLength", 0) <= len(value) <= rule.get("maxLength", len(value)),
            "schema_length",
        )
        require("pattern" not in rule or re.fullmatch(rule["pattern"], value), "schema_pattern")
    if type(value) is int:
        require(rule.get("minimum", value) <= value <= rule.get("maximum", value), "schema_range")


def _schema(value: Any, rule: dict[str, Any], definitions: dict[str, Any]) -> None:
    if "$ref" in rule:
        rule = definitions[rule["$ref"].rsplit("/", 1)[1]]
    _schema_scalar(value, rule)
    if type(value) is dict:
        closed(value, rule["required"])
        for key, item in value.items():
            _schema(item, rule["properties"][key], definitions)
    elif type(value) is list:
        require(
            rule.get("minItems", 0) <= len(value) <= rule.get("maxItems", len(value)),
            "schema_array_length",
        )
        if rule.get("uniqueItems"):
            require(len({canonical(item) for item in value}) == len(value), "schema_duplicate")
        for item in value:
            _schema(item, rule["items"], definitions)


def _reference(value: Any, source: str) -> dict[str, Any]:
    ref = closed(value, REFERENCES)
    require(ref["repository"] == REPOSITORY and ref["source_sha"] == source, "reference_source")
    for key in ("run_id", "run_attempt", "artifact_id"):
        require(
            type(ref[key]) is str and re.fullmatch(r"[1-9][0-9]{0,15}", ref[key]), "reference_id"
        )
    require(
        re.fullmatch(r"[0-9a-f]{40}", ref["workflow_sha"])
        and re.fullmatch(r"sha256:[0-9a-f]{64}", ref["artifact_digest"])
        and re.fullmatch(r"\.github/workflows/[A-Za-z0-9_-]+\.yml", ref["workflow_path"]),
        "reference_pin",
    )
    return ref


def normalize_reference(value: dict[str, Any]) -> dict[str, Any]:
    """Returns: referência com IDs decimais normalizados sem coerção de booleanos."""
    result = dict(value)
    for key in ("run_id", "run_attempt", "artifact_id"):
        require(type(result[key]) in (int, str), "reference_id")
        result[key] = str(result[key])
    return result


def _candidate(candidate: dict[str, Any], now: datetime) -> None:
    closed(candidate, CANDIDATE_FIELD_NAMES.split())
    valid_time(candidate, now)
    verification = candidate["verification"]
    expected = f"candidate-{candidate['source_sha']}-{verification['id']}-{verification['attempt']}"
    require(
        candidate["release_id"] == expected and candidate["repository"] == REPOSITORY,
        "candidate_identity",
    )
    require(
        candidate["schema_version"] == 1 and type(candidate["schema_version"]) is int,
        "candidate_schema",
    )
    for name, image in closed(candidate["images"], {"central_api", "data_processor"}).items():
        prefix = f"ghcr.io/{REPOSITORY.lower()}/{name}@sha256:"
        require(
            re.fullmatch(re.escape(prefix) + r"[0-9a-f]{64}", image["image"]), "candidate_image"
        )


def _phase_a(proof: dict[str, Any], candidate: dict[str, Any], now: datetime) -> None:
    data = json.loads(Path(__file__).with_name("activation.schema.json").read_bytes())
    schema = data["x-consumed-schemas"]["phase-a"]["schema"]
    require(sha(canonical(schema)) == PHASE_A_SCHEMA_SHA256, "consumed_schema_pin")
    _schema(proof, schema, schema["$defs"])
    valid_time(proof, now)
    require(timestamp(proof["expires_at"]) <= timestamp(candidate["expires_at"]), "expiry")
    require(
        proof["r1_verification"] == candidate["verification"]
        and proof["r1_artifacts"]
        == [{"name": k, **v} for k, v in sorted(candidate["artifacts"].items())],
        "r1_binding",
    )
    unit = proof["processor_task_definition_arns"]["unit"]
    retained = proof["retained_before"]
    require(
        all(
            [
                type(retained) is list,
                len(retained) <= 128,
                retained == sorted(set(retained)),
                proof["retained_after"] == sorted(set(retained) | {unit}),
                proof["active_before"] is None or proof["active_before"] in retained,
            ]
        ),
        "phase_a_retained",
    )
    _iam_cost(proof, now)
    promotion = proof["promotion"]
    image = candidate["images"]["data_processor"]
    require(
        proof["image_digest"] == image["image"].rsplit("@", 1)[1]
        and proof["ecr_image"].endswith(
            "/" + candidate["expected_ecr_repository"] + "@" + image["image"].rsplit("@", 1)[1]
        )
        and promotion["manifest_sha256"] == image["image"].rsplit("@sha256:", 1)[1]
        and promotion["config_digest"] == image["config_digest"]
        and promotion["sbom_sha256"] == image["sbom_sha256"],
        "promotion_binding",
    )


def _iam_cost(proof: dict[str, Any], now: datetime) -> None:
    iam = proof["iam"]
    first, second = Decimal(iam["first_readback_seconds"]), Decimal(iam["second_readback_seconds"])
    require(0 <= first < second < 300 and second - first >= 10, "iam_readback")
    cost = proof["cost"]
    require(
        all(
            [
                Decimal(cost["product_monthly_usd"]) <= 8,
                Decimal(cost["aggregate_monthly_usd"]) < 15,
                timestamp(cost["calculated_at"]) <= now,
                timestamp(proof["expires_at"]) <= timestamp(cost["expires_at"]),
                timestamp(proof["expires_at"]) <= timestamp(cost["cloudfront"]["expires_at"]),
            ]
        ),
        "cost",
    )


def _r2_provenance(proof: dict[str, Any], candidate: dict[str, Any], api: Any, helper: Any) -> None:
    reference = proof["references"]["candidate"]
    run = helper.read_metadata(api, f"actions/runs/{reference['run_id']}")
    helper.match_fields(
        run,
        {
            "id": reference["run_id"],
            "run_attempt": reference["run_attempt"],
            "event": "workflow_dispatch",
            "path": ".github/workflows/release-candidate.yml",
            "head_sha": reference["workflow_sha"],
        },
        "candidate_producer",
    )
    artifact = helper.read_metadata(api, f"actions/artifacts/{reference['artifact_id']}")
    require(artifact.get("size_in_bytes", 0) <= 131072, "candidate_size")
    name = f"production-candidate-{candidate['source_sha']}-{run['id']}-{run['run_attempt']}"
    require(artifact.get("name") == name, "candidate_name")
    locator = {
        key: reference[key]
        for key in ("repository", "run_id", "run_attempt", "artifact_id", "artifact_digest")
    }
    provenance = {
        "reference": locator,
        "run": run,
        "artifact": artifact,
        "verification": candidate["verification"],
        "artifacts": candidate["artifacts"],
    }
    require(
        sha(canonical(provenance)) == proof["source_provenance_sha256"],
        "source_provenance",
    )


def _scan(proof: dict[str, Any], candidate: dict[str, Any], raw: bytes, api: Any) -> None:
    source, verification = candidate["source_sha"], candidate["verification"]
    prefix = "repos/" + REPOSITORY + "/"
    require(0 < len(raw) <= 4 * 1024**2, "scan_size")
    scan = json.loads(raw, object_pairs_hook=_pairs, parse_constant=_invalid)
    image = candidate["images"]["data_processor"]
    require(scan.get("Metadata", {}).get("ImageID") == image["config_digest"], "scan_binding")
    checks = api.read(prefix + f"commits/{source}/check-runs?filter=all&per_page=100")
    jobs = [
        item
        for item in checks["check_runs"]
        if item.get("check_suite", {}).get("id") == verification["check_suite_id"]
        and item.get("name") == "oci-build (data_processor)"
    ]
    require(jobs, "scan_job_missing")
    job = max(jobs, key=lambda item: item["id"])
    require(
        job.get("head_sha") == source
        and job.get("status") == "completed"
        and job.get("conclusion") == "success",
        "scan_job_not_green",
    )
    document = api.read(prefix + f"contents/.github/workflows/verify-production.yml?ref={source}")
    require(document.get("type") == "file" and document.get("encoding") == "base64", "scan_source")
    workflow = base64.b64decode(document["content"].replace("\n", ""), validate=True)
    command = (
        rb"trivy image --input image.tar --scanners vuln\s*\\\n\s*"
        rb"--severity CRITICAL,HIGH --ignore-unfixed --exit-code 1\s*\\\n\s*"
        rb"--ignorefile \.trivyignore --format json --output scan.json"
    )
    require(re.search(command, workflow), "scan_threshold_contract")
    artifact = candidate["artifacts"]["production-oci-data_processor"]
    expected = {
        "scan_artifact_id": artifact["id"],
        "scan_artifact_sha256": artifact["sha256"],
        "scan_sha256": sha(raw),
        "scan_job_id": job["id"],
        "scan_workflow_sha256": sha(workflow),
    }
    require(
        all(proof["promotion"][key] == value for key, value in expected.items()), "scan_binding"
    )


def activation_fields(
    candidate: dict[str, Any], proof: dict[str, Any], raw: tuple[bytes, bytes]
) -> dict[str, Any]:
    """Returns: os treze campos exatos admitidos por EXT2."""
    binding_input = {k: candidate[k] for k in ("repository", "release_id", "source_sha")}
    binding_input["candidate_sha256"] = sha(raw[0])
    binding = sha(b"cnesdata:prod:canary:v1\0" + canonical(binding_input)[:-1])
    fields = {
        "project": "cnesdata",
        "environment": "prod",
        "schema_version": 1,
        **binding_input,
        "binding_sha256": binding,
        "canary_id": binding[:32],
        "derivation": "cnesdata:prod:canary:v1",
        "revisions": proof["processor_task_definition_arns"],
        "ecr_uri": proof["ecr_image"],
        "phase_a_evidence_digest": sha(raw[1]),
    }
    require(
        all(
            proof[k] == fields[k]
            for k in (*binding_input, "binding_sha256", "canary_id", "derivation", "schema_version")
        ),
        "phase_a_binding",
    )
    return fields


def prepare_payload(
    request: dict[str, Any], *, github: Any, governance: dict[str, Any], now: datetime
) -> bytes:
    """Returns: payload canônico só depois de autenticar candidato e Phase A."""
    helper = sibling("build_activation", governance)
    inputs = closed(request["inputs"], INPUTS)
    source = governance["provenance"]["source_sha"]
    require(request["source_sha"] == source and request["mode"] == "activation", "request_source")
    for ref in (inputs["candidate_reference"], inputs["phase_a_reference"]):
        _reference(ref, source)
    candidate_raw, proof_raw = helper.authenticate_inputs(inputs, github, now)
    require(len(candidate_raw) <= 65536, "candidate_size")
    candidate, proof = loads(candidate_raw), loads(proof_raw)
    _candidate(candidate, now)
    _phase_a(proof, candidate, now)
    fields = activation_fields(candidate, proof, (candidate_raw, proof_raw))
    require(candidate["source_sha"] == source, "candidate_source")
    require(
        normalize_reference(proof["references"]["candidate"]) == inputs["candidate_reference"],
        "candidate_reference",
    )
    require(
        inputs["retained_unit_task_definition_arns"] == proof["retained_after"], "retained_binding"
    )
    _r2_provenance(proof, candidate, github, helper)
    scan = helper.authenticate_evidence(proof, candidate, github, now)
    _scan(proof, candidate, scan, github)
    payload = fields | {"inputs": inputs}
    schema = json.loads(Path(__file__).with_name("activation.schema.json").read_bytes())
    _schema(payload, schema, schema["$defs"])
    helper.render_inputs(fields, inputs["retained_unit_task_definition_arns"])
    return canonical(payload)


def admit_ext2(
    payload: bytes,
    *,
    request: dict[str, Any],
    github: Any,
    governance: dict[str, Any],
    now: datetime,
    historical: bool = False,
) -> dict[str, Any]:
    """Returns: bytes autenticados e FIELDS13; Raises: ValueError para mistura/expiração."""
    value = closed(loads(payload), FIELDS | {"inputs"})
    require(historical is (request["mode"] == "historical"), "request_mode")
    if historical:
        require(request["inputs"] is None, "historical_inputs")
        selected = request | {
            "inputs": value["inputs"],
            "mode": "activation",
            "source_sha": governance["provenance"]["source_sha"],
        }
    else:
        require(request["inputs"] == value["inputs"], "input_binding")
        selected = request
    expected = prepare_payload(selected, github=github, governance=governance, now=now)
    require(payload == expected, "payload_binding")
    helper = sibling("build_activation", governance)
    candidate, proof = helper.authenticate_inputs(value["inputs"], github, now)
    return {
        "candidate_bytes": candidate,
        "phase_a_bytes": proof,
        "activation_fields": {key: value[key] for key in FIELDS},
    }


def verify_activation(
    *,
    github: Any,
    s3: Any,
    governance: dict[str, Any],
    now: datetime,
    ext2_post_upload_receipt_v1: Any,
    historical: bool = False,
) -> Any:
    """Returns: VerifiedActivation após autenticar carrier, três versões e assinatura."""
    require(type(ext2_post_upload_receipt_v1) is dict, "receipt_required")
    require(governance["provenance"]["workflow_sha"] == EXT2_SHA, "producer_pin")
    gate = importlib.import_module("tools.cnesdata_delivery.signing_gate")

    return gate.verify_ext2_reference(
        ext2_post_upload_receipt_v1,
        github=github,
        s3=s3,
        governance=governance,
        now=now,
        historical=historical,
    )
