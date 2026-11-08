"""Admissão pública antes do reusable governado, sem identidade ou acesso AWS."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import secrets
import stat
import tempfile
import zipfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from release.build_candidate import REQUIRED_CHECKS, GitHub
from release.verify_candidate import require, timestamp
from scripts.promotion._canonical import parse_document
from scripts.promotion._plan_contract import closed, validate_reference

type Node = dict[str, Any]
REPOSITORY = "VINIClUS/CnesData"
PRODUCER_SHA = "99fc0e4805b96fbca7df06440d5ea3ad42e53369"
PRODUCER_READY = False
CONTRACT = {
    "contract_repository": REPOSITORY,
    "contract_source_sha": "7ebc0e3dc024617b33903577c87dac3ca6fba529",
    "contract_path": "docs/superpowers/specs/2026-11-07-phase-a-contract-amendment.md",
    "contract_sha256": "ad3ceb54750963e708544c591f4bdcb31264f5dc3e91f41ec0b48bfc883ff86f",
}
BASELINES = ["backend", "cost", "drift", "policy", "shared-outputs", "toolchain"]
PURPOSES = {"acm-certificate", "bootstrap", "phase-a"}
LIMIT = 192 * 1024**2
LOGGER = logging.getLogger(__name__)


class PublicGitHub:
    def get(self, repository: str, path: str) -> Any:
        """Returns: metadados públicos autenticados no repositório exato."""
        return self._client(repository).get(path)

    def download(self, repository: str, metadata: Node, target: Path) -> None:
        """Raises: ValueError para tamanho externo diferente do autenticado."""
        self._client(repository).download(metadata, target)
        require(target.stat().st_size == metadata["size_in_bytes"], "artifact=archive_size")

    def _client(self, repository: str) -> GitHub:
        require(re.fullmatch(r"VINIClUS/[A-Za-z0-9_.-]+", repository), "repository=unapproved")
        return GitHub(repository)


def _hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _digest(value: Any) -> None:
    require(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value), "hash=invalid")


def _integer(value: Any) -> int:
    require(type(value) in (str, int) and re.fullmatch(r"[1-9][0-9]*", str(value)), "id=invalid")
    result = int(value)
    require(result <= 2**53 - 1, "id=range")
    return result


def _archive(path: Path) -> Node:
    files: Node = {}
    total = 0
    with zipfile.ZipFile(path) as archive:
        require(0 < len(archive.infolist()) <= 32, "artifact=members")
        for item in archive.infolist():
            name = item.filename
            require(
                re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", name)
                and name not in {".", ".."}
                and name not in files
                and not item.is_dir()
                and not item.external_attr & 16
                and stat.S_IFMT(item.external_attr >> 16) in {0, stat.S_IFREG},
                "artifact=unsafe_member",
            )
            total += item.file_size
            require(item.file_size <= 128 * 1024**2 and total <= LIMIT, "artifact=expanded_size")
            files[name] = archive.read(item)
    return files


def authenticate_reference(reference: Node, api: Any, now: datetime) -> Node:
    """Returns: membros reais após identidade, attempt, expiração e digest externos."""
    validate_reference(reference, reference["repository"])
    repository = reference["repository"]
    require(re.fullmatch(r"VINIClUS/[A-Za-z0-9_.-]+", repository), "reference=repository")
    run = api.get(
        repository, f"actions/runs/{reference['run_id']}/attempts/{reference['run_attempt']}"
    )
    require(
        run["id"] == reference["run_id"]
        and run["run_attempt"] == reference["run_attempt"]
        and run["repository"]["full_name"] == repository
        and run["head_sha"] == reference["source_sha"] == reference["workflow_sha"]
        and run["path"].split("@")[0] == reference["workflow_path"]
        and run["status"] == "completed"
        and run["conclusion"] == "success",
        "artifact=run_identity",
    )
    metadata = api.get(repository, f"actions/artifacts/{reference['artifact_id']}")
    require(
        metadata["id"] == reference["artifact_id"]
        and metadata["digest"] == reference["artifact_digest"]
        and metadata["expired"] is False
        and timestamp(metadata["expires_at"]) > now
        and metadata["workflow_run"]["id"] == reference["run_id"]
        and metadata["workflow_run"]["head_sha"] == reference["source_sha"],
        "artifact=metadata",
    )
    require(
        timestamp(run["run_started_at"])
        < timestamp(metadata["created_at"])
        < timestamp(run["updated_at"]),
        "artifact=attempt_boundary",
    )
    require(
        type(metadata["size_in_bytes"]) is int and 0 < metadata["size_in_bytes"] <= LIMIT,
        "artifact=size",
    )
    with tempfile.TemporaryDirectory(prefix="cnesdata-public-") as directory:
        path = Path(directory) / "artifact.zip"
        api.download(repository, metadata, path)
        require(path.stat().st_size == metadata["size_in_bytes"], "artifact=archive_size")
        require("sha256:" + _hash(path.read_bytes()) == metadata["digest"], "artifact=digest")
        return _archive(path)


def _source(request: Node, api: Any) -> None:
    source = request["expected_source_sha"]
    run = api.get(REPOSITORY, f"actions/runs/{request['run_id']}/attempts/{request['run_attempt']}")
    require(
        run["id"] == request["run_id"]
        and run["run_attempt"] == request["run_attempt"]
        and run["repository"]["full_name"] == REPOSITORY
        and run["head_sha"] == source
        and run["head_branch"] == "develop"
        and run["event"] == "workflow_dispatch"
        and run["path"].split("@")[0] == f".github/workflows/infra-{request['mode']}.yml",
        "caller=source",
    )
    compare = api.get(REPOSITORY, f"compare/{source}...develop")
    require(compare["status"] in {"ahead", "identical"}, "source=not_develop")
    rules = api.get(REPOSITORY, "rules/branches/develop")
    required = {
        check["context"]
        for rule in rules
        if rule["type"] == "required_status_checks"
        for check in rule["parameters"]["required_status_checks"]
    }
    require(required == REQUIRED_CHECKS, "checks=rules_changed")
    checks = api.get(REPOSITORY, f"commits/{source}/check-runs?per_page=100")
    require(checks["total_count"] <= 100, "checks=pagination_required")
    latest: Node = {}
    for check in checks["check_runs"]:
        if check["name"] not in latest or check["id"] > latest[check["name"]]["id"]:
            latest[check["name"]] = check
    require(
        all(
            name in latest
            and latest[name]["head_sha"] == source
            and latest[name]["status"] == "completed"
            and latest[name]["conclusion"] == "success"
            for name in required
        ),
        "checks=source_not_green",
    )


def _descriptor(request: Node) -> Node:
    raw = request["request_descriptor"].encode("utf-8")
    require(
        0 < len(raw) <= 65536 and _hash(raw) == request["request_descriptor_sha256"],
        "descriptor=digest_size",
    )
    descriptor = closed(
        parse_document(raw),
        "schema_version repository source_sha producer_pins baseline_artifacts payload_artifact",
    )
    require(
        type(descriptor["schema_version"]) is int
        and descriptor["schema_version"] == 1
        and descriptor["repository"] == REPOSITORY
        and descriptor["source_sha"] == request["expected_source_sha"],
        "descriptor=source",
    )
    pins = descriptor["producer_pins"]
    require(type(pins) is list and 0 < len(pins) <= 32, "descriptor=pins")
    identities = []
    for pin in pins:
        pin = closed(pin, "repository source_sha content_sha256")
        _digest(pin["content_sha256"])
        require(re.fullmatch(r"[0-9a-f]{40}", pin["source_sha"]), "pin=source")
        identities.append((pin["repository"], pin["source_sha"]))
    require(
        identities == sorted(set(identities))
        and ("VINIClUS/infra-ansible", PRODUCER_SHA) in identities,
        "descriptor=producer_pin",
    )
    baselines = descriptor["baseline_artifacts"]
    require(type(baselines) is list and len(baselines) == 6, "descriptor=baselines")
    require(
        [closed(x, "purpose reference")["purpose"] for x in baselines] == BASELINES,
        "descriptor=baseline_purposes",
    )
    return descriptor


def _payload(request: Node, descriptor: Node) -> Node | None:
    payload = descriptor["payload_artifact"]
    if request["mode"] == "plan":
        require(
            not any(request.get(key) for key in ("plan_run_id", "artifact_id", "artifact_digest")),
            "payload=unexpected_apply_transport",
        )
    if request["purpose"] != "phase-a":
        require(
            not any(
                request.get("phase_inputs_" + key)
                for key in ("run_id", "artifact_id", "artifact_digest")
            ),
            "payload=unexpected_phase_transport",
        )
    prefix = "plan_" if request["mode"] == "apply" else "phase_inputs_"
    required = request["mode"] == "apply" or request["purpose"] == "phase-a"
    keys = (prefix + "run_id", "artifact_id", "artifact_digest")
    if prefix == "phase_inputs_":
        keys = tuple(prefix + key for key in ("run_id", "artifact_id", "artifact_digest"))
    if not required:
        require(payload is None and not any(request.get(key) for key in keys), "payload=initial")
        return None
    require(type(payload) is dict, "payload=missing")
    validate_reference(payload)
    require(payload["source_sha"] == request["expected_source_sha"], "payload=source")
    require(
        _integer(request[keys[0]]) == payload["run_id"]
        and _integer(request[keys[1]]) == payload["artifact_id"]
        and request[keys[2]] == payload["artifact_digest"],
        "payload=transport",
    )
    if request["mode"] == "apply":
        require(payload["workflow_path"] == ".github/workflows/infra-plan.yml", "payload=caller")
    return payload


def _cost(cost: Node, purpose: str, now: datetime) -> None:
    common = "verdict evidence_sha256 manifest_sha256 product_monthly_usd "
    common += "aggregate_monthly_usd paid_actions_limit_usd calculated_at expires_at "
    scope = cost["scope"]
    extra = (
        "scope cloudfront"
        if scope == "active"
        else ("scope cloudfront activation dns workloads unbound_monthly_usd")
    )
    closed(cost, common + extra)
    require(cost["verdict"] == "pass" and cost["paid_actions_limit_usd"] == "0.00", "cost=verdict")
    for field in ("evidence_sha256", "manifest_sha256"):
        _digest(cost[field])
    amounts = [cost[field] for field in ("product_monthly_usd", "aggregate_monthly_usd")]
    require(
        all(type(x) is str and re.fullmatch(r"(0|[1-9][0-9]*)\.[0-9]{2}", x) for x in amounts),
        "cost=money",
    )
    require(Decimal(amounts[0]) <= 8 and Decimal(amounts[1]) < 15, "cost=budget")
    require(timestamp(cost["calculated_at"]) <= now < timestamp(cost["expires_at"]), "cost=expiry")
    if scope == "active":
        cloud = closed(
            cost["cloudfront"],
            "plan status subscription_sha256 distribution_sha256 "
            "waf_binding_sha256 observed_at expires_at",
        )
        require(cloud["plan"] == "FREE" and cloud["status"] == "ACTIVE", "cost=cloudfront")
        require(
            timestamp(cloud["observed_at"]) <= now < timestamp(cloud["expires_at"]),
            "cost=cloudfront_expiry",
        )
        for field in ("subscription_sha256", "distribution_sha256", "waf_binding_sha256"):
            _digest(cloud[field])
    else:
        require(
            scope == "pre-activation"
            and purpose != "phase-a"
            and cost["cloudfront"] == {"plan": None, "status": None}
            and all(cost[x] == "blocked" for x in ("activation", "dns", "workloads")),
            "cost=pre_scope",
        )
        require(
            type(cost["unbound_monthly_usd"]) is str
            and re.fullmatch(r"(0|[1-9][0-9]*)\.[0-9]{2}", cost["unbound_monthly_usd"]),
            "cost=unbound",
        )


def _planned(files: Node, reference: Node, request: Node, now: datetime) -> Node:
    require("index.json" in files and len(files["index.json"]) <= 32768, "index=missing_size")
    index = closed(
        parse_document(files["index.json"]),
        "index_version kind status repository source_sha producer purpose created_at "
        "expires_at plan_binding_sha256 envelope_sha256 package_aggregate_sha256 "
        "private_locator policy cost drift admission_proof",
    )
    _index_identity(index, reference, request)
    remaining = (timestamp(index["expires_at"]) - now).total_seconds()
    require(timestamp(index["created_at"]) <= now and remaining >= 1980, "index=validity_margin")
    require(
        (timestamp(index["expires_at"]) - timestamp(index["created_at"])).total_seconds() <= 3600,
        "index=ttl",
    )
    for field in ("policy", "drift"):
        verdict = closed(index[field], "verdict evidence_sha256")
        require(verdict["verdict"] == "pass", "index=verdict")
        _digest(verdict["evidence_sha256"])
    _cost(index["cost"], request["purpose"], now)
    require(
        timestamp(index["expires_at"]) <= timestamp(index["cost"]["expires_at"]),
        "index=cost_expiry",
    )
    if index["cost"]["scope"] == "active":
        require(
            timestamp(index["expires_at"]) <= timestamp(index["cost"]["cloudfront"]["expires_at"]),
            "index=free_expiry",
        )
    return index


def _index_identity(index: Node, reference: Node, request: Node) -> None:
    require(
        type(index["index_version"]) is int
        and index["index_version"] == 1
        and index["kind"] == "cnesdata-private-plan-transport"
        and index["status"] == "planned"
        and index["repository"] == REPOSITORY
        and index["source_sha"] == request["expected_source_sha"]
        and index["purpose"] == request["purpose"],
        "index=identity",
    )
    require(
        index["producer"]
        == {k: reference[k] for k in ("workflow_path", "workflow_sha", "run_id", "run_attempt")},
        "index=producer",
    )
    require(
        index["private_locator"]
        == {
            "scope": "cnesdata-prod",
            "source_sha": reference["source_sha"],
            "run_id": reference["run_id"],
            "run_attempt": reference["run_attempt"],
            "status": "planned",
        },
        "index=locator",
    )
    for field in ("plan_binding_sha256", "envelope_sha256", "package_aggregate_sha256"):
        _digest(index[field])


def _phase_transport(request: Node, index: Node, api: Any, now: datetime) -> None:
    entries = index["admission_proof"]["artifacts"]
    inputs = [entry["reference"] for entry in entries if entry["purpose"] == "phase-inputs"]
    require(len(inputs) == 1, "phase_inputs=unique_reference")
    reference = inputs[0]
    validate_reference(reference)
    require(
        reference["source_sha"] == request["expected_source_sha"]
        and _integer(request["phase_inputs_run_id"]) == reference["run_id"]
        and _integer(request["phase_inputs_artifact_id"]) == reference["artifact_id"]
        and request["phase_inputs_artifact_digest"] == reference["artifact_digest"],
        "phase_inputs=transport",
    )
    authenticate_reference(reference, api, now)


def prepare_request(request: Node, api: Any, now: datetime) -> Node:
    """Returns: ABI pública preliminar; o reusable repete a admissão governada completa."""
    fields = {
        "mode",
        "purpose",
        "expected_source_sha",
        "request_descriptor",
        "request_descriptor_sha256",
        "run_id",
        "run_attempt",
    }
    optional = {
        "plan_run_id",
        "artifact_id",
        "artifact_digest",
        "phase_inputs_run_id",
        "phase_inputs_artifact_id",
        "phase_inputs_artifact_digest",
    }
    require(fields <= request.keys() <= fields | optional, "request=fields")
    require(
        request["mode"] in {"plan", "apply"} and request["purpose"] in PURPOSES,
        "request=mode_purpose",
    )
    require(re.fullmatch(r"[0-9a-f]{40}", request["expected_source_sha"]), "request=source")
    require(len(json.dumps(request, ensure_ascii=False)) <= 65535, "request=transport_size")
    _integer(request["run_id"])
    _integer(request["run_attempt"])
    descriptor = _descriptor(request)
    _source(request, api)
    for entry in descriptor["baseline_artifacts"]:
        authenticate_reference(entry["reference"], api, now)
    payload = _payload(request, descriptor)
    if payload is not None:
        files = authenticate_reference(payload, api, now)
        if request["mode"] == "apply":
            index = _planned(files, payload, request, now)
            if request["purpose"] == "phase-a":
                _phase_transport(request, index, api, now)
    return CONTRACT | {
        key: request[key]
        for key in (
            "request_descriptor",
            "request_descriptor_sha256",
            "expected_source_sha",
            "purpose",
        )
    }


def main() -> None:
    """Raises: SystemExit antes do reusable se faltar pin integrado de P1/P2."""
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("plan", "apply"))
    mode = parser.parse_args().mode
    try:
        require(PRODUCER_READY, "producer=p1_p2_integrated_pin_pending")
        require(
            os.environ["GITHUB_REPOSITORY"] == REPOSITORY
            and os.environ["GITHUB_REF"] == "refs/heads/develop"
            and os.environ["GITHUB_EVENT_NAME"] == "workflow_dispatch",
            "caller=context",
        )
        request = json.loads(os.environ["REQUEST_JSON"])
        request.update(
            mode=mode,
            run_id=_integer(os.environ["GITHUB_RUN_ID"]),
            run_attempt=_integer(os.environ["GITHUB_RUN_ATTEMPT"]),
        )
        prepared = prepare_request(request, PublicGitHub(), datetime.now(UTC))
        with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as output:
            for key, value in prepared.items():
                delimiter = secrets.token_hex(32)
                output.write(f"{key}<<{delimiter}\n{value}\n{delimiter}\n")
    except (ValueError, KeyError, OSError, zipfile.BadZipFile) as error:
        LOGGER.error("admission=closed reason=%s", type(error).__name__)
        raise SystemExit(2) from None


if __name__ == "__main__":
    main()
