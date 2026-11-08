"""Emissão e validação da evidência Phase A, sem criar outro contrato de transporte."""

from __future__ import annotations

import json
import re
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

from release.verify_candidate import require, timestamp
from scripts.promotion._canonical import (
    STATE_LIMIT,
    canonical_bytes,
    decode_json,
    object_hash,
    parse_document,
)
from scripts.promotion._plan_contract import _hashes, closed
from scripts.promotion.promote_ecr import canary_binding, sha256

if TYPE_CHECKING:
    from datetime import datetime

type Node = dict[str, Any]
EVIDENCE_SCHEMA = Path(__file__).resolve().parents[2] / "release/phase-a-evidence.schema.json"


def _schema_scalar(value: Any, rule: Node) -> None:
    if "const" in rule:
        require(value == rule["const"], "evidence=const")
    if type(value) is str:
        require(len(value) >= rule.get("minLength", 0), "evidence=string_length")
        require("pattern" not in rule or re.fullmatch(rule["pattern"], value), "evidence=pattern")
    if type(value) is int:
        require(rule.get("minimum", value) <= value <= rule.get("maximum", value), "evidence=range")


def _schema_array(value: list[Any], rule: Node, definitions: Node) -> None:
    require(
        rule.get("minItems", 0) <= len(value) <= rule.get("maxItems", len(value)),
        "evidence=array_length",
    )
    if rule.get("uniqueItems"):
        require(len({canonical_bytes(item) for item in value}) == len(value), "evidence=duplicate")
    for item in value:
        _schema_value(item, rule["items"], definitions)


def _schema_value(value: Any, rule: Node, definitions: Node) -> None:
    if "$ref" in rule:
        rule = definitions[rule["$ref"].rsplit("/", 1)[1]]
    types = rule["type"] if type(rule["type"]) is list else [rule["type"]]
    mapping = {"object": dict, "array": list, "string": str, "integer": int, "null": type(None)}
    require(type(value) in [mapping[item] for item in types], "evidence=type")
    _schema_scalar(value, rule)
    if type(value) is dict:
        require(set(value) == set(rule["required"]), "evidence=fields")
        for key, item in value.items():
            _schema_value(item, rule["properties"][key], definitions)
    elif type(value) is list:
        _schema_array(value, rule, definitions)


def validate_schema(value: Any, schema: Node) -> None:
    """Raises: ValueError para valores fora do schema de evidência local."""
    _schema_value(value, schema, schema["$defs"])


def validate_evidence(value: Node, now: datetime) -> None:
    """Raises: ValueError para evidência expirada, ambígua ou fora do schema versionado."""
    schema = json.loads(EVIDENCE_SCHEMA.read_bytes())
    validate_schema(value, schema)
    require(
        timestamp(value["created_at"]) <= now < timestamp(value["expires_at"]), "evidence=expiry"
    )
    binding = canary_binding(value, value["candidate_sha256"])
    require(
        all(value[key] == binding[key] for key in ("canary_id", "binding_sha256", "derivation")),
        "evidence=canary_binding",
    )
    proof = value["iam"]
    first, second = (
        Decimal(proof["first_readback_seconds"]),
        Decimal(proof["second_readback_seconds"]),
    )
    require(0 <= first < second < 300 and second - first >= 10, "evidence=readback_interval")
    retained = value["retained_before"]
    unit = value["processor_task_definition_arns"]["unit"]
    require(
        retained == sorted(set(retained))
        and value["retained_after"] == sorted(set(retained) | {unit})
        and (value["active_before"] is None or value["active_before"] in retained),
        "evidence=retained",
    )
    require(len({item["id"] for item in value["r1_artifacts"]}) == 6, "evidence=r1_artifacts")
    require(value["ecr_image"].endswith("@" + value["image_digest"]), "evidence=ecr_digest")
    cost = value["cost"]
    require(
        Decimal(cost["product_monthly_usd"]) <= 8
        and Decimal(cost["aggregate_monthly_usd"]) < 15
        and timestamp(value["expires_at"]) <= timestamp(cost["expires_at"])
        and timestamp(value["expires_at"]) <= timestamp(cost["cloudfront"]["expires_at"]),
        "evidence=active_cost",
    )
    canonical_bytes(value)


def build_evidence(promotion: Node, result: Node, references: Node, governed: Node) -> Node:
    """Returns: evidência do schema único depois de todas as verificações."""
    candidate, source = promotion["candidate"], promotion["provenance"]
    r2 = source["reference"] | {
        "workflow_path": source["run"]["path"],
        "workflow_sha": source["run"]["head_sha"],
        "source_sha": candidate["source_sha"],
    }
    scan = promotion["scan"]
    proof = result["iam_proof"]
    typed_proof = proof | {
        key: f"{proof[key]:.6f}" for key in ("first_readback_seconds", "second_readback_seconds")
    }
    return {
        "schema_version": 1,
        **promotion["binding"],
        **result["outputs"],
        "ecr_image": promotion["ecr_image"],
        "plan_binding_sha256": result["plan_binding_sha256"],
        "state_sha256": result["apply"]["state_after"]["state_sha256"],
        "outputs_sha256": result["apply"]["state_after"]["outputs_sha256"],
        "source_provenance_sha256": object_hash(source),
        "references": references | {"candidate": r2},
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
        "iam": typed_proof,
        "network_sha256": object_hash(governed["network"]),
        "created_at": governed["clock"].utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "expires_at": result["plan"]["expires_at"],
        "cost": result["plan"]["cost"],
    }


BASE_CHECKS = {
    "source-develop-checks",
    "artifact-origin-attempt-expiry",
    "dependency-pins",
    "toolchain-lock-selections",
    "backend-identity-workspace",
    "policy-evaluation",
    "cost-ledger-purpose-scope",
    "drift-baseline",
    "promotion469-origin",
    "r2-candidate-a2-binding",
    "oci-promotion-byte-integrity",
}


def _admission_artifacts(items: list[Node], expires: datetime, expected: list[str]) -> None:
    require(type(items) is list and len(items) <= 32, "admission=artifacts")
    names = [item["purpose"] for item in items]
    require(names == sorted(set(names)), "admission=artifact_order")
    require(
        names == expected
        and {"backend", "policy", "cost", "drift", "shared-outputs", "toolchain"} <= set(names),
        "admission=required_artifacts",
    )
    for item in items:
        closed(item, "purpose reference content_sha256 expires_at")
        reference = closed(
            item["reference"],
            "repository run_id run_attempt artifact_id "
            "artifact_digest workflow_path workflow_sha source_sha",
        )
        require(
            re.fullmatch(r"sha256:[0-9a-f]{64}", reference["artifact_digest"])
            and re.fullmatch(r"[0-9a-f]{40}", reference["source_sha"])
            and re.fullmatch(r"[0-9a-f]{40}", reference["workflow_sha"])
            and re.fullmatch(r"[0-9a-f]{64}", item["content_sha256"])
            and timestamp(item["expires_at"]) >= expires,
            "admission=artifact_binding",
        )


def _admission_reports(proof: Node, members: Node, expected: set[str]) -> None:
    reports, checks = proof["reports"], proof["checks"]
    require(
        [item["check_id"] for item in reports] == sorted(expected)
        and [item["check_id"] for item in checks] == sorted(expected),
        "admission=checks",
    )
    require(len({item["name"] for item in reports}) == len(reports), "admission=report_names")
    require(
        set(members) == {"index.json", "redacted-plan.json"} | {item["name"] for item in reports},
        "admission=members",
    )
    for report, check in zip(reports, checks, strict=True):
        closed(report, "check_id name size sha256")
        closed(
            check,
            "check_id subject_sha256 expected_sha256 observed_sha256 result observed_at expires_at",
        )
        data = members[report["name"]]
        require(
            type(report["size"]) is int
            and 0 < len(data) == report["size"] <= 4 * 1024**2
            and sha256(data) == report["sha256"],
            "admission=report_digest",
        )
        value = closed(parse_document(data), "check_id source_sha measurement")
        require(
            value["check_id"] == report["check_id"] == check["check_id"]
            and value["source_sha"] == proof["source"]["source_sha"]
            and type(value["measurement"]) is dict
            and value["measurement"],
            "admission=facts",
        )
        require(
            check["result"] == "pass"
            and check["expected_sha256"]
            == check["observed_sha256"]
            == object_hash(value["measurement"])
            and re.fullmatch(r"[0-9a-f]{64}", check["subject_sha256"]),
            "admission=measurement",
        )
        require(
            timestamp(check["observed_at"]) <= timestamp(proof["observed_at"])
            and timestamp(check["expires_at"]) >= timestamp(proof["expires_at"]),
            "admission=time",
        )


def _admission(index: Node, members: Node, governed: Node, latest: datetime) -> None:
    proof = closed(
        index["admission_proof"],
        "proof_version source producer artifacts reports checks "
        "candidate_sha256 promotion_evidence_sha256 plan_binding_sha256 observed_at expires_at",
    )
    require(
        type(proof["proof_version"]) is int and proof["proof_version"] == 1, "admission=version"
    )
    require(
        proof["source"]
        == {
            "repository": index["repository"],
            "ref": "refs/heads/develop",
            "source_sha": index["source_sha"],
        },
        "admission=source",
    )
    require(
        proof["producer"] == {"repository": index["repository"]} | index["producer"],
        "admission=producer",
    )
    require(
        proof["candidate_sha256"] == governed["release"]["candidate_sha256"]
        and proof["promotion_evidence_sha256"] == governed["promotion_evidence_sha256"]
        and proof["plan_binding_sha256"] == index["plan_binding_sha256"],
        "admission=release",
    )
    expires = timestamp(proof["expires_at"])
    require(
        timestamp(proof["observed_at"]) <= latest and expires >= timestamp(index["expires_at"]),
        "admission=validity",
    )
    _admission_artifacts(
        proof["artifacts"], expires, governed["artifact_purposes"][index["status"]]
    )
    require(
        proof["artifacts"] == governed["prior_artifacts"][index["status"]],
        "admission=governed_refs",
    )
    required = BASE_CHECKS | (
        {"exact-saved-plan-package", "planned-source-input-binding"}
        if index["status"] == "applied"
        else set()
    )
    _admission_reports(proof, members, required)


def _index(artifact: Node, result: Node, status: str, governed: Node) -> None:
    members = artifact["members"]
    require(len(members["index.json"]) <= 32768, "index=size")
    index = closed(
        parse_document(members["index.json"]),
        "index_version kind status repository source_sha "
        "producer purpose created_at expires_at plan_binding_sha256 envelope_sha256 "
        "package_aggregate_sha256 private_locator policy cost drift admission_proof",
    )
    require(
        type(index["index_version"]) is int
        and index["index_version"] == 1
        and index["kind"] == "cnesdata-private-plan-transport"
        and index["status"] == status,
        "index=kind_status",
    )
    _index_projection(index, result, status, members)
    plan, apply = result["plan"], result["apply"]
    require(
        index["policy"] == {key: plan["policy"][key] for key in ("verdict", "evidence_sha256")}
        and index["cost"] == plan["cost"],
        "index=policy_cost",
    )
    drift = plan["drift"] if status == "planned" else apply["drift"]
    require(
        index["drift"] == {key: drift[key] for key in ("verdict", "evidence_sha256")}, "index=drift"
    )
    latest = timestamp(plan["created_at"] if status == "planned" else apply["started_at"])
    _admission(index, members, governed, latest)


def _index_projection(index: Node, result: Node, status: str, members: Node) -> None:
    plan, apply = result["plan"], result["apply"]
    require(
        all(
            index[key] == plan[key]
            for key in ("repository", "source_sha", "purpose", "created_at", "expires_at")
        ),
        "index=projection",
    )
    producer = plan["producer"] if status == "planned" else apply["producer"]
    require(
        index["producer"] == producer
        and index["plan_binding_sha256"] == result["plan_binding_sha256"]
        and index["package_aggregate_sha256"] == plan["package"]["aggregate_sha256"],
        "index=plan_binding",
    )
    require(index["envelope_sha256"] == result["envelope_hashes"][status], "index=envelope_hash")
    require(members["redacted-plan.json"] == result["redacted_plan"], "index=redacted_view")
    require(
        index["private_locator"]
        == {
            "scope": "cnesdata-prod",
            "source_sha": plan["source_sha"],
            "run_id": producer["run_id"],
            "run_attempt": producer["run_attempt"],
            "status": status,
        },
        "index=locator",
    )


def _policy_cost(plan: Node, governed: Node) -> None:
    policy = closed(
        plan["policy"], "verdict evidence_sha256 policy_sha256 evaluator_sha256 plan_json_sha256"
    )
    _hashes(policy, "evidence_sha256 policy_sha256 evaluator_sha256 plan_json_sha256")
    require(
        policy["verdict"] == "pass"
        and policy["policy_sha256"] == governed["policy_sha256"]
        and policy["evaluator_sha256"] == governed["evaluator_sha256"]
        and policy["plan_json_sha256"] == plan["json_sha256"],
        "policy=binding",
    )
    cost = closed(
        plan["cost"],
        "verdict evidence_sha256 manifest_sha256 product_monthly_usd "
        "aggregate_monthly_usd paid_actions_limit_usd calculated_at expires_at scope cloudfront",
    )
    _hashes(cost, "evidence_sha256 manifest_sha256")
    for key in ("product_monthly_usd", "aggregate_monthly_usd"):
        require(
            type(cost[key]) is str and re.fullmatch(r"(0|[1-9][0-9]*)\.[0-9]{2}", cost[key]),
            "cost=decimal",
        )
    require(
        cost["verdict"] == "pass"
        and cost["manifest_sha256"] == governed["manifest_sha256"]
        and Decimal(cost["product_monthly_usd"]) <= 8
        and Decimal(cost["aggregate_monthly_usd"]) < 15
        and cost["paid_actions_limit_usd"] == "0.00"
        and cost["scope"] == "active",
        "cost=gate",
    )
    require(
        timestamp(cost["calculated_at"]) <= timestamp(plan["created_at"])
        and timestamp(cost["expires_at"]) >= timestamp(plan["expires_at"]),
        "cost=validity",
    )
    _cloudfront_cost(cost, plan, governed)


def _cloudfront_cost(cost: Node, plan: Node, governed: Node) -> None:
    cloudfront = closed(
        cost["cloudfront"],
        "plan status subscription_sha256 distribution_sha256 "
        "waf_binding_sha256 observed_at expires_at",
    )
    _hashes(cloudfront, "subscription_sha256 distribution_sha256 waf_binding_sha256")
    require(
        cloudfront["plan"] == "FREE"
        and cloudfront["status"] == "ACTIVE"
        and all(cloudfront[key] == value for key, value in governed["cloudfront"].items()),
        "cost=cloudfront_identity",
    )
    require(
        timestamp(cloudfront["observed_at"]) <= timestamp(cost["calculated_at"])
        and timestamp(cloudfront["expires_at"]) >= timestamp(plan["expires_at"]),
        "cost=cloudfront_validity",
    )


def _cost_report(plan: Node, reports: Node) -> None:
    cost = plan["cost"]
    report = closed(
        decode_json(reports[cost["evidence_sha256"]], limit=STATE_LIMIT),
        "check_id source_sha measurement",
    )
    require(
        report["check_id"] == "cost-ledger-purpose-scope"
        and report["source_sha"] == plan["source_sha"]
        and type(report["measurement"]) is dict
        and report["measurement"]["cost"]
        == {key: value for key, value in cost.items() if key != "evidence_sha256"},
        "cost=report_facts_without_selfhash",
    )


def _drift(plan: Node, apply: Node, publication: Node) -> None:
    before = closed(plan["drift"], "verdict evidence_sha256 state_sha256 observed_at expires_at")
    after = closed(apply["drift"], "verdict evidence_sha256 readback_sha256 observed_at")
    _hashes(before, "evidence_sha256")
    _hashes(after, "evidence_sha256 readback_sha256")
    require(
        before["verdict"] == "pass"
        and after["verdict"] == "clean"
        and before["state_sha256"] == plan["state_before"]["state_sha256"],
        "drift=verdict_state",
    )
    require(
        timestamp(before["observed_at"]) <= timestamp(plan["created_at"])
        and timestamp(before["expires_at"]) >= timestamp(plan["expires_at"])
        and timestamp(apply["completed_at"])
        <= timestamp(after["observed_at"])
        <= timestamp(publication["verified_at"]),
        "drift=validity",
    )


def _verify_publication_artifacts(result: Node, artifacts: Node) -> None:
    planned = timestamp(artifacts["planned"]["artifact"]["created_at"])
    applied = timestamp(artifacts["applied"]["artifact"]["created_at"])
    published = timestamp(artifacts["publication"]["artifact"]["created_at"])
    started = timestamp(result["apply"]["started_at"])
    completed = timestamp(result["apply"]["completed_at"])
    verified = timestamp(result["publication"]["verified_at"])
    require(
        timestamp(result["plan"]["created_at"]) <= planned <= started
        and completed <= applied <= verified <= published <= completed + timedelta(seconds=45),
        "publication=artifact_time",
    )
