"""Valida o delta Phase A e reconcilia retries antes de emitir evidência."""

from __future__ import annotations

import json
import re
from typing import Any

from release.verify_candidate import canonical, require
from scripts.promotion._artifact_origin import (
    artifact_content_hash,
    authenticate_artifacts,
    authenticate_r2_artifact,
    authenticate_sources,
)
from scripts.promotion._phase_a_evidence import (
    _index,
    _verify_publication_artifacts,
    build_evidence,
    validate_evidence,
)
from scripts.promotion._phase_a_graph import redacted_plan, resolved_change, review_graph
from scripts.promotion._plan_contract import (
    STATE_LIMIT,
    canonical_bytes,
    canonical_state,
    closed,
    decode_json,
    object_hash,
    parse_document,
    verify_application,
)
from scripts.promotion.promote_ecr import (
    authenticate_candidate,
    authenticated_scan,
    canary_binding,
    promote_candidate,
    sha256,
    verify_promoted_image,
    verify_promotion_receipt,
)
from scripts.promotion.verify_task_revisions import (
    _prove_iam,
    verify_revisions,
)

type Node = dict[str, Any]
TASK_PREFIX = "module.processing.aws_ecs_task_definition.processor"
SFN_POLICY = "module.processing.aws_iam_role_policy.step_functions_ecs[0]"
VERIFIER_POLICY = "module.processing.aws_iam_role_policy.canary_verifier_run_task[0]"
CANARY_ROLES = {
    "canary_task",
    "canary_execution",
    "canary_seeder",
    "canary_verifier",
    "promotion_fence_operator",
}
STORAGE_TYPES = {
    "aws_dynamodb_table",
    "aws_s3_bucket",
    "aws_s3_bucket_public_access_block",
    "aws_s3_bucket_ownership_controls",
    "aws_s3_bucket_versioning",
    "aws_s3_bucket_server_side_encryption_configuration",
    "aws_s3_bucket_object_lock_configuration",
    "aws_s3_bucket_lifecycle_configuration",
    "aws_s3_bucket_policy",
}
COMPUTED = {
    "arn",
    "arn_without_revision",
    "id",
    "revision",
    "unique_id",
    "create_date",
    "tags_all",
    "latest_stream_arn",
    "latest_stream_label",
    "stream_arn",
    "stream_label",
    "bucket_domain_name",
    "bucket_regional_domain_name",
    "hosted_zone_id",
    "region",
    "name_prefix",
}


def retry_action(identity: Node, observation: Node) -> str:
    """Returns: decisão sem mutação; efeitos ambíguos exigem recuperação auditada."""
    require(
        set(identity) == {"release_id", "candidate_sha256", "image_digest", "plan_binding_sha256"},
        "retry=identity_fields",
    )
    require(observation.get("identity") == identity, "recovery_required=identity_mismatch")
    status = observation.get("status")
    if status == "applied":
        return "read-only"
    require(
        status == "planned"
        and observation.get("effects_confirmed") is False
        and observation.get("state_matches") is True
        and observation.get("plan_valid") is True,
        "recovery_required=partial_or_unknown",
    )
    return "apply-exact-plan"


def _kind(address: str) -> str:
    task = re.fullmatch(re.escape(TASK_PREFIX) + r'\["(unit|recovery|audit)"\]', address)
    if task:
        return "task"
    if address == SFN_POLICY:
        return "sfn"
    if address == VERIFIER_POLICY:
        return "verifier"
    role = re.fullmatch(
        r"module\.runtime_iam\.(aws_iam_role|aws_iam_role_policy)\.(\w+)\[0\]", address
    )
    if role and role[2] in CANARY_ROLES:
        return "role" if role[1] == "aws_iam_role" else "policy"
    storage = re.fullmatch(r"module\.processing\.(\w+)\.canary\[0\]", address)
    if storage and storage[1] in STORAGE_TYPES:
        return "storage"
    raise ValueError(f"delta=address_forbidden address={address}")


def _functional(value: Node) -> Node:
    return {key: item for key, item in value.items() if key not in COMPUTED}


def _task_delta(before: Node | None, after: Node, context: Node) -> None:
    require(
        after.get("skip_destroy") is True and after.get("track_latest") is False,
        "delta=revision_retention",
    )
    containers = json.loads(after["container_definitions"])
    require(
        len(containers) == 1 and containers[0]["image"] == context["ecr_image"], "delta=task_image"
    )
    if before is None:
        return
    old = json.loads(before["container_definitions"])
    require(len(old) == 1, "delta=old_containers")
    old[0]["image"] = containers[0]["image"]
    require(old == containers, "delta=container_functional_change")
    first, second = _functional(before), _functional(after)
    first.pop("container_definitions", None)
    second.pop("container_definitions", None)
    require(first == second, "delta=task_functional_change")


def _sfn_delta(before: Node | None, after: Node, context: Node) -> None:
    policy = json.loads(after["policy"])
    statements = policy["Statement"]
    runs = [item for item in statements if item.get("Action") == "ecs:RunTask"]
    require(
        len(runs) == 1
        and set(runs[0]) == {"Effect", "Action", "Resource"}
        and runs[0]["Effect"] == "Allow",
        "delta=sfn_run_task",
    )
    resources = sorted(set(context["retained_before"]) | {context["candidate_unit_arn"]})
    require(runs[0]["Resource"] == resources, "delta=sfn_resources")
    if before is not None:
        previous = json.loads(before["policy"])
        old_other = [item for item in previous["Statement"] if item.get("Action") != "ecs:RunTask"]
        new_other = [item for item in statements if item.get("Action") != "ecs:RunTask"]
        require(
            old_other == new_other and previous.get("Version") == policy.get("Version"),
            "delta=sfn_other_grants",
        )
        require(
            {key: value for key, value in before.items() if key not in COMPUTED | {"policy"}}
            == {key: value for key, value in after.items() if key not in COMPUTED | {"policy"}},
            "delta=sfn_role",
        )


def _verifier_delta(after: Node, context: Node) -> None:
    require(after["role"] == context["canary_verifier_role_id"], "delta=verifier_role")
    policy = json.loads(after["policy"])
    require(
        policy
        == {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "ecs:RunTask",
                    "Resource": context["candidate_audit_arn"],
                    "Condition": {"ArnEquals": {"ecs:cluster": context["cluster_arn"]}},
                }
            ],
        },
        "delta=verifier_grants",
    )


def _approved(change: Node, address: str, context: Node) -> None:
    require(address in context.get("resources", {}), "delta=missing_source_contract")
    require(
        _functional(change["after"]) == context["resources"][address],
        "delta=source_contract_mismatch",
    )
    actions = change["actions"]
    require(
        actions in (["create"], ["update"], ["create", "delete"], ["delete", "create"]),
        "delta=actions",
    )
    if "delete" in actions:
        require(
            address.startswith(TASK_PREFIX) and change["before"].get("skip_destroy") is True,
            "delta=destructive_change",
        )


def _canary_delta(before: Node | None, after: Node, kind: str) -> None:
    if before is None:
        return
    allowed = {"tags", "assume_role_policy"} if kind == "role" else {"policy"}
    previous = {key: value for key, value in _functional(before).items() if key not in allowed}
    current = {key: value for key, value in _functional(after).items() if key not in allowed}
    require(previous == current, "delta=canary_functional_change")


def _changed_resource(resource: Node, context: Node) -> None:
    change, address = resource["change"], resource["address"]
    kind = _kind(address)
    _approved(change, address, context)
    before, after = change["before"], change["after"]
    unknown = {
        key
        for key, value in change.get("after_unknown", {}).items()
        if value and key not in COMPUTED
    }
    if unknown:
        resolved = resolved_change(resource, context, unknown)
        if resolved is None:
            return
        after = resolved
    if kind == "task":
        _task_delta(before, after, context)
    elif kind == "sfn":
        _sfn_delta(before, after, context)
    elif kind == "verifier":
        _verifier_delta(after, context)
    elif kind == "storage":
        require(change["actions"] == ["create"], "delta=storage_reconfiguration")
    else:
        _canary_delta(before, after, kind)


def verify_delta(plan: Node, context: Node) -> None:
    """Raises: ValueError para endereço/campo fora do contrato imutável de origem."""
    resources = plan["resource_changes"]
    require(len({item["address"] for item in resources}) == len(resources), "delta=duplicates")
    for resource in resources:
        if resource["mode"] != "managed":
            continue
        change = resource["change"]
        if change["actions"] == ["no-op"]:
            require(change["before"] == change["after"], "delta=false_noop")
            continue
        _changed_resource(resource, context)


def review_phase_a_plan(plan: Node, context: Node) -> Node:
    """Returns: prova pré-apply; unknowns só passam sob fonte e produtores já aprovados."""
    proof = review_graph(plan, context)
    verify_delta(plan, context | {"graph_proof": proof})
    return proof


def _prior_artifacts(items: list[Node], api: Any, governed: Node) -> None:
    for item in items:
        profile = governed["artifact_profiles"][item["purpose"]]
        client = api.for_repository(profile["repository"])
        if item["purpose"] == "r2-candidate":
            require(
                profile["repository"] == item["reference"]["repository"]
                and profile["source_sha"]
                == item["reference"]["source_sha"]
                == governed["source_sha"],
                "candidate=prior_source",
            )
            artifact = authenticate_r2_artifact(
                item["reference"], client, governed["candidate_creator"], governed["clock"].utcnow()
            )
        else:
            artifact = authenticate_artifacts(
                {"input": item["reference"]}, client, profile, governed["clock"].utcnow()
            )["input"]
        require(
            artifact_content_hash(artifact["members"]) == item["content_sha256"],
            "admission=prior_content",
        )


def prepare_inputs(promotion: Node, baseline: Node, governed: Node) -> Node:
    """Returns: somente os seis inputs Phase A; active e retained anteriores são preservados."""
    verify_promotion_receipt(promotion, governed)
    candidate = promotion["candidate"]
    binding = canary_binding(candidate, sha256(canonical(candidate)))
    require(
        binding == promotion["binding"] and candidate["source_sha"] == governed["source_sha"],
        "prepare=candidate_binding",
    )
    keys = "processor_active_unit_task_definition_arn processor_retained_unit_task_definition_arns"
    closed(baseline, keys)
    return {
        **baseline,
        "image_digest": promotion["image_digest"],
        "canary_storage_enabled": True,
        "canary_release_id": binding["canary_id"],
        "canary_trusted_principal_arn": governed["canary_trusted_principal_arn"],
    }


def prepare_phase_a(reference: Node, api: Any, registry: Any, governed: Node) -> Node:
    """Returns: promoção e inputs após reserva durável, sem trocar binding em retries."""
    verified = authenticate_candidate(
        reference, api, governed["candidate_creator"], governed["now"]
    )
    binding = verified["binding"]
    require(api.reserve_canary_binding(binding) == binding, "recovery_required=canary_collision")
    promotion = promote_candidate(reference, api, registry, governed)
    require(promotion["binding"] == binding, "recovery_required=candidate_changed")
    return {
        "promotion": promotion,
        "inputs": prepare_inputs(promotion, governed["previous_inputs"], governed),
    }


def resource_fingerprint(address: str, value: Node) -> str:
    """Returns: fingerprint dos campos funcionais observados, normalizando documentos JSON."""
    fields = _functional(value)
    for key in ("policy", "assume_role_policy", "container_definitions"):
        if type(fields.get(key)) is str:
            fields[key] = decode_json(fields[key].encode())
    return object_hash({"address": address, "fields": fields})


def _load_application(references: Node, api: Any, governed: Node) -> Node:
    clock = governed["clock"]
    artifacts = authenticate_artifacts(references, api, governed, clock.utcnow())
    locators = {
        name: {
            "scope": "cnesdata-prod",
            "source_sha": reference["source_sha"],
            "run_id": reference["run_id"],
            "run_attempt": reference["run_attempt"],
            "status": name,
        }
        for name, reference in references.items()
        if name in {"planned", "applied"}
    }
    bundle = api.load_private_application(locators)
    require(
        artifacts["publication"]["members"] == {"receipt.json": bundle["publication"]},
        "publication=external_bytes",
    )
    require(api.read_backend_configuration() == governed["backend"], "backend=live_configuration")
    bundle["state_after"] = api.read_backend_state(governed["backend"])
    bundle["references"] = references
    result = verify_application(bundle, governed, clock.utcnow())
    _verify_publication_artifacts(result, artifacts)
    result["redacted_plan"] = bundle["files"]["redacted-plan.json"]
    require(
        parse_document(result["redacted_plan"])
        == redacted_plan(
            result["plan_json"], governed["source_sha"], result["plan"]["json_sha256"]
        ),
        "redacted=projection",
    )
    result["envelope_hashes"] = {name: sha256(bundle[name]) for name in ("planned", "applied")}
    for name in ("planned", "applied"):
        _index(artifacts[name], result, name, governed)
        index = parse_document(artifacts[name]["members"]["index.json"])
        _prior_artifacts(index["admission_proof"]["artifacts"], api, governed)
    _verify_resource_readbacks(result, bundle, api, governed)
    return result


def _verify_resource_readbacks(result: Node, bundle: Node, api: Any, governed: Node) -> None:
    graph_context = governed["delta"] | {
        **{key: governed[key] for key in ("generated_inputs", "account_id", "region")},
        "source_files": authenticate_sources(
            governed["delta"]["approved_graph"]["source_files"], api, governed["source_sha"]
        ),
    }
    graph = review_phase_a_plan(result["plan_json"], graph_context)
    report = json.loads(bundle["reports"][result["plan"]["policy"]["evidence_sha256"]])
    require(report["graph_proof"] == graph, "policy=graph_report")
    resources = api.read_applied_resources()
    fingerprints = {
        item["address"]: item["fingerprint_sha256"]
        for key in ("resource_fingerprints", "grant_fingerprints")
        for item in result["outputs"][key]
    }
    require(
        all(
            resource_fingerprint(address, resources[address]) == digest
            for address, digest in fingerprints.items()
        ),
        "readback=resource_fingerprints",
    )
    verify_delta(
        result["plan_json"], graph_context | {"graph_proof": graph, "readback_resources": resources}
    )
    if "iam_resolver" in graph_context:
        graph_context["iam_resolver"].verify_state(_readback_state(bundle["state_after"]))


def finalize_phase_a(promotion: Node, references: Node, api: Any, governed: Node) -> Node:
    """Returns: evidência imutável; retry concluído só lê e confere, sem registrar/apply."""
    clock = governed["clock"]
    _verify_promotion(promotion, api, governed)
    governed = governed | {"promotion_evidence_sha256": object_hash(promotion)}
    result = _load_application(references, api, governed)
    revisions = result["outputs"]["processor_task_definition_arns"]
    verify_revisions(governed["revisions_context"], revisions, api)
    proof = _prove_iam(result, api, governed)
    result["iam_proof"] = proof
    require(
        sha256(canonical_state(api.read_backend_state(governed["backend"])))
        == result["apply"]["state_after"]["state_sha256"],
        "recovery_required=state_changed",
    )
    identity = {key: promotion["binding"][key] for key in ("release_id", "candidate_sha256")}
    identity |= {
        "image_digest": promotion["image_digest"],
        "plan_binding_sha256": result["plan_binding_sha256"],
    }
    existing = api.read_phase_a_evidence(identity)
    value = (
        parse_document(existing)
        if existing is not None
        else build_evidence(promotion, result, references, governed)
    )
    validate_evidence(value, clock.utcnow())
    require(
        all(value[key] == item for key, item in identity.items()),
        "recovery_required=evidence_binding",
    )
    require(
        all(value[key] == item for key, item in result["outputs"].items())
        and value["state_sha256"] == result["apply"]["state_after"]["state_sha256"]
        and value["outputs_sha256"] == result["apply"]["state_after"]["outputs_sha256"]
        and value["iam"]["readback_sha256"] == proof["readback_sha256"]
        and value["iam"]["matrix_sha256"] == proof["matrix_sha256"],
        "recovery_required=evidence_drift",
    )
    data = canonical_bytes(value)
    if existing is None:
        require(
            api.store_phase_a_evidence_once(identity, data) == data,
            "recovery_required=evidence_write",
        )
    validate_evidence(value, clock.utcnow())
    return value


def _verify_promotion(promotion: Node, api: Any, governed: Node) -> None:
    clock = governed["clock"]
    verified = authenticate_candidate(
        promotion["provenance"]["reference"], api, governed["candidate_creator"], clock.utcnow()
    )
    require(
        verified["candidate"] == promotion["candidate"]
        and verified["binding"] == promotion["binding"]
        and authenticated_scan(verified, api) == promotion["scan"],
        "recovery_required=provenance",
    )
    verify_promotion_receipt(promotion, governed)
    context = {
        key: promotion["candidate"][key]
        for key in ("repository", "source_sha", "release_id", "expected_ecr_repository")
    }
    context |= {key: governed[key] for key in ("account_id", "region")}
    context |= {**promotion["candidate"]["images"]["data_processor"], "scan": promotion["scan"]}
    verify_promoted_image(context, api.registry)


def _readback_state(data: bytes) -> Node:
    _state_readback_size(len(data))
    return decode_json(data, state=True, limit=STATE_LIMIT)


def _state_readback_size(size: int) -> None:
    require(type(size) is int and 0 < size <= STATE_LIMIT, "state=readback_size")
