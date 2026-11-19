"""Validação consumidora offline do contrato A3, sem schema ou envelope alternativo."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta
from typing import Any

from release.verify_candidate import require, timestamp
from scripts.promotion._canonical import (
    STATE_LIMIT,
    canonical_bytes,
    canonical_state,
    decode_json,
    object_hash,
    parse_document,
)
from scripts.promotion.verify_task_revisions import retained_inputs

type Node = dict[str, Any]


def closed(value: Any, fields: str) -> Node:
    """Returns: objeto de campos exatos; chaves ausentes ou adicionais falham fechadas."""
    require(type(value) is dict and set(value) == set(fields.split()), "contract=fields")
    return value


def _hashes(value: Node, fields: str) -> None:
    require(
        all(
            type(value[key]) is str and re.fullmatch(r"[0-9a-f]{64}", value[key])
            for key in fields.split()
        ),
        "contract=sha256",
    )


def _id(value: Any, zero: bool = False) -> None:
    require(type(value) is int and (0 if zero else 1) <= value <= 2**53 - 1, "contract=id")


def _producer(value: Node) -> None:
    closed(value, "workflow_path workflow_sha run_id run_attempt")
    require(
        type(value["workflow_path"]) is str
        and value["workflow_path"]
        and re.fullmatch(r"[0-9a-f]{40}", value["workflow_sha"]),
        "contract=producer",
    )
    _id(value["run_id"])
    _id(value["run_attempt"])


def validate_reference(value: Node, repository: str = "VINIClUS/CnesData") -> None:
    """Raises: ValueError para referência externa ambígua ou mutável."""
    closed(
        value,
        "repository run_id run_attempt artifact_id artifact_digest workflow_path "
        "workflow_sha source_sha",
    )
    require(
        value["repository"] == repository
        and re.fullmatch(r"sha256:[0-9a-f]{64}", value["artifact_digest"])
        and re.fullmatch(r"[0-9a-f]{40}", value["source_sha"]),
        "contract=reference",
    )
    _producer(
        {key: value[key] for key in ("workflow_path", "workflow_sha", "run_id", "run_attempt")}
    )
    _id(value["artifact_id"])


def _package(plan: Node, files: Node) -> Node:
    package = closed(plan["package"], "files aggregate_sha256")
    entries: list[Node] = package["files"]
    require(type(entries) is list and 3 <= len(entries) <= 32, "package=count")
    names = [item["name"] for item in entries]
    require(names == sorted(set(names)) and set(names) == set(files), "package=names")
    require({"plan.bin", "plan.json", "inputs.tfvars.json"} <= set(names), "package=required")
    require(package["aggregate_sha256"] == object_hash(entries), "package=aggregate")
    for item in entries:
        closed(item, "name size sha256")
        require(
            bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", item["name"]))
            and ".." not in item["name"],
            "package=path",
        )
        _id(item["size"])
        _hashes(item, "sha256")
        data = files[item["name"]]
        require(
            type(data) is bytes
            and len(data) == item["size"]
            and hashlib.sha256(data).hexdigest() == item["sha256"],
            "package=content",
        )
    require(sum(item["size"] for item in entries) <= 192 * 1024**2, "package=size")
    require(
        len(files["plan.bin"]) <= 32 * 1024**2 and len(files["plan.json"]) <= STATE_LIMIT,
        "package=plan_size",
    )
    for field, name in (
        ("binary_sha256", "plan.bin"),
        ("json_sha256", "plan.json"),
        ("inputs_sha256", "inputs.tfvars.json"),
    ):
        require(plan[field] == hashlib.sha256(files[name]).hexdigest(), "package=plan_hash")
    return decode_json(files["plan.json"], state=True, limit=STATE_LIMIT)


def _toolchain(value: Node, governed: Node) -> None:
    closed(value, "opentofu providers lock_sha256")
    closed(value["opentofu"], "version binary_sha256")
    require(value["opentofu"]["version"] == "1.13.2", "toolchain=opentofu")
    _hashes(value["opentofu"], "binary_sha256")
    _hashes(value, "lock_sha256")
    providers: list[Node] = value["providers"]
    require(type(providers) is list and 0 < len(providers) <= 32, "toolchain=providers")
    names = [item["source"] for item in providers]
    require(names == sorted(set(names)), "toolchain=provider_order")
    for item in providers:
        closed(item, "source version package_sha256")
        _hashes(item, "package_sha256")
    require(
        any(
            item["source"] == "registry.opentofu.org/hashicorp/aws" and item["version"] == "6.54.0"
            for item in providers
        ),
        "toolchain=aws",
    )
    require(value == governed["toolchain"], "toolchain=governed_lock")


def _backend(value: Node, governed: Node) -> None:
    closed(value, "identity_sha256 configuration")
    configuration = closed(
        value["configuration"], "type bucket key workspace region encrypt use_lockfile kms_key_id"
    )
    require(
        configuration == governed["backend"]
        and configuration["type"] == "s3"
        and configuration["key"] == "cnesdata/prod/opentofu.tfstate"
        and configuration["workspace"] == "default"
        and configuration["region"] == "us-east-2"
        and configuration["encrypt"] is True
        and configuration["use_lockfile"] is True,
        "backend=identity",
    )
    require(value["identity_sha256"] == object_hash(configuration), "backend=hash")


def _state(value: Node, raw: bytes | None, after: bool = False) -> Node | None:
    closed(
        value,
        "lineage serial state_sha256 outputs_sha256" if after else "lineage serial state_sha256",
    )
    if raw is None:
        require(not after and all(item is None for item in value.values()), "state=null_bootstrap")
        return None
    logical = decode_json(raw, state=True, limit=STATE_LIMIT)
    require(
        type(logical) is dict
        and logical["lineage"] == value["lineage"]
        and logical["serial"] == value["serial"]
        and type(value["lineage"]) is str
        and value["lineage"],
        "state=identity",
    )
    _id(value["serial"], zero=True)
    require(hashlib.sha256(canonical_state(raw)).hexdigest() == value["state_sha256"], "state=hash")
    return logical


def _release(plan: Node, governed: Node) -> None:
    release = closed(
        plan["release"],
        "release_id source_sha candidate_sha256 canary_id binding_sha256 image_digest",
    )
    inputs = closed(
        plan["phase_a_inputs"],
        "image_digest processor_active_unit_task_definition_arn "
        "processor_retained_unit_task_definition_arns canary_storage_enabled "
        "canary_release_id canary_trusted_principal_arn",
    )
    require(
        release == governed["release"] and inputs == governed["phase_a_inputs"], "release=binding"
    )
    _hashes(release, "candidate_sha256 binding_sha256")
    binding = {key: release[key] for key in ("release_id", "source_sha", "candidate_sha256")}
    binding["repository"] = plan["repository"]
    digest = hashlib.sha256(
        b"cnesdata:prod:canary:v1\0" + canonical_bytes(binding)[:-1]
    ).hexdigest()
    require(
        release["binding_sha256"] == digest
        and release["canary_id"] == digest[:32]
        and release["source_sha"] == plan["source_sha"],
        "release=canary",
    )
    require(
        inputs["image_digest"] == release["image_digest"]
        and re.fullmatch(r"sha256:[0-9a-f]{64}", release["image_digest"])
        and inputs["canary_release_id"] == release["canary_id"]
        and inputs["canary_storage_enabled"] is True,
        "release=phase_inputs",
    )


def _outputs(value: Node, plan: Node, governed: Node) -> None:
    closed(
        value,
        "processor_task_definition_arns image_digest active_before retained_before "
        "retained_after canary_id binding_sha256 resource_fingerprints grant_fingerprints "
        "route_fingerprints",
    )
    context = {"region": "us-east-2", "account_id": governed["account_id"]} | plan["phase_a_inputs"]
    retained = retained_inputs(context, value["processor_task_definition_arns"])
    require(all(value[key] == item for key, item in retained.items()), "outputs=retained")
    require(
        all(
            value[key] == plan["release"][key]
            for key in ("image_digest", "canary_id", "binding_sha256")
        ),
        "outputs=release",
    )
    for key in ("resource_fingerprints", "grant_fingerprints", "route_fingerprints"):
        records: list[Node] = value[key]
        require(type(records) is list and len(records) <= 64, "outputs=fingerprints")
        names = [item["address"] for item in records]
        require(names == sorted(set(names)), "outputs=fingerprint_order")
        require(names == governed["fingerprint_addresses"][key], "outputs=fingerprint_allowlist")
        for item in records:
            closed(item, "address fingerprint_sha256")
            require(type(item["address"]) is str and item["address"], "outputs=address")
            _hashes(item, "fingerprint_sha256")


def _lease_principal(apply: Node, governed: Node) -> None:
    principal = closed(
        apply["principal"],
        "kind account_id region gate role_arn session_sha256 credential_expires_at evidence_sha256",
    )
    _hashes(principal, "session_sha256 evidence_sha256")
    require(
        principal["kind"] == "assumed-role"
        and principal["account_id"] == governed["account_id"]
        and principal["region"] == "us-east-2"
        and principal["gate"] == "deploy"
        and principal["role_arn"] == governed["deploy_role_arn"],
        "principal=identity",
    )
    lease = closed(
        apply["lease"],
        "protocol owner_sha256 repository run_id run_attempt session_sha256 "
        "acquired_at released_at ttl_seconds renew_interval_seconds safety_margin_seconds "
        "acquire_etag_sha256 release_etag_sha256 renewal_count checkpoint_sha256 "
        "evidence_sha256 terminal_status",
    )
    _hashes(
        lease,
        "owner_sha256 session_sha256 acquire_etag_sha256 release_etag_sha256 "
        "checkpoint_sha256 evidence_sha256",
    )
    require(
        lease["protocol"] == "s3-cas-v1"
        and lease["repository"] == governed["repository"]
        and lease["ttl_seconds"] == 300
        and lease["renew_interval_seconds"] == 60
        and lease["safety_margin_seconds"] == 120
        and lease["terminal_status"] == "released"
        and lease["session_sha256"] == principal["session_sha256"],
        "lease=contract",
    )
    require(
        all(lease[key] == apply["producer"][key] for key in ("run_id", "run_attempt")),
        "lease=owner_run",
    )
    _id(lease["renewal_count"], zero=True)
    require(
        timestamp(lease["acquired_at"])
        <= timestamp(apply["started_at"])
        < timestamp(apply["completed_at"])
        <= timestamp(lease["released_at"]),
        "lease=time",
    )


def _proof_reports(value: Any, reports: Node) -> None:
    if type(value) is list:
        for item in value:
            _proof_reports(item, reports)
    elif type(value) is dict:
        for key, item in value.items():
            if key in {"evidence_sha256", "readback_sha256", "checkpoint_sha256"}:
                require(
                    item in reports and hashlib.sha256(reports[item]).hexdigest() == item,
                    "proof=missing_or_hash_mismatch",
                )
                logical = decode_json(reports[item], limit=STATE_LIMIT)
                require(
                    canonical_bytes(logical, STATE_LIMIT) == reports[item], "proof=noncanonical"
                )
            _proof_reports(item, reports)


def _publication(bundle: Node, plan: Node, apply: Node, now: datetime) -> Node:
    value = closed(
        parse_document(bundle["publication"]),
        "receipt_version repository run_id "
        "run_attempt envelope_sha256 index_artifact_id index_artifact_digest "
        "lease_evidence_sha256 checkpoint_sha256 verified_at result",
    )
    require(
        value["receipt_version"] == 1
        and value["result"] == "success"
        and value["repository"] == plan["repository"],
        "publication=result",
    )
    require(
        all(value[key] == apply["producer"][key] for key in ("run_id", "run_attempt")),
        "publication=producer",
    )
    require(
        value["envelope_sha256"] == hashlib.sha256(bundle["applied"]).hexdigest()
        and value["lease_evidence_sha256"] == apply["lease"]["evidence_sha256"]
        and value["checkpoint_sha256"] == apply["lease"]["checkpoint_sha256"],
        "publication=binding",
    )
    reference = bundle["references"]["applied"]
    require(
        value["index_artifact_id"] == reference["artifact_id"]
        and value["index_artifact_digest"] == reference["artifact_digest"],
        "publication=index",
    )
    started, completed = timestamp(apply["started_at"]), timestamp(apply["completed_at"])
    verified, expires = timestamp(value["verified_at"]), timestamp(plan["expires_at"])
    require(
        timestamp(plan["created_at"]) <= started < completed <= verified <= now < expires,
        "publication=time",
    )
    require(
        expires - started >= timedelta(seconds=1980)
        and completed - started <= timedelta(seconds=1800)
        and verified - completed <= timedelta(seconds=45)
        and verified < expires - timedelta(seconds=120)
        and timestamp(apply["lease"]["released_at"]) <= verified,
        "publication=deadline",
    )
    require(
        timestamp(apply["principal"]["credential_expires_at"]) - started >= timedelta(seconds=1980),
        "principal=expiry",
    )
    return value


def _envelope(data: bytes, status: str, governed: Node) -> Node:
    envelope = closed(parse_document(data), "schema_version status plan_binding_sha256 plan apply")
    require(
        type(envelope["schema_version"]) is int
        and envelope["schema_version"] == 1
        and envelope["status"] == status,
        "envelope=version_status",
    )
    plan = closed(
        envelope["plan"],
        "repository source_sha producer package purpose created_at "
        "expires_at binary_sha256 json_sha256 inputs_sha256 dependencies toolchain backend "
        "state_before policy cost drift release phase_a_inputs",
    )
    require(
        plan["repository"] == governed["repository"] == "VINIClUS/CnesData"
        and plan["source_sha"] == governed["source_sha"]
        and plan["purpose"] == "phase-a",
        "plan=source_purpose",
    )
    require(envelope["plan_binding_sha256"] == object_hash(plan), "plan=binding_hash")
    _producer(plan["producer"])
    require(
        timedelta(0)
        < timestamp(plan["expires_at"]) - timestamp(plan["created_at"])
        <= timedelta(seconds=3600)
        and timestamp(plan["expires_at"]) <= timestamp(governed["expires_at"]),
        "plan=validity",
    )
    if status == "planned":
        require(envelope["apply"] is None, "envelope=planned_apply")
    else:
        apply = closed(
            envelope["apply"],
            "producer consumed_plan_artifact plan_binding_sha256 "
            "started_at completed_at result lease principal state_after drift outputs",
        )
        _producer(apply["producer"])
        require(
            apply["result"] == "success"
            and apply["plan_binding_sha256"] == envelope["plan_binding_sha256"],
            "apply=result_binding",
        )
    return envelope


def _references(bundle: Node, plan: Node, apply: Node) -> None:
    references = closed(bundle["references"], "planned applied publication")
    for name, reference in references.items():
        validate_reference(reference)
        producer = plan["producer"] if name == "planned" else apply["producer"]
        require(
            all(reference[key] == producer[key] for key in producer)
            and reference["source_sha"] == plan["source_sha"],
            "reference=producer_binding",
        )
    consumed = closed(
        apply["consumed_plan_artifact"], "artifact_id artifact_sha256 run_id run_attempt"
    )
    planned = references["planned"]
    require(
        all(consumed[key] == planned[key] for key in ("artifact_id", "run_id", "run_attempt"))
        and consumed["artifact_sha256"] == planned["artifact_digest"][7:],
        "apply=consumed_plan",
    )


def verify_application(bundle: Node, governed: Node, now: datetime) -> Node:
    """Returns: campos A3 conferidos; autenticação dos artifacts ocorre na fronteira externa."""
    from scripts.promotion._phase_a_evidence import _cost_report, _drift, _policy_cost

    planned = _envelope(bundle["planned"], "planned", governed)
    applied = _envelope(bundle["applied"], "applied", governed)
    require(planned["plan"] == applied["plan"], "apply=changed_plan")
    plan, apply = planned["plan"], applied["apply"]
    _references(bundle, plan, apply)
    _toolchain(plan["toolchain"], governed)
    _backend(plan["backend"], governed)
    _policy_cost(plan, governed)
    _release(plan, governed)
    _dependencies(plan, governed)
    plan_json = _package(plan, bundle["files"])
    require(set(bundle["files"]) == set(governed["files"].values()), "package=governed_members")
    require(
        parse_document(bundle["files"]["inputs.tfvars.json"]) == governed["generated_inputs"],
        "plan=generated_inputs",
    )
    _state(plan["state_before"], bundle["state_before"])
    _state(apply["state_after"], bundle["state_after"], after=True)
    if bundle["state_before"] is not None:
        require(
            plan["state_before"]["lineage"] == apply["state_after"]["lineage"]
            and plan["state_before"]["serial"] <= apply["state_after"]["serial"],
            "state=transition",
        )
    _outputs(apply["outputs"], plan, governed)
    require(apply["state_after"]["outputs_sha256"] == object_hash(apply["outputs"]), "outputs=hash")
    _lease_principal(apply, governed)
    publication = _publication(bundle, plan, apply, now)
    _drift(plan, apply, publication)
    _proof_reports({"plan": plan, "apply": apply}, bundle["reports"])
    _cost_report(plan, bundle["reports"])
    return {
        "plan": plan,
        "apply": apply,
        "outputs": apply["outputs"],
        "plan_json": plan_json,
        "plan_binding_sha256": applied["plan_binding_sha256"],
        "publication": publication,
    }


def _dependencies(plan: Node, governed: Node) -> None:
    dependencies: list[Node] = plan["dependencies"]
    require(
        dependencies == governed["dependencies"]
        and type(dependencies) is list
        and len(dependencies) <= 32,
        "plan=dependencies",
    )
    dependency_keys = [(item["repository"], item["source_sha"]) for item in dependencies]
    require(dependency_keys == sorted(set(dependency_keys)), "plan=dependency_order")
    for item in dependencies:
        closed(item, "repository source_sha content_sha256")
        _hashes(item, "content_sha256")
        require(re.fullmatch(r"[0-9a-f]{40}", item["source_sha"]), "plan=dependency_source")
