"""Payloads A3 e publicações sintéticas na fronteira de aplicação."""

import json

from tests.production.delivery._phase_a_fixtures import ACCOUNT, REGION, SHA, _bytes, _digest


def _application():
    context, files, release = _application_context()
    report = b'{"synthetic":"contract-fixture-only"}\n'
    proof = context["report_sha256"]
    plan = _synthetic_plan(context, files)
    outputs = _synthetic_outputs(release)
    context["fingerprint_addresses"] = {
        key: [] for key in ("resource_fingerprints", "grant_fingerprints", "route_fingerprints")
    }
    state = _synthetic_state(outputs)
    applied = _synthetic_apply(context, plan, outputs, state)
    binding_hash = _digest(_bytes(plan) + b"\n")[7:]
    planned = {
        "schema_version": 1,
        "status": "planned",
        "plan_binding_sha256": binding_hash,
        "plan": plan,
        "apply": None,
    }
    complete = planned | {"status": "applied", "apply": applied}
    publication, references = _application_publication(plan, complete)
    return {
        "planned": _bytes(planned) + b"\n",
        "applied": _bytes(complete) + b"\n",
        "publication": _bytes(publication) + b"\n",
        "files": files,
        "state_before": None,
        "state_after": state,
        "reports": {proof: report, plan["cost"]["evidence_sha256"]: _cost_report(plan["cost"])},
        "references": references,
    }, context


def _application_identity():
    source = {
        "repository": "VINIClUS/CnesData",
        "source_sha": SHA,
        "release_id": f"candidate-{SHA}-42-2",
        "candidate_sha256": "a" * 64,
    }
    binding = _digest(b"cnesdata:prod:canary:v1\0" + _bytes(source))[7:]
    release = {key: source[key] for key in ("release_id", "source_sha", "candidate_sha256")}
    release |= {
        "binding_sha256": binding,
        "canary_id": binding[:32],
        "image_digest": "sha256:" + "b" * 64,
    }
    inputs = {
        "image_digest": release["image_digest"],
        "processor_active_unit_task_definition_arn": None,
        "processor_retained_unit_task_definition_arns": [],
        "canary_storage_enabled": True,
        "canary_release_id": release["canary_id"],
        "canary_trusted_principal_arn": f"arn:aws:iam::{ACCOUNT}:role/promotion-workflow",
    }
    return release, inputs


def _application_context():
    release, inputs = _application_identity()
    report = b'{"synthetic":"contract-fixture-only"}\n'
    proof = _digest(report)[7:]
    backend = {
        "type": "s3",
        "bucket": "synthetic-state",
        "key": "cnesdata/prod/opentofu.tfstate",
        "workspace": "default",
        "region": REGION,
        "encrypt": True,
        "use_lockfile": True,
        "kms_key_id": f"arn:aws:kms:{REGION}:{ACCOUNT}:key/synthetic",
    }
    toolchain = _application_toolchain(proof)
    files = {
        "plan.bin": b"synthetic-binary",
        "plan.json": b'{"resource_changes":[]}\n',
        "inputs.tfvars.json": _bytes(inputs) + b"\n",
    }
    context = {
        "repository": "VINIClUS/CnesData",
        "source_sha": SHA,
        "release": release,
        "phase_a_inputs": inputs,
        "generated_inputs": inputs,
        "backend": backend,
        "toolchain": toolchain,
        "policy_sha256": proof,
        "evaluator_sha256": proof,
        "expires_at": "2026-12-01T21:00:00Z",
        "manifest_sha256": proof,
        "dependencies": [],
        "account_id": ACCOUNT,
        "deploy_role_arn": f"arn:aws:iam::{ACCOUNT}:role/deploy",
        "report_sha256": proof,
        "files": {"binary": "plan.bin", "json": "plan.json", "inputs": "inputs.tfvars.json"},
    }
    context["cloudfront"] = {
        "subscription_sha256": "c" * 64,
        "distribution_sha256": "d" * 64,
        "waf_binding_sha256": "e" * 64,
    }
    return context, files, release


def _application_toolchain(proof):
    return {
        "opentofu": {"version": "1.13.2", "binary_sha256": proof},
        "providers": [
            {
                "source": "registry.opentofu.org/hashicorp/aws",
                "version": "6.54.0",
                "package_sha256": proof,
            }
        ],
        "lock_sha256": proof,
    }


def _application_publication(plan, complete):
    proof = plan["policy"]["evidence_sha256"]
    publication = {
        "receipt_version": 1,
        "repository": "VINIClUS/CnesData",
        "run_id": 102,
        "run_attempt": 1,
        "envelope_sha256": _digest(_bytes(complete) + b"\n")[7:],
        "index_artifact_id": 22,
        "index_artifact_digest": "sha256:" + proof,
        "lease_evidence_sha256": proof,
        "checkpoint_sha256": proof,
        "verified_at": "2026-11-07T20:51:10Z",
        "result": "success",
    }
    references = {
        name: {
            "repository": "VINIClUS/CnesData",
            "run_id": run,
            "run_attempt": 1,
            "artifact_id": artifact,
            "artifact_digest": "sha256:" + proof,
            "workflow_path": "synthetic-plan" if name == "planned" else "synthetic-apply",
            "workflow_sha": SHA,
            "source_sha": SHA,
        }
        for name, run, artifact in (
            ("planned", 101, 21),
            ("applied", 102, 22),
            ("publication", 102, 23),
        )
    }
    return publication, references


def _synthetic_plan(context, files):
    proof = context["report_sha256"]
    package = [
        {"name": name, "size": len(data), "sha256": _digest(data)[7:]}
        for name, data in sorted(files.items())
    ]
    return {
        "repository": context["repository"],
        "source_sha": SHA,
        "producer": {
            "workflow_path": "synthetic-plan",
            "workflow_sha": SHA,
            "run_id": 101,
            "run_attempt": 1,
        },
        "package": {"files": package, "aggregate_sha256": _digest(_bytes(package) + b"\n")[7:]},
        "purpose": "phase-a",
        "created_at": "2026-11-07T20:30:00Z",
        "expires_at": "2026-11-07T21:30:00Z",
        "binary_sha256": _digest(files["plan.bin"])[7:],
        "json_sha256": _digest(files["plan.json"])[7:],
        "inputs_sha256": _digest(files["inputs.tfvars.json"])[7:],
        "dependencies": [],
        "toolchain": context["toolchain"],
        "backend": {
            "configuration": context["backend"],
            "identity_sha256": _digest(_bytes(context["backend"]) + b"\n")[7:],
        },
        "state_before": dict.fromkeys(("lineage", "serial", "state_sha256")),
        "policy": {
            "verdict": "pass",
            "evidence_sha256": proof,
            "policy_sha256": proof,
            "evaluator_sha256": proof,
            "plan_json_sha256": _digest(files["plan.json"])[7:],
        },
        "cost": _active_cost(context),
        "drift": {
            "verdict": "pass",
            "evidence_sha256": proof,
            "state_sha256": None,
            "observed_at": "2026-11-07T20:30:00Z",
            "expires_at": "2026-11-07T21:30:00Z",
        },
        "release": context["release"],
        "phase_a_inputs": context["phase_a_inputs"],
    }


def _cost_report(cost):
    facts = {key: value for key, value in cost.items() if key != "evidence_sha256"}
    return (
        _bytes(
            {
                "check_id": "cost-ledger-purpose-scope",
                "source_sha": SHA,
                "measurement": {"cost": facts, "ledger": {"synthetic": "8.00"}},
            }
        )
        + b"\n"
    )


def _active_cost(context):
    cost = {
        "verdict": "pass",
        "manifest_sha256": context["manifest_sha256"],
        "product_monthly_usd": "8.00",
        "aggregate_monthly_usd": "14.99",
        "paid_actions_limit_usd": "0.00",
        "calculated_at": "2026-11-07T20:30:00Z",
        "expires_at": "2026-11-07T21:30:00Z",
        "scope": "active",
        "cloudfront": {
            "plan": "FREE",
            "status": "ACTIVE",
            **context["cloudfront"],
            "observed_at": "2026-11-07T20:30:00Z",
            "expires_at": "2026-11-07T21:30:00Z",
        },
    }
    return cost | {"evidence_sha256": _digest(_cost_report(cost))[7:]}


def _rebind_application(bundle, applied):
    cost = applied["plan"]["cost"]
    report = _cost_report(cost)
    cost["evidence_sha256"] = _digest(report)[7:]
    bundle["reports"][cost["evidence_sha256"]] = report
    planned = json.loads(bundle["planned"])
    planned["plan"] = applied["plan"]
    binding = _digest(_bytes(applied["plan"]) + b"\n")[7:]
    planned["plan_binding_sha256"] = applied["plan_binding_sha256"] = binding
    applied["apply"]["plan_binding_sha256"] = binding
    bundle["planned"] = _bytes(planned) + b"\n"
    bundle["applied"] = _bytes(applied) + b"\n"
    publication = json.loads(bundle["publication"])
    publication["envelope_sha256"] = _digest(bundle["applied"])[7:]
    bundle["publication"] = _bytes(publication) + b"\n"


def _synthetic_outputs(release):
    prefix = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/cnesdata-prod-processor-"
    revisions = {mode: prefix + mode + ":3" for mode in ("unit", "recovery", "audit")}
    return {
        "processor_task_definition_arns": revisions,
        "image_digest": release["image_digest"],
        "active_before": None,
        "retained_before": [],
        "retained_after": [revisions["unit"]],
        "canary_id": release["canary_id"],
        "binding_sha256": release["binding_sha256"],
        "resource_fingerprints": [],
        "grant_fingerprints": [],
        "route_fingerprints": [],
    }


def _synthetic_apply(context, plan, outputs, state):
    proof = context["report_sha256"]
    state_metadata = {
        key: value for key, value in json.loads(state).items() if key in ("lineage", "serial")
    }
    state_metadata.update(
        state_sha256=_digest(state)[7:], outputs_sha256=_digest(_bytes(outputs) + b"\n")[7:]
    )
    return {
        "producer": {
            "workflow_path": "synthetic-apply",
            "workflow_sha": SHA,
            "run_id": 102,
            "run_attempt": 1,
        },
        "consumed_plan_artifact": {
            "artifact_id": 21,
            "artifact_sha256": proof,
            "run_id": 101,
            "run_attempt": 1,
        },
        "plan_binding_sha256": _digest(_bytes(plan) + b"\n")[7:],
        "started_at": "2026-11-07T20:50:00Z",
        "completed_at": "2026-11-07T20:51:00Z",
        "result": "success",
        "lease": _apply_lease(context, proof),
        "principal": {
            "kind": "assumed-role",
            "account_id": ACCOUNT,
            "region": REGION,
            "gate": "deploy",
            "role_arn": context["deploy_role_arn"],
            "session_sha256": proof,
            "credential_expires_at": "2026-11-07T22:00:00Z",
            "evidence_sha256": proof,
        },
        "state_after": state_metadata,
        "drift": {
            "verdict": "clean",
            "evidence_sha256": proof,
            "readback_sha256": proof,
            "observed_at": "2026-11-07T20:51:01Z",
        },
        "outputs": outputs,
    }


def _public_index(envelope, governed, status, prior):
    plan = envelope["plan"]
    producer = plan["producer"] if status == "planned" else envelope["apply"]["producer"]
    reports, checks, members = _index_reports(plan, status)
    binding = _digest(_bytes(plan) + b"\n")[7:]
    proof = {
        "proof_version": 1,
        "source": {
            "repository": plan["repository"],
            "ref": "refs/heads/develop",
            "source_sha": SHA,
        },
        "producer": {"repository": plan["repository"], **producer},
        "artifacts": prior,
        "reports": reports,
        "checks": checks,
        "candidate_sha256": plan["release"]["candidate_sha256"],
        "promotion_evidence_sha256": governed["promotion_evidence_sha256"],
        "plan_binding_sha256": binding,
        "observed_at": plan["created_at"],
        "expires_at": plan["expires_at"],
    }
    drift = plan["drift"] if status == "planned" else envelope["apply"]["drift"]
    index = {
        "index_version": 1,
        "kind": "cnesdata-private-plan-transport",
        "status": status,
        **{
            key: plan[key]
            for key in ("repository", "source_sha", "purpose", "created_at", "expires_at")
        },
        "producer": producer,
        "plan_binding_sha256": binding,
        "envelope_sha256": _digest(_bytes(envelope) + b"\n")[7:],
        "package_aggregate_sha256": plan["package"]["aggregate_sha256"],
        "private_locator": {
            "scope": "cnesdata-prod",
            "source_sha": SHA,
            "run_id": producer["run_id"],
            "run_attempt": producer["run_attempt"],
            "status": status,
        },
        "policy": {key: plan["policy"][key] for key in ("verdict", "evidence_sha256")},
        "cost": plan["cost"],
        "drift": {key: drift[key] for key in ("verdict", "evidence_sha256")},
        "admission_proof": proof,
    }
    members["index.json"] = _bytes(index) + b"\n"
    members["redacted-plan.json"] = governed["redacted_plan"]
    return members


def _index_reports(plan, status):
    required = {
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
    required |= (
        {"exact-saved-plan-package", "planned-source-input-binding"}
        if status == "applied"
        else set()
    )
    reports, checks, members = [], [], {}
    for name in sorted(required):
        measurement = {"synthetic_check": name}
        data = _bytes({"check_id": name, "source_sha": SHA, "measurement": measurement}) + b"\n"
        member = name + ".json"
        members[member] = data
        reports.append(
            {"check_id": name, "name": member, "size": len(data), "sha256": _digest(data)[7:]}
        )
        checks.append(
            {
                "check_id": name,
                "subject_sha256": "d" * 64,
                "expected_sha256": _digest(_bytes(measurement) + b"\n")[7:],
                "observed_sha256": _digest(_bytes(measurement) + b"\n")[7:],
                "result": "pass",
                "observed_at": plan["created_at"],
                "expires_at": plan["expires_at"],
            }
        )
    return reports, checks, members


def _apply_lease(context, proof):
    return {
        "protocol": "s3-cas-v1",
        "owner_sha256": proof,
        "repository": context["repository"],
        "run_id": 102,
        "run_attempt": 1,
        "session_sha256": proof,
        "acquired_at": "2026-11-07T20:49:50Z",
        "released_at": "2026-11-07T20:51:05Z",
        "ttl_seconds": 300,
        "renew_interval_seconds": 60,
        "safety_margin_seconds": 120,
        "acquire_etag_sha256": proof,
        "release_etag_sha256": proof,
        "renewal_count": 1,
        "checkpoint_sha256": proof,
        "evidence_sha256": proof,
        "terminal_status": "released",
    }


def _synthetic_state(outputs):
    state = {"outputs": outputs}
    state.update(lineage="synthetic-lineage", serial=1)
    return _bytes(state) + b"\n"
