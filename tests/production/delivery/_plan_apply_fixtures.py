"""Transporte97 sintético; bundle A3 integrado reutilizado sem alterar a origem."""

import copy
import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime

from tests.production.delivery._phase_a_application_fixtures import _application

NOW = datetime(2026, 11, 9, 0, 45, tzinfo=UTC)
APPLICATION_NOW = datetime(2026, 11, 7, 20, 52, tzinfo=UTC)
SOURCE = "a" * 40
PRODUCER = "97f8e7e086e3f107401b5eb35ff4017dde21ff34"
BASELINES = ["backend", "cost", "drift", "policy", "shared-outputs", "toolchain"]


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def reference(number, repository="VINIClUS/infra-ansible", path="baseline.yml"):
    sha = SOURCE if repository == "VINIClUS/CnesData" else PRODUCER
    return {
        "repository": repository,
        "run_id": number,
        "run_attempt": 1,
        "artifact_id": number + 1000,
        "artifact_digest": "sha256:" + "f" * 64,
        "workflow_path": ".github/workflows/" + path,
        "workflow_sha": sha,
        "source_sha": sha,
    }


class PublicGitHub:
    def __init__(self):
        self.responses = {}
        self.archives = {}
        self.calls = []

    def get(self, repository, path):
        self.calls.append((repository, path))
        return copy.deepcopy(self.responses[repository, path])

    def download(self, repository, metadata, target):
        self.calls.append((repository, "download"))
        target.write_bytes(self.archives[repository, metadata["id"]])

    def artifact(self, ref, members):
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w") as archive:
            for name, value in members.items():
                archive.writestr(name, value)
        raw = stream.getvalue()
        ref["artifact_digest"] = "sha256:" + hashlib.sha256(raw).hexdigest()
        self.archives[ref["repository"], ref["artifact_id"]] = raw
        self.responses[ref["repository"], f"actions/artifacts/{ref['artifact_id']}"] = {
            "id": ref["artifact_id"],
            "digest": ref["artifact_digest"],
            "expired": False,
            "expires_at": "2026-11-09T02:00:00Z",
            "created_at": "2026-11-09T00:31:00Z",
            "size_in_bytes": len(raw),
            "workflow_run": {"id": ref["run_id"], "head_sha": ref["source_sha"]},
        }
        self.responses[
            ref["repository"], f"actions/runs/{ref['run_id']}/attempts/{ref['run_attempt']}"
        ] = {
            "id": ref["run_id"],
            "run_attempt": ref["run_attempt"],
            "repository": {"full_name": ref["repository"]},
            "head_sha": ref["source_sha"],
            "path": ref["workflow_path"],
            "status": "completed",
            "conclusion": "success",
            "run_started_at": "2026-11-09T00:30:00Z",
            "updated_at": "2026-11-09T00:32:00Z",
        }


def request(mode="plan", purpose="acm-certificate"):
    api = PublicGitHub()
    baselines = []
    for number, name in enumerate(BASELINES, 1):
        ref = reference(number)
        api.artifact(ref, {"proof.json": canonical({"fixture_only": True})})
        baselines.append({"purpose": name, "reference": ref})
    payload = None
    if mode == "apply" or purpose == "phase-a":
        path = "infra-plan.yml" if mode == "apply" else "phase-inputs.yml"
        payload = reference(100, "VINIClUS/CnesData", path)
        members = {"proof.json": canonical({"fixture_only": True})}
        if mode == "apply":
            members = {"index.json": canonical(transport(payload, purpose))}
        api.artifact(payload, members)
    descriptor = {
        "schema_version": 1,
        "repository": "VINIClUS/CnesData",
        "source_sha": SOURCE,
        "producer_pins": [
            {
                "repository": "VINIClUS/infra-ansible",
                "source_sha": PRODUCER,
                "content_sha256": "c" * 64,
            }
        ],
        "baseline_artifacts": baselines,
        "payload_artifact": payload,
    }
    value = {
        "mode": mode,
        "purpose": purpose,
        "expected_source_sha": SOURCE,
        "request_descriptor": canonical(descriptor).decode(),
        "request_descriptor_sha256": hashlib.sha256(canonical(descriptor)).hexdigest(),
        "run_id": 900,
        "run_attempt": 2,
    }
    if payload:
        prefix = "plan_" if mode == "apply" else "phase_inputs_"
        value[prefix + "run_id"] = str(payload["run_id"])
        value[("artifact_id" if mode == "apply" else prefix + "artifact_id")] = str(
            payload["artifact_id"]
        )
        value[("artifact_digest" if mode == "apply" else prefix + "artifact_digest")] = payload[
            "artifact_digest"
        ]
    source_responses(api, mode)
    return value, api


def source_responses(api, mode):
    path = ".github/workflows/infra-" + mode + ".yml"
    api.responses["VINIClUS/CnesData", "actions/runs/900/attempts/2"] = {
        "id": 900,
        "run_attempt": 2,
        "repository": {"full_name": "VINIClUS/CnesData"},
        "head_sha": SOURCE,
        "head_branch": "develop",
        "event": "workflow_dispatch",
        "path": path,
        "status": "in_progress",
    }
    api.responses["VINIClUS/CnesData", f"compare/{SOURCE}...develop"] = {"status": "identical"}
    from release.build_candidate import REQUIRED_CHECKS

    api.responses["VINIClUS/CnesData", "rules/branches/develop"] = [
        {
            "type": "required_status_checks",
            "parameters": {
                "required_status_checks": [{"context": x} for x in sorted(REQUIRED_CHECKS)]
            },
        }
    ]
    api.responses["VINIClUS/CnesData", f"commits/{SOURCE}/check-runs?per_page=100"] = {
        "total_count": 10,
        "check_runs": [
            {
                "id": i,
                "name": name,
                "head_sha": SOURCE,
                "status": "completed",
                "conclusion": "success",
            }
            for i, name in enumerate(sorted(REQUIRED_CHECKS), 1)
        ],
    }


def transport(ref, purpose):
    return {
        "index_version": 1,
        "kind": "cnesdata-private-plan-transport",
        "status": "planned",
        "repository": "VINIClUS/CnesData",
        "source_sha": SOURCE,
        "producer": {
            key: ref[key] for key in ("workflow_path", "workflow_sha", "run_id", "run_attempt")
        },
        "purpose": purpose,
        "created_at": "2026-11-09T00:30:00Z",
        "expires_at": "2026-11-09T01:30:00Z",
        "plan_binding_sha256": "1" * 64,
        "envelope_sha256": "2" * 64,
        "package_aggregate_sha256": "3" * 64,
        "private_locator": {
            "scope": "cnesdata-prod",
            "source_sha": SOURCE,
            "run_id": ref["run_id"],
            "run_attempt": ref["run_attempt"],
            "status": "planned",
        },
        "policy": {"verdict": "pass", "evidence_sha256": "4" * 64},
        "cost": transport_cost(purpose),
        "drift": {"verdict": "pass", "evidence_sha256": "5" * 64},
        "admission_proof": {"fixture_only": True},
    }


def replace_payload(value, api, index):
    descriptor = json.loads(value["request_descriptor"])
    payload = descriptor["payload_artifact"]
    api.artifact(payload, {"index.json": canonical(index)})
    value["artifact_digest"] = payload["artifact_digest"]
    value["request_descriptor"] = canonical(descriptor).decode()
    value["request_descriptor_sha256"] = hashlib.sha256(canonical(descriptor)).hexdigest()


def phase_apply():
    value, api = request("apply", "phase-a")
    phase = reference(200, "VINIClUS/CnesData", "phase-inputs.yml")
    api.artifact(phase, {"proof.json": canonical({"fixture_only": True})})
    payload = json.loads(value["request_descriptor"])["payload_artifact"]
    index = transport(payload, "phase-a")
    index["admission_proof"] = {"artifacts": [{"purpose": "phase-inputs", "reference": phase}]}
    replace_payload(value, api, index)
    value.update(
        phase_inputs_run_id="200",
        phase_inputs_artifact_id="1200",
        phase_inputs_artifact_digest=phase["artifact_digest"],
    )
    return value, api


def evidence(purpose="phase-a", status="applied"):
    bundle, _ = _application()
    value = json.loads(bundle[status])
    if purpose == "phase-a":
        return value
    plan = value["plan"]
    plan.update(purpose=purpose, release=None, phase_a_inputs=None)
    cost = plan["cost"]
    cost.update(scope="pre-activation", cloudfront={"plan": None, "status": None})
    cost.update(
        activation="blocked", dns="blocked", workloads="blocked", unbound_monthly_usd="7.00"
    )
    if status == "applied":
        value["apply"]["outputs"] = outputs(purpose)
    return copy.deepcopy(value)


def outputs(purpose):
    if purpose == "acm-certificate":
        return {
            "acm_certificate_arn": "arn:aws:acm:us-east-1:123456789012:certificate/synthetic",
            "acm_dns_validation_records": [
                {
                    "domain": "cnesdata.com.br",
                    "name": "_proof.cnesdata.com.br.",
                    "type": "CNAME",
                    "value": "_proof.acm-validations.aws.",
                }
            ],
        }
    return {
        "cloudfront_distribution_id": "SYNTHETIC",
        "cloudfront_domain_name": "synthetic.cloudfront.net",
        "free_subscription_binding_input": {
            "distribution_arn": "arn:aws:cloudfront::123456789012:distribution/SYNTHETIC",
            "web_acl_arn": (
                "arn:aws:wafv2:us-east-1:123456789012:global/webacl/cnesdata-prod-static-web/id"
            ),
        },
        "bootstrap_invariants": {
            "active_unit": None,
            "retained_units": [],
            "state_machine": None,
            "schedulers": {"recovery": None, "audit": None},
            "canary_release": None,
            "canary_storage": False,
        },
    }


def application():
    return _application()


def transport_cost(purpose):
    cost = copy.deepcopy(evidence(purpose, "planned")["plan"]["cost"])
    cost.update(calculated_at="2026-11-09T00:30:00Z", expires_at="2026-11-09T01:30:00Z")
    if cost["scope"] == "active":
        cost["cloudfront"].update(
            observed_at="2026-11-09T00:30:00Z", expires_at="2026-11-09T01:30:00Z"
        )
    return cost
