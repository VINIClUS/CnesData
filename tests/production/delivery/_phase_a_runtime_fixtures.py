"""Fronteira sintética de finalização, sem operações de produção."""

import base64
import copy
import json
from typing import Any

from tests.production.delivery._phase_a_application_fixtures import (
    _application,
    _public_index,
    _synthetic_apply,
    _synthetic_plan,
    _synthetic_state,
)
from tests.production.delivery._phase_a_fixtures import (
    ACCOUNT,
    REGION,
    SHA,
    _bytes,
    _digest,
    _iam_context,
    _public_artifact,
)


def _runtime_resources(promotion, outputs):
    image = promotion["ecr_image"]
    task = {"skip_destroy": True, "track_latest": False, "cpu": "256", "memory": "512"}
    resources = {
        f'module.processing.aws_ecs_task_definition.processor["{mode}"]': task
        | {
            "family": "cnesdata-prod-processor-" + mode,
            "container_definitions": json.dumps(
                [{"name": "processor", "image": image, "command": []}]
            ),
        }
        for mode in ("unit", "recovery", "audit")
    }
    roles = [
        f'module.runtime_iam.aws_iam_role.task["{mode}"]' for mode in ("unit", "recovery", "audit")
    ]
    roles += [
        "module.runtime_iam.aws_iam_role.execution",
        "module.runtime_iam.aws_iam_role.step_functions[0]",
    ]
    roles += [
        f"module.runtime_iam.aws_iam_role.{name}[0]"
        for name in (
            "canary_task",
            "canary_execution",
            "canary_seeder",
            "canary_verifier",
            "promotion_fence_operator",
        )
    ]
    roles += ["external.roles_anywhere.api_role"]
    resources |= {
        address: {
            "name": "cnesdata-prod-" + address.rsplit(".", 1)[1].replace("_", "-"),
            "path": "/",
            "max_session_duration": 3600,
            "assume_role_policy": '{"Statement":[]}',
            "tags": {"ReleaseId": promotion["binding"]["canary_id"]},
        }
        for address in roles
    }
    resources.update(_runtime_grants(outputs))
    return resources, roles


def _runtime_grants(outputs):
    resources = {}
    resources["module.processing.aws_iam_role_policy.step_functions_ecs[0]"] = {
        "name": "cnesdata-prod-step-functions-ecs",
        "role": "sfn",
        "policy": json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["ecs:DescribeTasks", "ecs:StopTask"],
                        "Resource": "*",
                    },
                    {
                        "Effect": "Allow",
                        "Action": "ecs:RunTask",
                        "Resource": [outputs["processor_task_definition_arns"]["unit"]],
                    },
                ],
            }
        ),
    }
    resources["module.processing.aws_iam_role_policy.canary_verifier_run_task[0]"] = {
        "name": "cnesdata-prod-canary-verifier-run-task",
        "role": "cnesdata-prod-canary-verifier",
        "policy": json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "ecs:RunTask",
                        "Resource": outputs["processor_task_definition_arns"]["audit"],
                        "Condition": {"ArnEquals": {"ecs:cluster": "cluster"}},
                    }
                ],
            }
        ),
    }
    return resources


def _runtime_bundle(promotion):

    bundle, governed = _application()
    governed["release"] = {
        key: promotion["binding"][key] for key in governed["release"] if key != "image_digest"
    }
    governed["release"]["image_digest"] = promotion["image_digest"]
    governed["phase_a_inputs"].update(
        image_digest=promotion["image_digest"], canary_release_id=promotion["binding"]["canary_id"]
    )
    governed["toolchain"]["lock_sha256"] = promotion["candidate"]["locks"]["opentofu"]
    applied = json.loads(bundle["applied"])
    outputs = applied["apply"]["outputs"]
    outputs.update(
        {key: governed["release"][key] for key in ("image_digest", "canary_id", "binding_sha256")}
    )
    resources, roles = _runtime_resources(promotion, outputs)
    changes, plan_json, source, approved = _runtime_graph(resources, governed, outputs)
    plan = _runtime_saved_plan(bundle, governed, plan_json, approved)
    bundle["state_after"] = _synthetic_state(outputs)
    applied = {
        "schema_version": 1,
        "status": "applied",
        "plan": plan,
        "plan_binding_sha256": _digest(_bytes(plan) + b"\n")[7:],
        "apply": _synthetic_apply(governed, plan, outputs, bundle["state_after"]),
    }
    governed["delta"] = {
        "resources": {
            name: value
            for name, value in resources.items()
            if name in {item["address"] for item in changes}
        },
        "ecr_image": promotion["ecr_image"],
        "approved_graph": approved,
        "candidate_unit_arn": outputs["processor_task_definition_arns"]["unit"],
        "candidate_audit_arn": outputs["processor_task_definition_arns"]["audit"],
        "canary_verifier_role_id": "cnesdata-prod-canary-verifier",
        "cluster_arn": "cluster",
        "retained_before": [],
    }
    return bundle, governed, applied, resources, roles, source


def _runtime_graph(resources, governed, outputs):
    from scripts.promotion.phase_a import resource_fingerprint

    for key in ("resource_fingerprints", "grant_fingerprints"):
        addresses = sorted(
            address
            for address in resources
            if (".aws_iam_role_policy." in address) == (key == "grant_fingerprints")
        )
        outputs[key] = [
            {"address": name, "fingerprint_sha256": resource_fingerprint(name, resources[name])}
            for name in addresses
        ]
        governed["fingerprint_addresses"][key] = addresses
    changes = [
        {
            "address": name,
            "type": name.split(".")[2],
            "mode": "managed",
            "change": {"actions": ["create"], "before": None, "after": fields, "after_unknown": {}},
        }
        for name, fields in resources.items()
        if not name.startswith("external.")
    ]
    for item in changes:
        if (
            ".aws_iam_role." in item["address"]
            and "canary" not in item["address"]
            and "promotion_fence" not in item["address"]
        ):
            item["change"].update(actions=["no-op"], before=item["change"]["after"])
    configuration = {"root_module": {"resources": []}}
    plan_json = {"configuration": configuration, "resource_changes": changes}
    source = {"infra/opentofu/modules/processing/step_functions.tf": b"synthetic source only"}
    approved = {
        "source_files": [
            {"path": name, "sha256": _digest(data)[7:]} for name, data in source.items()
        ],
        "configuration_sha256": _digest(_bytes(configuration) + b"\n")[7:],
        "unknown_policies": [],
        "known_policies": {
            name: _digest(_bytes(json.loads(value["policy"])) + b"\n")[7:]
            for name, value in resources.items()
            if ".aws_iam_role_policy." in name
        },
    }
    return changes, plan_json, source, approved


def _runtime_saved_plan(bundle, governed, plan_json, approved):
    from scripts.promotion._phase_a_graph import redacted_plan

    bundle["files"].update(
        {
            "plan.json": _bytes(plan_json) + b"\n",
            "inputs.tfvars.json": _bytes(governed["generated_inputs"]) + b"\n",
        }
    )
    governed["redacted_plan"] = (
        _bytes(redacted_plan(plan_json, SHA, _digest(bundle["files"]["plan.json"])[7:])) + b"\n"
    )
    bundle["files"]["redacted-plan.json"] = governed["redacted_plan"]
    governed["files"]["redacted"] = "redacted-plan.json"
    plan = _synthetic_plan(governed, bundle["files"])
    report = (
        _bytes(
            {
                "evaluation_sha256": governed["report_sha256"],
                "graph_proof": approved
                | {"governance_sha256": _digest(_bytes(approved) + b"\n")[7:]},
            }
        )
        + b"\n"
    )
    plan["policy"]["evidence_sha256"] = _digest(report)[7:]
    bundle["reports"][_digest(report)[7:]] = report
    return plan


def _finalization_fixture(api, promotion, clock) -> dict[str, Any]:
    bundle, governed, applied, resources, roles, source = _runtime_bundle(promotion)
    governed.update(
        clock=clock,
        region=REGION,
        branch="develop",
        event="workflow_dispatch",
        candidate_creator={
            "workflow_path": ".github/workflows/release-candidate.yml",
            "workflow_sha": "b" * 40,
        },
        promotion_evidence_sha256=_digest(_bytes(promotion) + b"\n")[7:],
    )
    common, prior_apply, profiles = _runtime_publication(api, bundle, governed, applied)
    _runtime_governance(governed, {"planned": common, "applied": prior_apply}, profiles, roles)
    for name, data in source.items():
        api.routes["contents/" + name] = {
            "type": "file",
            "encoding": "base64",
            "content": base64.b64encode(data).decode(),
        }
    _runtime_context(governed, promotion)
    return {"bundle": bundle, "governed": governed, "resources": resources, "promotion": promotion}


def _runtime_common(api, governed):
    from scripts.promotion._artifact_origin import artifact_content_hash

    profile = {key: governed[key] for key in ("repository", "source_sha", "branch", "event")}
    profile |= {
        "callers": [{"workflow_path": "synthetic-source", "workflow_sha": SHA}],
        "reusables": {"synthetic-source": []},
    }
    common = []
    profiles = {}
    for index, purpose in enumerate(
        ("backend", "cost", "drift", "policy", "shared-outputs", "toolchain", "phase-inputs")
    ):
        members = {"proof.json": _bytes({"synthetic": purpose}) + b"\n"}
        reference = _public_artifact(
            api, "synthetic-" + purpose, members, (201 + index, "synthetic-source")
        )
        common.append(
            {
                "purpose": purpose,
                "reference": reference,
                "content_sha256": artifact_content_hash(members),
                "expires_at": "2026-12-01T21:00:00Z",
            }
        )
        profiles[purpose] = profile
    common.sort(key=lambda item: item["purpose"])
    return common, profiles


def _runtime_publication(api, bundle, governed, applied):
    from scripts.promotion._artifact_origin import artifact_content_hash

    common, profiles = _runtime_common(api, governed)
    planned = {**applied, "status": "planned", "apply": None}
    bundle["planned"] = _bytes(planned) + b"\n"
    planned_members = _public_index(planned, governed, "planned", common)
    planned_ref = _public_artifact(
        api, "synthetic-planned", planned_members, (101, "synthetic-plan")
    )
    applied["apply"]["consumed_plan_artifact"] = {
        key: planned_ref[key] for key in ("artifact_id", "run_id", "run_attempt")
    }
    applied["apply"]["consumed_plan_artifact"]["artifact_sha256"] = planned_ref["artifact_digest"][
        7:
    ]
    prior_apply = [item for item in common if item["purpose"] != "phase-inputs"] + [
        {
            "purpose": "planned",
            "reference": planned_ref,
            "content_sha256": artifact_content_hash(planned_members),
            "expires_at": "2026-12-01T21:00:00Z",
        }
    ]
    prior_apply.sort(key=lambda item: item["purpose"])
    bundle["applied"] = _bytes(applied) + b"\n"
    applied_ref = _public_artifact(
        api,
        "synthetic-applied",
        _public_index(applied, governed, "applied", prior_apply),
        (102, "synthetic-apply"),
    )
    publication = json.loads(bundle["publication"])
    publication.update(
        envelope_sha256=_digest(bundle["applied"])[7:],
        index_artifact_id=applied_ref["artifact_id"],
        index_artifact_digest=applied_ref["artifact_digest"],
    )
    bundle["publication"] = _bytes(publication) + b"\n"
    receipt_ref = _public_artifact(
        api, "synthetic-receipt", {"receipt.json": bundle["publication"]}, (102, "synthetic-apply")
    )
    bundle["references"] = {
        "planned": planned_ref,
        "applied": applied_ref,
        "publication": receipt_ref,
    }
    return common, prior_apply, profiles


def _runtime_governance(governed, prior, profiles, roles):
    governed.update(
        callers=[
            {"workflow_path": name, "workflow_sha": SHA}
            for name in ("synthetic-plan", "synthetic-apply")
        ],
        reusables={name: [] for name in ("synthetic-plan", "synthetic-apply")},
        artifact_purposes={
            "planned": [item["purpose"] for item in prior["planned"]],
            "applied": [item["purpose"] for item in prior["applied"]],
        },
        prior_artifacts=prior,
        artifact_profiles=profiles,
        iam_role_addresses=roles,
        network={
            "cluster": "cluster",
            "subnets": ["subnet"],
            "security_group": "sg",
            "public_ip": "ENABLED",
            "platform": "1.4.0",
        },
    )
    profiles["planned"] = {
        key: governed[key]
        for key in ("repository", "source_sha", "branch", "event", "callers", "reusables")
    }


def _runtime_context(governed, promotion):
    governed["iam_context"] = _iam_context()
    governed["iam_context"]["scheduler_previous"] = {"recovery": None, "audit": None}
    governed["iam_context"]["principals"].update(scheduler_recovery=None, scheduler_audit=None)
    governed["revisions_context"] = {
        "account_id": ACCOUNT,
        "region": REGION,
        "ecr_image": promotion["ecr_image"],
        **governed["phase_a_inputs"],
        "task_definitions": {
            mode: {
                "family": "cnesdata-prod-processor-" + mode,
                "networkMode": "awsvpc",
                "requiresCompatibilities": ["FARGATE"],
                "cpu": "256",
                "memory": "512",
                "executionRoleArn": "execution",
                "taskRoleArn": mode,
                "runtimePlatform": {},
                "volumes": [],
                "containerDefinitions": [{"name": "processor", "image": promotion["ecr_image"]}],
            }
            for mode in ("unit", "recovery", "audit")
        },
    }


class RuntimeBoundary:
    """Respostas sintéticas de serviços; nunca representa prova operacional."""

    def __init__(self, github, prepared):
        self.github, self.prepared = github, prepared
        self.registry = prepared["registry"]
        self.saved = None
        self.writes, self.registrations = 0, 0
        self.mutation = None
        self.deadlines = []

    def get(self, path):
        return self.github.get(path)

    def download(self, artifact, target):
        self.github.download(artifact, target)

    def for_repository(self, repository):
        return self.github

    def load_private_application(self, locators):
        return copy.deepcopy(self.prepared["bundle"])

    def read_backend_configuration(self):
        return self.prepared["governed"]["backend"]

    def read_backend_state(self, configuration):
        data = self.prepared["bundle"]["state_after"]
        return data.replace(b'"serial":1', b'"serial":2') if self.mutation == "state" else data

    def read_applied_resources(self):
        return self.prepared["resources"]

    def read_network(self):
        return self.prepared["governed"]["network"]

    def read_active_cloudfront_evidence(self):
        return json.loads(self.prepared["bundle"]["applied"])["plan"]["cost"]["cloudfront"]

    def describe_task_definition(self, arn):
        data = json.loads(self.prepared["bundle"]["applied"])["apply"]["outputs"]
        mode = next(
            mode for mode, value in data["processor_task_definition_arns"].items() if value == arn
        )
        return self.prepared["governed"]["revisions_context"]["task_definitions"][mode] | {
            "taskDefinitionArn": arn,
            "status": "ACTIVE",
        }

    def read_iam(self, cases, deadline):
        self.deadlines.append(deadline)
        outputs = json.loads(self.prepared["bundle"]["applied"])["apply"]["outputs"]
        roles = {
            item["address"]: item["fingerprint_sha256"]
            for item in outputs["resource_fingerprints"]
            if item["address"] in self.prepared["governed"]["iam_role_addresses"]
        }
        allowed = {
            "sfn-candidate-unit",
            "verifier-audit",
            "sfn-pass-role-0",
            "sfn-pass-role-1",
            "verifier-pass-role-0",
            "verifier-pass-role-1",
            "seeder-canary",
            "fence-read",
            "fence-semaphore-read",
            "fence-update",
        }
        evaluations = [case | {"allowed": case["case_id"] in allowed} for case in cases]
        if self.mutation == "iam":
            evaluations[0]["allowed"] = not evaluations[0]["allowed"]
        return {
            "policies": {
                item["address"]: item["fingerprint_sha256"]
                for item in outputs["grant_fingerprints"]
            },
            "trust": roles,
            "roles": roles,
            "network": self.read_network(),
            "evaluations": evaluations,
        }

    def read_phase_a_evidence(self, identity):
        return self.saved

    def store_phase_a_evidence_once(self, identity, data):
        self.writes += 1
        self.saved = data
        return data

    def register_task_definition(self, definition):
        self.registrations += 1
        raise AssertionError("registration=forbidden")
