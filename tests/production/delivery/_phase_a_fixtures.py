"""Fronteiras externas e payloads sintéticos usados no contrato Phase A."""

import copy
import hashlib
import json
from datetime import UTC, datetime, timedelta

NOW = datetime(2026, 11, 7, 21, tzinfo=UTC)

ACCOUNT = "123456789012"

REGION = "us-east-2"

REPOSITORY = "cnesdata-prod-processor"

ECR = f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/{REPOSITORY}"

SHA = "a" * 40


def _creator_pin():
    return {"workflow_path": ".github/workflows/release-candidate.yml", "workflow_sha": "b" * 40}


def _iam_context():
    return {
        "principals": {
            name: "principal-" + name
            for name in (
                "sfn",
                "verifier",
                "scheduler_recovery",
                "scheduler_audit",
                "seeder",
                "fence",
            )
        },
        "revisions": {name: "candidate-" + name for name in ("unit", "recovery", "audit")},
        "retained_before": ["old-unit"],
        "scheduler_previous": {"recovery": "old-recovery", "audit": "old-audit"},
        "cluster_arn": "cluster",
        "other_cluster_arn": "other-cluster",
        "runtime_roles": ["unit-role", "execution-role"],
        "canary_roles": ["canary-task", "canary-execution"],
        "canary_table_arn": "canary-table",
        "control_plane_table_arn": "control-plane",
    }


def _bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _digest(value):
    return "sha256:" + hashlib.sha256(value).hexdigest()


class RegistryBoundary:
    def __init__(self):
        self.config = _bytes({"architecture": "amd64", "os": "linux"})
        self.layer = b"synthetic-compressed-layer"
        self.manifest = _bytes(
            {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "digest": _digest(self.config),
                    "size": len(self.config),
                },
                "layers": [
                    {
                        "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                        "digest": _digest(self.layer),
                        "size": len(self.layer),
                    }
                ],
            }
        )
        self.repository = {
            "account_id": ACCOUNT,
            "region": REGION,
            "name": REPOSITORY,
            "uri": ECR,
            "image_tag_mutability": "IMMUTABLE",
        }
        self.copied = []

    def describe_repository(self):
        return copy.deepcopy(self.repository)

    def read_manifest(self, image):
        return self.manifest

    def download_blob(self, image, digest, target):
        blobs = {_digest(self.config): self.config, _digest(self.layer): self.layer}
        if digest not in blobs:
            raise ValueError("oci=blob_missing")
        target.write_bytes(blobs[digest])

    def copy_blob(self, digest, path):
        self.copied.append((digest, path.read_bytes()))

    def put_manifest(self, digest, data):
        self.copied.append((digest, data))

    def describe_image(self, digest):
        return {"digest": digest, "manifest": self.manifest}

    def download_destination_blob(self, digest, target):
        target.write_bytes(dict(self.copied)[digest])


def _promotion(registry):
    return {
        "repository": "VINIClUS/CnesData",
        "source_sha": SHA,
        "release_id": f"candidate-{SHA}-42-2",
        "candidate_sha256": "b" * 64,
        "image": "ghcr.io/viniclus/cnesdata/data_processor@" + _digest(registry.manifest),
        "config_digest": _digest(registry.config),
        "expected_ecr_repository": REPOSITORY,
        "account_id": ACCOUNT,
        "region": REGION,
        "scan": {
            "sha256": "c" * 64,
            "source_sha": SHA,
            "run_id": 42,
            "run_attempt": 2,
            "config_digest": _digest(registry.config),
            "sbom_sha256": "d" * 64,
            "job": "oci-build (data_processor)",
            "threshold": ["CRITICAL", "HIGH"],
            "conclusion": "success",
            "vulnerabilities": [],
        },
    }


def _candidate_reference(api, registry=None):
    from release.build_candidate import collect_candidate
    from release.verify_candidate import canonical

    repository = "VINIClUS/CnesData"
    _candidate_inputs(api, registry)
    candidate = collect_candidate(repository, SHA, api, NOW)
    data = canonical(candidate)
    name = f"production-candidate-{SHA}-93-1"
    api.add(
        name, {"candidate.json": data, "candidate.json.sha256": (_digest(data)[7:] + "\n").encode()}
    )
    artifact = api.metadata[-1]
    artifact["workflow_run"] = {"id": 93, "head_sha": "b" * 40, "head_branch": "develop"}
    api.routes["actions/artifacts/" + str(artifact["id"])] = artifact
    api.routes["actions/runs/93"] = {
        "id": 93,
        "run_attempt": 1,
        "head_sha": "b" * 40,
        "head_branch": "develop",
        "event": "workflow_dispatch",
        "status": "completed",
        "conclusion": "success",
        "path": ".github/workflows/release-candidate.yml",
        "repository": {"full_name": repository},
    }
    return {
        "repository": repository,
        "run_id": 93,
        "run_attempt": 1,
        "artifact_id": artifact["id"],
        "artifact_digest": artifact["digest"],
    }


def _candidate_inputs(api, registry):
    from release.verify_candidate import canonical

    repository = "VINIClUS/CnesData"
    api.routes["pulls/9"]["base"]["repo"]["full_name"] = repository
    if registry is not None:
        payload = api.payloads["production-oci-data_processor"]
        payload["scan.json"] = canonical(
            {"Metadata": {"ImageID": _digest(registry.config)}, "Results": []}
        )
        payload["SHA256SUMS"] = "".join(
            f"{_digest(data)[7:]}  {name}\n"
            for name, data in payload.items()
            if name != "SHA256SUMS"
        ).encode()
        api.refresh("production-oci-data_processor")
    for app in ("central_api", "data_processor"):
        name = "production-published-" + app
        published = json.loads(api.payloads[name]["manifest.json"])
        published["image"] = f"ghcr.io/{repository.lower()}/{app}"
        if registry is not None and app == "data_processor":
            published["digest"] = _digest(registry.manifest)
            published["config_digest"] = _digest(registry.config)
        data = canonical(published)
        api.payloads[name] = {
            "manifest.json": data,
            "manifest.sha256": f"{_digest(data)[7:]}  manifest.json\n".encode(),
        }
        api.refresh(name)
    for artifact in api.metadata:
        api.routes["actions/artifacts/" + str(artifact["id"])] = artifact
    command = (
        b"trivy image --input image.tar --scanners vuln \\\n"
        b"  --severity CRITICAL,HIGH --ignore-unfixed --exit-code 1 \\\n"
        b"  --ignorefile .trivyignore --format json --output scan.json\n"
    )
    import base64

    api.routes["contents/.github/workflows/verify-production.yml"] = {
        "type": "file",
        "encoding": "base64",
        "content": base64.b64encode(command).decode(),
    }


def _graph(role_only=False):
    address, before, after, producer, unknown, expressions = _graph_resources(role_only)
    module = "runtime_iam" if role_only else "processing"
    local = address.split(".", 2)[2].removesuffix("[0]")
    configuration = {
        "root_module": {
            "module_calls": {
                module: {"module": {"resources": [{"address": local, "expressions": expressions}]}}
            }
        }
    }
    plan = {
        "configuration": configuration,
        "resource_changes": [
            producer,
            {
                "mode": "managed",
                "type": "aws_iam_role_policy",
                "address": address,
                "change": {
                    "actions": ["create"] if before is None else ["update"],
                    "before": before,
                    "after": after,
                    "after_unknown": unknown,
                },
            },
        ],
    }
    files = {"infra/opentofu/modules/processing/step_functions.tf": b"synthetic source only"}
    approved = {
        "source_files": [{"path": key, "sha256": _digest(data)[7:]} for key, data in files.items()],
        "configuration_sha256": _digest(_bytes(configuration) + b"\n")[7:],
        "unknown_policies": [
            {
                "address": address,
                "references": sorted(set(expressions["policy"]["references"])),
                "producers": [producer["address"]],
                "before_policy_sha256": _digest(_bytes(json.loads(before["policy"])) + b"\n")[7:]
                if before
                else None,
            }
        ],
        "known_policies": {address: _digest(_bytes(json.loads(after["policy"])) + b"\n")[7:]}
        if role_only
        else {},
    }
    return plan, approved, files


def _graph_resources(role_only):
    unit = 'module.processing.aws_ecs_task_definition.processor["unit"]'
    address = "module.processing.aws_iam_role_policy.step_functions_ecs[0]"
    before = {
        "policy": json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["ecs:DescribeTasks", "ecs:StopTask"],
                        "Resource": "*",
                    }
                ],
            }
        )
    }
    expressions = {"policy": {"references": ["local.run_task_resources"]}}
    after = {"name": "cnesdata-prod-step-functions-ecs", "role": "sfn", "policy": None}
    producer = {
        "mode": "managed",
        "type": "aws_ecs_task_definition",
        "address": unit,
        "change": {
            "actions": ["create"],
            "before": None,
            "after": {"family": "cnesdata-prod-processor-unit"},
            "after_unknown": {"arn": True},
        },
    }
    unknown = {"policy": True}
    if role_only:
        return _role_graph()
    return address, before, after, producer, unknown, expressions


def _role_graph():
    role = "module.runtime_iam.aws_iam_role.canary_execution[0]"
    address = "module.runtime_iam.aws_iam_role_policy.canary_execution[0]"
    before = None
    after = {
        "name": "cnesdata-prod-canary-pull-logs",
        "role": None,
        "policy": json.dumps({"Version": "2012-10-17", "Statement": []}),
    }
    expressions = {
        "role": {
            "references": [
                "aws_iam_role.canary_execution[0].id",
                "aws_iam_role.canary_execution[0]",
                "aws_iam_role.canary_execution",
            ]
        },
        "policy": {"references": ["var.repository_arn", "var.log_group_arn"]},
    }
    producer = {
        "mode": "managed",
        "type": "aws_iam_role",
        "address": role,
        "change": {
            "actions": ["create"],
            "before": None,
            "after": {"name": "cnesdata-prod-canary-execution"},
            "after_unknown": {"id": True},
        },
    }
    unknown = {"role": True}
    return address, before, after, producer, unknown, expressions


def _public_artifact(api, name, members, producer):
    run_id, workflow = producer
    api.add(name, members)
    artifact = api.metadata[-1]
    artifact["workflow_run"] = {"id": run_id, "head_sha": SHA, "head_branch": "develop"}
    artifact["created_at"] = "2026-11-07T20:51:10Z" if run_id == 102 else "2026-11-07T20:30:00Z"
    api.routes[f"actions/artifacts/{artifact['id']}"] = artifact
    api.routes[f"actions/runs/{run_id}"] = {
        "id": run_id,
        "run_attempt": 1,
        "head_sha": SHA,
        "head_branch": "develop",
        "event": "workflow_dispatch",
        "status": "completed",
        "conclusion": "success",
        "path": workflow,
        "repository": {"full_name": "VINIClUS/CnesData"},
    }
    return {
        "repository": "VINIClUS/CnesData",
        "run_id": run_id,
        "run_attempt": 1,
        "artifact_id": artifact["id"],
        "artifact_digest": artifact["digest"],
        "workflow_path": workflow,
        "workflow_sha": SHA,
        "source_sha": SHA,
    }


class FixtureClock:
    def __init__(self):
        self.elapsed = 0.0

    def monotonic(self):
        return self.elapsed

    def sleep(self, delay):
        self.elapsed += delay

    def jitter(self):
        return 0.0

    def utcnow(self):
        return NOW + timedelta(seconds=self.elapsed)
