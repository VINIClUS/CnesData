"""Fontes reais e identidades sintéticas na fronteira de policies computadas."""

import copy
import json
from pathlib import Path
from typing import Any

from tests.production.delivery._phase_a_fixtures import ACCOUNT, ECR, REGION, _bytes, _digest


def _symbolic_sources():
    root = Path(__file__).resolve().parents[3]
    paths = {
        "env/prod": ("main.tf", "variables.tf", "providers.tf"),
        "modules/runtime-iam": ("canary.tf", "promotion.tf", "outputs.tf", "step-functions.tf"),
        "modules/processing": ("ecs.tf", "canary.tf", "step_functions.tf", "outputs.tf"),
        "modules/processor-registry": ("main.tf", "outputs.tf"),
        "modules/control-plane": ("main.tf", "outputs.tf"),
    }
    return {
        f"infra/opentofu/{directory}/{name}": (
            root / "infra/opentofu" / directory / name
        ).read_bytes()
        for directory, names in paths.items()
        for name in names
    }


def _symbolic_configuration(kind, role, policy):
    role_name = role.split(".aws_iam_role.")[1].removesuffix("[0]")
    role_ref = (
        "var.canary_verifier_role_id" if kind == "verifier" else f"aws_iam_role.{role_name}[0].id"
    )
    refs = (
        ["local.canary.table_arn", "local.canary"]
        if kind == "seeder"
        else [
            "local.canary_audit_task_definition_arn",
            "local.canary_task_arn_pattern",
            "aws_ecs_cluster.processor.arn",
            "aws_ecs_cluster.processor",
        ]
    )
    modules = _symbolic_modules(kind, role_name)
    module = "runtime_iam" if kind == "seeder" else "processing"
    modules[module]["module"]["resources"].insert(
        0,
        {
            "address": policy.split(".", 2)[2].removesuffix("[0]"),
            "expressions": {"policy": {"references": refs}, "role": {"references": [role_ref]}},
        },
    )
    return {"root_module": {"module_calls": modules, "resources": []}}, refs


def _symbolic_modules(kind, role_name):
    modules = {
        name: {
            "source": f"../../modules/{source}",
            "expressions": {},
            "module": {"resources": [], "outputs": {}},
        }
        for name, source in (("runtime_iam", "runtime-iam"), ("processing", "processing"))
    }
    _symbolic_inputs(modules)
    modules["runtime_iam"]["module"]["resources"] = [
        {
            "address": "aws_iam_role." + role_name,
            "expressions": {"name": {"references": ["var.name_prefix"]}},
        }
    ]
    modules["processing"]["module"]["resources"] = (
        [
            {
                "address": "aws_dynamodb_table.canary",
                "expressions": {"name": {"references": ["local.canary_name"]}},
            }
        ]
        if kind == "seeder"
        else [
            {
                "address": "aws_ecs_task_definition.processor",
                "expressions": {"family": {"references": ["var.name_prefix", "each.key"]}},
            },
            {
                "address": "aws_ecs_cluster.processor",
                "expressions": {"name": {"references": ["var.name_prefix"]}},
            },
        ]
    )
    modules["runtime_iam"]["module"]["outputs"] = {
        "canary_verifier_role_id": {"expression": {"references": ["aws_iam_role.canary_verifier"]}}
    }
    modules["processing"]["module"]["outputs"] = {
        "canary_table_arn": {"expression": {"references": ["aws_dynamodb_table.canary"]}}
    }
    return modules


def _symbolic_inputs(modules):
    modules["runtime_iam"]["expressions"] = {
        "name_prefix": {"references": ["local.name_prefix"]},
        "canary_release": {
            "references": [
                "var.canary_release_id",
                "var.canary_trusted_principal_arn",
                "module.processing.canary_table_arn",
                "module.processing.canary_gsi6_arn",
                "module.processing.canary_bucket_arn",
                "module.processing",
            ]
        },
    }
    modules["processing"]["expressions"] = {
        "name_prefix": {"references": ["local.name_prefix"]},
        "canary_verifier_role_id": {
            "references": ["module.runtime_iam.canary_verifier_role_id", "module.runtime_iam"]
        },
    }


def _symbolic_fields(kind):
    role = f"module.runtime_iam.aws_iam_role.canary_{kind}[0]"
    policy = (
        "module.runtime_iam.aws_iam_role_policy.canary_seeder[0]"
        if kind == "seeder"
        else "module.processing.aws_iam_role_policy.canary_verifier_run_task[0]"
    )
    table = "module.processing.aws_dynamodb_table.canary[0]"
    task = 'module.processing.aws_ecs_task_definition.processor["audit"]'
    fields = {
        role: {"name": f"cnesdata-prod-canary-{kind}", "assume_role_policy": "{}"},
        policy: {
            "name": f"cnesdata-prod-canary-{kind}-" + ("put" if kind == "seeder" else "run-task"),
            "policy": None,
            "role": None,
        },
    }
    if kind == "seeder":
        fields[table] = {"name": "cnesdata-prod-canary"}
    else:
        fields[task] = {
            "family": "cnesdata-prod-processor-audit",
            "skip_destroy": True,
            "track_latest": False,
            "container_definitions": json.dumps(
                [{"name": "processor", "image": ECR + "@sha256:" + "a" * 64}]
            ),
        }
        fields["module.processing.aws_ecs_cluster.processor"] = {
            "name": "cnesdata-prod-processor",
            "arn": f"arn:aws:ecs:{REGION}:{ACCOUNT}:cluster/cnesdata-prod-processor",
        }
    return role, policy, fields, table if kind == "seeder" else task


def symbolic_phase_a(kind="seeder"):
    role, policy, fields, identity = _symbolic_fields(kind)
    configuration, refs = _symbolic_configuration(kind, role, policy)
    inputs = {
        "image_digest": "sha256:" + "a" * 64,
        "canary_release_id": "c" * 32,
        "canary_trusted_principal_arn": f"arn:aws:iam::{ACCOUNT}:role/promotion",
        "canary_storage_enabled": True,
        "processor_retained_unit_task_definition_arns": [],
        "processor_active_unit_task_definition_arn": None,
    }
    witness = {
        "address": policy,
        "references": sorted(refs),
        "producers": sorted([role, identity]),
        "before_policy_sha256": None,
    }
    plan, approved = _symbolic_envelopes(configuration, fields, inputs, witness)
    observed = copy.deepcopy(fields)
    observed[role].update(
        id=fields[role]["name"], arn=f"arn:aws:iam::{ACCOUNT}:role/{fields[role]['name']}"
    )
    if kind == "seeder":
        observed[identity]["arn"] = (
            f"arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/cnesdata-prod-canary"
        )
    else:
        observed[identity]["arn"] = (
            f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/cnesdata-prod-processor-audit:2"
        )
    observed[policy]["role"] = fields[role]["name"]
    cluster = f"arn:aws:ecs:{REGION}:{ACCOUNT}:cluster/cnesdata-prod-processor"
    context = {
        "approved_graph": approved,
        "source_files": _symbolic_sources(),
        "resources": fields,
        "generated_inputs": inputs,
        "account_id": ACCOUNT,
        "region": REGION,
        "ecr_image": ECR + "@sha256:" + "a" * 64,
        "retained_before": [],
        "candidate_audit_arn": observed[identity]["arn"],
        "cluster_arn": cluster,
        "canary_verifier_role_id": fields[role]["name"],
    }
    return plan, context, observed, policy


def _symbolic_envelopes(configuration, fields, inputs, witness):
    policy = witness["address"]
    plan = {
        "configuration": configuration,
        "variables": {
            key: {"value": value}
            for key, value in (inputs | {"shared_outputs": {"account_id": ACCOUNT}}).items()
        },
        "resource_changes": [
            {
                "address": address,
                "mode": "managed",
                "type": address.split(".")[2],
                "change": {
                    "actions": ["no-op"] if ".aws_ecs_cluster." in address else ["create"],
                    "before": value if ".aws_ecs_cluster." in address else None,
                    "after": value,
                    "after_unknown": {"policy": True, "role": True}
                    if address == policy
                    else {"arn": True},
                },
            }
            for address, value in fields.items()
        ],
    }
    approved = {
        "source_files": [
            {"path": path, "sha256": _digest(data)[7:]}
            for path, data in sorted(_symbolic_sources().items())
        ],
        "configuration_sha256": _digest(_bytes(configuration) + b"\n")[7:],
        "known_policies": {},
        "unknown_policies": [witness],
    }
    return plan, approved


def symbolic_state(resources) -> dict[str, Any]:
    result = []
    for address, value in resources.items():
        module, kind, local = address.rsplit(".", 2)
        name = local.split("[", 1)[0]
        instance = {"schema_version": 0, "attributes": copy.deepcopy(value)}
        if "[" in local:
            instance["index_key"] = json.loads(local[local.index("[") + 1 : -1])
        result.append(
            {
                "module": module,
                "mode": "managed",
                "type": kind,
                "name": name,
                "provider": 'provider["registry.opentofu.org/hashicorp/aws"]',
                "instances": [instance],
            }
        )
    return {"resources": result}


def symbolic_readback_boundary(api, case, proof):
    import base64

    from tests.production.delivery._phase_a_fixtures import SHA

    plan, context, observed = case
    for path, data in context["source_files"].items():
        api.routes["contents/" + path] = {
            "type": "file",
            "encoding": "base64",
            "content": base64.b64encode(data).decode(),
        }
    api.read_applied_resources = lambda: observed
    fingerprints = {"resource_fingerprints": [], "grant_fingerprints": []}
    for address, values in observed.items():
        fields = {
            key: json.loads(value)
            if key in {"policy", "assume_role_policy", "container_definitions"}
            and type(value) is str
            else value
            for key, value in values.items()
            if key not in {"id", "arn"}
        }
        digest = _digest(_bytes({"address": address, "fields": fields}) + b"\n")[7:]
        group = (
            "grant_fingerprints" if ".aws_iam_role_policy." in address else "resource_fingerprints"
        )
        fingerprints[group].append({"address": address, "fingerprint_sha256": digest})
    report = _bytes({"graph_proof": proof}) + b"\n"
    digest = _digest(report)[7:]
    result = {
        "plan_json": plan,
        "plan": {"policy": {"evidence_sha256": digest}},
        "outputs": fingerprints,
    }
    state = symbolic_state(observed)
    state.update(version=4, serial=1, lineage="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", outputs={})
    state["resources"][0]["instances"][0]["private"] = base64.b64encode(b"x" * 262144).decode()
    bundle = {"reports": {digest: report}, "state_after": _bytes(state) + b"\n"}
    governed = context | {"delta": context, "source_sha": SHA}
    return result, bundle, governed
