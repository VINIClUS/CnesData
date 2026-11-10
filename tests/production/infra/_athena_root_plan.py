"""Plan nativo da raiz com revisões de scheduler, sobre a sessão do processing."""

import hashlib
import json
import re
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.production.infra._processing_native import (
    PLAN_ARGUMENTS,
    ROOT_RUN,
    NativePlans,
    Tofu,
    _configuration_address,
    _configuration_resources,
    _discovered_identities,
    _native_result,
    _processing_runs,
    _save_plans,
    _successful_plans,
    _suite,
)
from tests.production.infra._root_native_identities import (
    ROOT_PREFIXES,
    TARGETS,
    _assert_producer,
    _seed_record,
    merge_identity_records,
)

type Node = dict[str, Any]

ACCOUNT = "123456789012"
PREFIX = "cnesdata-prod"
SCHEDULED_RUN = "raiz_com_schedulers"
CLUSTER_ARN = f"arn:aws:ecs:us-east-2:{ACCOUNT}:cluster/{PREFIX}-processor"
REVISION = f"arn:aws:ecs:us-east-2:{ACCOUNT}:task-definition/{PREFIX}-processor-%s:%d"
SCHEDULER_INPUTS = {
    "scheduler_recovery_task_definition_arn": REVISION % ("recovery", 7),
    "scheduler_audit_task_definition_arn": REVISION % ("audit", 3),
}
CLUSTER_OVERRIDE = {
    "address": "module.processing.aws_ecs_cluster.processor",
    "fields": {"arn": {"value": CLUSTER_ARN}},
}


_CACHE: dict[str, Node] = {}
_CANARY_CACHE: dict[str, Node] = {}
CANARY_ID = "d155e7af91747d96917453791d3c2125"
CANARY_PRINCIPAL = f"arn:aws:iam::{ACCOUNT}:role/promotion-workflow"
CANARY_RUN = "raiz_com_canary"
CANARY_ROLES = (
    "canary_task",
    "canary_execution",
    "canary_seeder",
    "canary_verifier",
    "promotion_fence_operator",
)


def canary_policy_sources() -> dict[str, bytes]:
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


def _canary_overrides() -> list[Node]:
    table = f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/{PREFIX}-canary"
    bucket = f"arn:aws:s3:::{PREFIX}-canary"
    identities = [
        ("module.processing.aws_dynamodb_table.canary[0]", table),
        ("module.processing.aws_s3_bucket.canary[0]", bucket),
        (
            "aws_cloudwatch_log_group.processor",
            f"arn:aws:logs:us-east-2:{ACCOUNT}:log-group:/ecs/{PREFIX}-processor:*",
        ),
        (
            "module.processor_registry.aws_ecr_repository.this",
            f"arn:aws:ecr:us-east-2:{ACCOUNT}:repository/{PREFIX}-processor",
        ),
        (
            "module.control_plane.aws_dynamodb_table.control_plane",
            f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/{PREFIX}-control-plane",
        ),
    ]
    identities += [
        (f'module.processing.aws_ecs_task_definition.processor["{mode}"]', REVISION % (mode, 17))
        for mode in ("unit", "recovery", "audit")
    ]
    return [
        {"address": address, "fields": {"arn": {"value": arn}}} for address, arn in identities
    ] + [CLUSTER_OVERRIDE]


def _canary_role_seeds(artifacts: Node) -> list[Node]:
    contract = {"account_id": ACCOUNT, "name_prefix": PREFIX, "region": "us-east-2"}
    records = []
    for name in CANARY_ROLES:
        file = "promotion.tf" if name == "promotion_fence_operator" else "canary.tf"
        target = (
            f"module.runtime_iam.aws_iam_role.{name}[0]",
            name.replace("_", "-"),
            ["infra/opentofu/env/prod/main.tf", f"infra/opentofu/modules/runtime-iam/{file}"],
        )
        records.append(
            _seed_record(
                target,
                artifacts["configuration"],
                artifacts["schema"],
                contract,
            )
        )
    return records


def _assert_canary_producers(plan: Node, seeds: list[Node], active: bool) -> None:
    expected = {target[0] for target in TARGETS}
    if active:
        expected |= {f"module.runtime_iam.aws_iam_role.{name}[0]" for name in CANARY_ROLES}
    assert {seed["address"] for seed in seeds} == expected
    resources = [
        item for item in plan["resource_changes"] if item["address"].startswith(ROOT_PREFIXES)
    ]
    actual = {item["address"]: item for item in resources}
    assert len(resources) == len(expected), "canary_producer_count"
    assert set(actual) == expected, "canary_producer_inventory"
    configured = dict(_configuration_resources(plan["configuration"]["root_module"]))
    assert plan["variables"]["shared_outputs"]["value"]["account_id"] == ACCOUNT
    for seed in seeds:
        address = seed["address"]
        _assert_producer(actual[address], seed, configured[_configuration_address(address)])


def canary_root_plan(tofu: Tofu, prod_root: Path, artifacts: Node) -> Node:
    evidence = Path(tofu[1]["TF_DATA_DIR"])
    key = str(evidence)
    if key in _CANARY_CACHE:
        return _CANARY_CACHE[key]
    declared = artifacts["configuration"]["root_module"]["variables"]
    values = {
        "canary_release_id": CANARY_ID,
        "canary_trusted_principal_arn": CANARY_PRINCIPAL,
        "canary_storage_enabled": True,
    }
    inputs = {"image_digest": "sha256:" + "a" * 64}
    inputs |= {name: value for name, value in values.items() if name in declared}
    identities = json.loads((evidence / "processing-identity-overrides.json").read_text())
    seeds = json.loads((evidence / "root-identity-records.json").read_text())["seeds"]
    extra = _canary_role_seeds(artifacts) if "canary_release_id" in inputs else []
    shared = json.loads((evidence / "processing-shared-input.json").read_text())
    overrides = merge_identity_records(identities, extra, _canary_overrides())
    suite = _suite(shared, {CANARY_RUN: inputs}, overrides)
    (prod_root / ".terraform/processing-tests/processing.tftest.hcl").write_text(suite)
    (evidence / "canary-root.tftest.hcl").write_text(suite)
    result = _native_result(tofu, prod_root, PLAN_ARGUMENTS, "canary-root")
    plans = _successful_plans(result, {CANARY_RUN})
    _save_plans(evidence, "canary-root", plans)
    _assert_canary_producers(plans[CANARY_RUN], [*seeds, *extra], bool(extra))
    _CANARY_CACHE[key] = plans[CANARY_RUN]
    return plans[CANARY_RUN]


@pytest.fixture(name="canary_plan", scope="module")
def fixture_canary_plan(
    tofu_context: Tofu,
    tofu_artifacts: Node,
    prod_root: Path,
    valid_shared_outputs: Node,
) -> Iterator[Node]:
    directory = prod_root / ".terraform" / "processing-tests"
    owned = not directory.exists()
    directory.mkdir(parents=True, exist_ok=True)
    evidence = Path(tofu_context[1]["TF_DATA_DIR"])
    shared = {"shared_outputs": valid_shared_outputs}
    try:
        if not (evidence / "processing-identity-overrides.json").exists():
            _discovered_identities(tofu_context, prod_root, tofu_artifacts, shared)
            (evidence / "processing-shared-input.json").write_text(json.dumps(shared))
        yield canary_root_plan(tofu_context, prod_root, tofu_artifacts)
    finally:
        if owned:
            shutil.rmtree(directory)


def scheduled_root_plan(tofu: Tofu, prod_root: Path) -> Node:
    evidence = Path(tofu[1]["TF_DATA_DIR"])
    if str(evidence) in _CACHE:
        return _CACHE[str(evidence)]
    overrides = json.loads((evidence / "processing-identity-overrides.json").read_text())
    plans = _processing_runs(
        tofu,
        prod_root,
        ("athena-root", {SCHEDULED_RUN: SCHEDULER_INPUTS}, [*overrides, CLUSTER_OVERRIDE]),
    )
    _CACHE[str(evidence)] = plans[SCHEDULED_RUN]
    return plans[SCHEDULED_RUN]


def resources(plan: Node, kind: str) -> list[Node]:
    return [
        item
        for item in plan["resource_changes"]
        if item["mode"] == "managed" and item["type"] == kind
    ]


def after_of(plan: Node, address: str) -> Node:
    matches = [item for item in plan["resource_changes"] if item["address"] == address]
    assert len(matches) == 1, f"resource_missing address={address}"
    return matches[0]["change"]["after"]


def planned_output(plan: Node, name: str) -> Any:
    outputs = plan["planned_values"]["outputs"]
    assert name in outputs, f"output_missing name={name}"
    return outputs[name]["value"]


@pytest.fixture(name="bootstrap_plan", scope="module")
def fixture_bootstrap_plan(processing_native: NativePlans) -> Node:
    return processing_native.plans[ROOT_RUN]


@pytest.fixture(name="scheduled_plan", scope="module")
def fixture_scheduled_plan(
    processing_native: NativePlans, tofu_context: Tofu, prod_root: Path
) -> Node:
    assert processing_native.plans
    return scheduled_root_plan(tofu_context, prod_root)


@pytest.fixture(name="all_root_plans", scope="module")
def fixture_all_root_plans(processing_native: NativePlans, scheduled_plan: Node) -> dict[str, Node]:
    return {**processing_native.plans, SCHEDULED_RUN: scheduled_plan}


def phase_b_root_plans(tofu: Tofu, prod_root: Path, artifacts: Node) -> dict[str, Node]:
    from tests.production.infra._processing_native import _native_plan_events
    evidence = Path(tofu[1]["TF_DATA_DIR"])
    identities = json.loads((evidence / "processing-identity-overrides.json").read_text())
    shared = json.loads((evidence / "processing-shared-input.json").read_text())
    overrides = merge_identity_records(
        identities, _canary_role_seeds(artifacts), _canary_overrides())
    retained = [REVISION % ("unit", 7), REVISION % ("unit", 17)]
    common = {
        "image_digest": "sha256:" + "a" * 64,
        "canary_release_id": CANARY_ID,
        "canary_trusted_principal_arn": CANARY_PRINCIPAL,
        "canary_storage_enabled": True,
        "processor_retained_unit_task_definition_arns": retained,
        **SCHEDULER_INPUTS,
    }
    runs = {
        "seed_initial": common | {"processor_active_unit_task_definition_arn": None},
        "phase_b_initial": common | {"processor_active_unit_task_definition_arn": retained[1]},
        "seed_established": common | {"processor_active_unit_task_definition_arn": retained[0]},
        "phase_b_established": common | {"processor_active_unit_task_definition_arn": retained[1]},
    }
    suite = _suite(shared, runs, overrides)
    for name in ("seed_initial", "seed_established"):
        suite = suite.replace(
            f'run "{name}" {{\n  command = plan', f'run "{name}" {{\n  command = apply'
        )
    mock_root, source_hashes = _phase_b_mock_root(prod_root, evidence)
    directory = mock_root / "env/prod/.terraform/processing-tests"
    directory.mkdir(parents=True)
    (directory / "processing.tftest.hcl").write_text(suite)
    (evidence / "phase-b-root.tftest.hcl").write_text(suite)
    mock_tofu = (tofu[0], tofu[1] | {"TF_DATA_DIR": str(evidence / "phase-b-mock-data")})
    initialized = _native_result(
        mock_tofu, mock_root / "env/prod",
        ["init", "-backend=false", "-input=false", "-lockfile=readonly"],
        "phase-b-mock-init",
    )
    assert initialized.returncode == 0, initialized.stdout + initialized.stderr
    result = _native_result(mock_tofu, mock_root / "env/prod", PLAN_ARGUMENTS, "phase-b-root")
    statuses, plans, diagnostics = _native_plan_events(result.stdout)
    assert result.returncode == 0, result.stdout + result.stderr
    assert statuses == dict.fromkeys(runs, "pass")
    assert not diagnostics
    for relative, binding in source_hashes.items():
        assert (
            hashlib.sha256((prod_root.parents[1] / relative).read_bytes()).hexdigest()
            == binding["sha256"]
        )
    _save_plans(evidence, "phase-b-root", plans)
    return {name: plans[name] for name in ("phase_b_initial", "phase_b_established")}


def _phase_b_mock_root(prod_root: Path, evidence: Path) -> tuple[Path, Node]:
    mock_root = evidence / "phase-b-mock-root"
    shutil.copytree(prod_root.parents[1], mock_root, ignore=shutil.ignore_patterns(".terraform"))
    source_hashes = {}
    for source in (prod_root.parents[1] / "modules").rglob("*.tf"):
        relative = source.relative_to(prod_root.parents[1])
        original = source.read_bytes()
        transformed, count = re.subn(
            rb"prevent_destroy\s*=\s*true", b"prevent_destroy = false", original
        )
        if count:
            (mock_root / relative).write_bytes(transformed)
            source_hashes[str(relative)] = {
                "sha256": hashlib.sha256(original).hexdigest(),
                "mock_cleanup_overrides": count,
            }
    assert sum(item["mock_cleanup_overrides"] for item in source_hashes.values()) == 7
    (evidence / "phase-b-mock-lifecycle.json").write_text(json.dumps(source_hashes, indent=2))
    return mock_root, source_hashes
