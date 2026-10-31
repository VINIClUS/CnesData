"""Observação nativa compartilhada da composição de processamento."""

import json
import shutil
import subprocess
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import NamedTuple, cast

import pytest

from tests.production.infra._aws_mock_defaults import MOCK_AWS_PROVIDERS
from tests.production.infra._processing_identities import (  # noqa: F401
    Node,
    _computed_fields,
    _configuration_address,
    _configuration_resources,
    _network_identity,
    _repository_identity,
    _role_identity,
    _task_identity,
)

type Tofu = tuple[str, dict[str, str]]
type Artifacts = dict[str, dict[str, object]]
type Plans = dict[str, Node]
type Registration = tuple[str, str, str, str]

ROOT_RUN, NULL_RUN = "raiz_avalia_processing", "raiz_processing_nulo"
DIGEST_A, DIGEST_B = "sha256:" + "a" * 64, "sha256:" + "b" * 64
A_RUN, B_RUN = "raiz_processing_digest_a", "raiz_processing_digest_b"
MODULE_SOURCE = "../../modules/processing"
RUN_INPUTS = {
    ROOT_RUN: {},
    NULL_RUN: {"image_digest": None},
    A_RUN: {"image_digest": DIGEST_A},
    B_RUN: {"image_digest": DIGEST_B},
}
COMMAND_MODES = {(): "unit", ("recover-once",): "recovery", ("dispatch-outbox-once",): "audit"}
MOCK_PROVIDERS = MOCK_AWS_PROVIDERS
PLAN_ARGUMENTS = (
    "test",
    "-no-color",
    "-json",
    "-verbose",
    "-test-directory=.terraform/processing-tests",
)


class NativePlans(NamedTuple):
    plans: Plans
    configuration: Node


def _native_result(
    tofu: Tofu, root: Path, arguments: Sequence[str], label: str
) -> subprocess.CompletedProcess[str]:
    argv = [tofu[0], f"-chdir={root}", *arguments]
    result = subprocess.run(  # noqa: S603
        argv, check=False, capture_output=True, cwd=root, env=tofu[1], text=True
    )
    evidence = Path(tofu[1]["TF_DATA_DIR"])
    (evidence / f"{label}.stdout").write_text(result.stdout, encoding="utf-8")
    (evidence / f"{label}.stderr").write_text(result.stderr, encoding="utf-8")
    command = {"argv": argv, "cwd": str(root), "environment": tofu[1], "rc": result.returncode}
    (evidence / f"{label}.command.json").write_text(
        json.dumps(command, indent=2) + "\n", encoding="utf-8"
    )
    return result


def _successful_plans(result: subprocess.CompletedProcess[str], expected: set[str]) -> Plans:
    if result.returncode != 0:
        pytest.fail(
            f"setup_error command=processing-plan rc={result.returncode} "
            f"stdout={result.stdout!r} stderr={result.stderr!r}"
        )
    statuses, plans, diagnostics = {}, {}, []
    for line in result.stdout.splitlines():
        message = json.loads(line)
        kind = message.get("type")
        if kind == "test_run":
            statuses[message["test_run"]["run"]] = message["test_run"]["status"]
        if kind == "test_plan":
            plans[message["@testrun"]] = message["test_plan"]
        if kind == "diagnostic" and message["diagnostic"]["severity"] == "error":
            diagnostics.append(message["diagnostic"])
    if statuses != dict.fromkeys(expected, "pass") or diagnostics or set(plans) != expected:
        pytest.fail(f"setup_error statuses={statuses!r} diagnostics={diagnostics!r}")
    if not all(plan.get("resource_changes") for plan in plans.values()):
        pytest.fail("setup_error native_plan=empty")
    return cast("Plans", plans)


def _suite(shared: Node, runs: Mapping[str, Node]) -> str:
    return MOCK_PROVIDERS + "".join(
        f'\nrun "{name}" {{\n  command = plan\n  variables {{\n'
        + "\n".join(f"    {key} = {json.dumps(value)}" for key, value in (shared | inputs).items())
        + "\n  }\n}\n"
        for name, inputs in runs.items()
    )


def _save_plans(evidence: Path, label: str, plans: Plans) -> None:
    for name, plan in plans.items():
        (evidence / f"{label}.{name}.plan.json").write_text(
            json.dumps(plan, indent=2) + "\n", encoding="utf-8"
        )


def _managed_resources(plan: Node, kind: str) -> list[Node]:
    return [
        resource
        for resource in plan["resource_changes"]
        if resource["mode"] == "managed" and resource["type"] == kind
    ]


def _module_names(configuration: Node, root: Path, source: str) -> list[str]:
    target = (root / source).resolve()
    return [
        name
        for name, call in configuration["root_module"].get("module_calls", {}).items()
        if (root / call["source"]).resolve() == target
    ]


def _module_resources(plan: Node, configuration: Node, root: Path, source: str) -> list[Node]:
    prefixes = tuple(f"module.{name}." for name in _module_names(configuration, root, source))
    return [
        resource
        for resource in plan["resource_changes"]
        if resource["mode"] == "managed"
        and _configuration_address(resource["address"]).startswith(prefixes)
    ]


def _identity_overrides(plan: Node, configuration: Node, schema: Node, root: Path) -> list[Node]:
    account = plan["variables"]["shared_outputs"]["value"]["account_id"]
    roles = [
        r
        for r in _module_resources(plan, configuration, root, "../../modules/runtime-iam")
        if r["type"] == "aws_iam_role"
    ]
    repositories = [
        r
        for r in _module_resources(plan, configuration, root, "../../modules/processor-registry")
        if r["type"] == "aws_ecr_repository"
    ]
    if len(roles) not in {4, 5} or len(repositories) != 1:
        pytest.fail(f"setup_error identity_producers roles={len(roles)} repos={len(repositories)}")
    network = [
        r
        for r in _module_resources(plan, configuration, root, "../../modules/network")
        if r["type"] in {"aws_security_group", "aws_subnet"}
    ]
    if sum(r["type"] == "aws_security_group" for r in network) != 1 or not any(
        r["type"] == "aws_subnet" for r in network
    ):
        pytest.fail("setup_error network_identity_producers")
    network_identities = [
        _network_identity(resource, configuration, schema, account) for resource in network
    ]
    if len({item["fields"]["id"]["value"] for item in network_identities}) != len(network):
        pytest.fail("setup_error network_identity_alias")
    return (
        [_role_identity(role, configuration, schema, account) for role in roles]
        + [_repository_identity(repositories[0], configuration, schema, account)]
        + network_identities
    )


def _service_identity(resource: Node, context: tuple[Node, Node, str]) -> Node:
    configuration, schema, account = context
    configured = _computed_fields(resource, configuration, schema, {"arn", "id"})
    values = resource["change"]["after"]
    name = values.get("name")
    provider = configuration["provider_config"][configured["provider_config_key"]]
    region = provider.get("expressions", {}).get("region", {}).get("constant_value")
    if (
        resource["type"] not in {"aws_ecs_cluster", "aws_sfn_state_machine"}
        or not isinstance(name, str)
        or not name
        or provider.get("name") != "aws"
        or provider.get("alias")
        or region != "us-east-2"
    ):
        pytest.fail(f"setup_error service_identity={resource['address']}")
    if resource["type"] == "aws_ecs_cluster":
        arn = f"arn:aws:ecs:{region}:{account}:cluster/{name}"
    else:
        arn = f"arn:aws:states:{region}:{account}:stateMachine:{name}"
    return {
        "address": resource["address"],
        "fields": {
            "arn": {
                "value": arn,
                "source": {"account_id": account, "region": region, "name": name},
            },
            "id": {
                "value": arn,
                "source": {"account_id": account, "region": region, "name": name},
            },
        },
        "configured_expressions": configured.get("expressions", {}),
        "provider": resource["provider_name"],
        "type": resource["type"],
    }


def _service_identity_overrides(
    plan: Node, configuration: Node, schema: Node, root: Path
) -> list[Node]:
    resources = [
        resource
        for resource in _module_resources(plan, configuration, root, MODULE_SOURCE)
        if resource["type"] in {"aws_ecs_cluster", "aws_sfn_state_machine"}
    ]
    if len(resources) != 2 or {resource["type"] for resource in resources} != {
        "aws_ecs_cluster",
        "aws_sfn_state_machine",
    }:
        pytest.fail("setup_error service_identity_resources")
    account = plan["variables"]["shared_outputs"]["value"]["account_id"]
    context = (configuration, schema, account)
    return [_service_identity(resource, context) for resource in resources]


def _override_hcl(identities: list[Node]) -> str:
    return "\n".join(
        f"override_resource {{\n  target = {item['address']}\n  values = "
        + json.dumps({field: data["value"] for field, data in item["fields"].items()})
        + "\n}\n"
        for item in identities
    )


def _provider_schema(tofu: Tofu, root: Path, artifacts: Artifacts) -> Node:
    configuration = cast("Node", artifacts["configuration"])
    sources = {
        p["full_name"] for p in configuration["provider_config"].values() if p["name"] == "aws"
    }
    if len(sources) != 1:
        pytest.fail(f"setup_error schema_provider_sources={sources!r}")
    source = sources.pop()
    version = cast("Node", artifacts["version"])["provider_selections"][source]
    directory = Path(tofu[1]["TF_DATA_DIR"]) / "processing-provider-schema"
    directory.mkdir(exist_ok=False)
    data = directory / "data"
    data.mkdir()
    context = (tofu[0], tofu[1] | {"TF_DATA_DIR": str(data)})
    required = json.dumps({"source": source, "version": f"={version}"})
    tooling = f"terraform {{\n  required_providers {{\n    aws = {required}\n  }}\n}}\n"
    (directory / "main.tf").write_text(tooling, encoding="utf-8")
    lock = directory / ".terraform.lock.hcl"
    shutil.copyfile(root / ".terraform.lock.hcl", lock)
    lock.chmod(0o444)
    results = {}
    commands = {
        "init": ("init", "-backend=false", "-input=false", "-lockfile=readonly"),
        "schema": ("providers", "schema", "-json"),
        "version": ("version", "-json"),
    }
    for label, arguments in commands.items():
        result = _native_result(context, directory, arguments, f"processing-schema-{label}")
        if result.returncode != 0:
            pytest.fail(f"setup_error provider_schema_{label} rc={result.returncode}")
        results[label] = result.stdout
    schema = json.loads(results["schema"])
    selected = json.loads(results["version"])["provider_selections"]
    if selected != {source: version} or source not in schema["provider_schemas"]:
        pytest.fail(f"setup_error schema_provider_selection={selected!r}")
    if lock.read_bytes() != (root / ".terraform.lock.hcl").read_bytes():
        pytest.fail("setup_error schema_lock_changed")
    return cast("Node", schema)


def _discovered_identities(
    tofu: Tofu, root: Path, artifacts: Artifacts, shared: Node
) -> list[Node]:
    configuration = cast("Node", artifacts["configuration"])
    evidence = Path(tofu[1]["TF_DATA_DIR"])
    suite = _suite(shared, {ROOT_RUN: {}})
    (root / ".terraform/processing-tests/processing.tftest.hcl").write_text(suite, encoding="utf-8")
    (evidence / "processing-discovery.tftest.hcl").write_text(suite, encoding="utf-8")
    discovery = _successful_plans(
        _native_result(tofu, root, PLAN_ARGUMENTS, "processing-discovery"), {ROOT_RUN}
    )
    _save_plans(evidence, "processing-discovery", discovery)
    if discovery[ROOT_RUN]["configuration"] != configuration:
        pytest.fail("setup_error discovery_configuration_mismatch")
    schema = _provider_schema(tofu, root, artifacts)
    identities = _identity_overrides(discovery[ROOT_RUN], configuration, schema, root)
    (evidence / "processing-identity-overrides.json").write_text(
        json.dumps(identities, indent=2) + "\n", encoding="utf-8"
    )
    return identities


def _task_identities(
    plan: Node, configuration: Node, root: Path, context: tuple[Node, int]
) -> list[Node]:
    schema, seed = context
    tasks = [
        resource
        for resource in _module_resources(plan, configuration, root, MODULE_SOURCE)
        if resource["type"] == "aws_ecs_task_definition"
    ]
    account = plan["variables"]["shared_outputs"]["value"]["account_id"]
    return [
        _task_identity(resource, configuration, schema, (account, seed + index + 1))
        for index, resource in enumerate(sorted(tasks, key=lambda item: item["address"]))
    ]


def _processing_runs(
    tofu: Tofu, root: Path, specification: tuple[str, Mapping[str, Node], list[Node]]
) -> Plans:
    label, runs, identities = specification
    evidence = Path(tofu[1]["TF_DATA_DIR"])
    shared = json.loads((evidence / "processing-shared-input.json").read_text())
    suite = _suite(shared, runs) + _override_hcl(identities)
    (root / ".terraform/processing-tests/processing.tftest.hcl").write_text(suite, encoding="utf-8")
    (evidence / f"{label}.tftest.hcl").write_text(suite, encoding="utf-8")
    plans = _successful_plans(_native_result(tofu, root, PLAN_ARGUMENTS, label), set(runs))
    _save_plans(evidence, label, plans)
    if label.startswith("processing-native"):
        _save_plans(evidence, "processing-native", plans)
    return plans


def _discover_task_identities(
    tofu: Tofu, root: Path, configuration: Node, identities: list[Node]
) -> dict[str, list[Node]]:
    evidence = Path(tofu[1]["TF_DATA_DIR"])
    runs = {name: RUN_INPUTS[name] for name in (A_RUN, B_RUN)}
    discovery = _processing_runs(tofu, root, ("processing-task-discovery", runs, identities))
    schema = json.loads(
        (evidence / "processing-provider-schema/data/processing-schema-schema.stdout").read_text()
    )
    tasks = {
        name: _task_identities(discovery[name], configuration, root, (schema, seed))
        for name, seed in [(A_RUN, 100), (B_RUN, 200)]
    }
    (evidence / "processing-task-identities.json").write_text(
        json.dumps(tasks, indent=2) + "\n", encoding="utf-8"
    )
    return tasks


@pytest.fixture(scope="session")
def processing_native(
    tofu_context: Tofu,
    tofu_artifacts: Artifacts,
    prod_root: Path,
    valid_shared_outputs: dict[str, object],
) -> Iterator[NativePlans]:
    directory = prod_root / ".terraform" / "processing-tests"
    directory.mkdir(parents=True, exist_ok=False)
    evidence = Path(tofu_context[1]["TF_DATA_DIR"])
    shared = {"shared_outputs": valid_shared_outputs}
    configuration = cast("Node", tofu_artifacts["configuration"])
    try:
        identities = _discovered_identities(tofu_context, prod_root, tofu_artifacts, shared)
        (evidence / "processing-shared-input.json").write_text(json.dumps(shared), encoding="utf-8")
        task_identities = _discover_task_identities(
            tofu_context, prod_root, configuration, identities
        )
        plans = _processing_runs(
            tofu_context,
            prod_root,
            (
                "processing-native",
                {name: RUN_INPUTS[name] for name in (ROOT_RUN, NULL_RUN)},
                identities,
            ),
        )
        streams: dict[str, str] = dict.fromkeys((ROOT_RUN, NULL_RUN), "processing-native")
        for name, label in [(A_RUN, "processing-native-a"), (B_RUN, "processing-native-b")]:
            plans |= _processing_runs(
                tofu_context,
                prod_root,
                (label, {name: RUN_INPUTS[name]}, identities + task_identities[name]),
            )
            streams[name] = label
        (evidence / "processing-native.streams.json").write_text(
            json.dumps(streams), encoding="utf-8"
        )
        if any(plan["configuration"] != configuration for plan in plans.values()):
            pytest.fail("setup_error configuration_mismatch")
        (evidence / "processing-native.config.json").write_text(
            json.dumps(configuration, indent=2) + "\n", encoding="utf-8"
        )
        (evidence / "processing-native.artifacts.json").write_text(
            json.dumps(tofu_artifacts, indent=2) + "\n", encoding="utf-8"
        )
        yield NativePlans(plans, configuration)
    finally:
        shutil.rmtree(directory)


@pytest.fixture(scope="session")
def processing_plan(processing_native: NativePlans) -> Node:
    return processing_native.configuration
