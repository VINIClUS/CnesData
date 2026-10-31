"""Slice 2 native contracts for the processor cluster, ASL and IAM seam."""

import json
import shutil
from pathlib import Path

import pytest

from tests.production.infra._processing_native import (
    A_RUN,
    B_RUN,
    DIGEST_A,
    DIGEST_B,
    MOCK_PROVIDERS,
    ROOT_RUN,
    RUN_INPUTS,
    NativePlans,
    Node,
    Tofu,
    _native_result,
    _override_hcl,
    _processing_runs,
    _service_identity_overrides,
)
from tests.production.infra._root_native_identities import (
    assert_root_identity_producers,
    merge_identity_records,
)
from tests.production.infra._step_functions_iam_contracts import (
    actions_in_runtime_iam,
    assert_no_step_functions_policies,
    assert_output_null,
    configuration_module,
    configuration_output_names,
    configuration_variable_names,
    event_for,
    exact_step_functions_trust,
    expected_machine_arn,
    module_resources_for,
    native_events,
    one_resource,
    output_value,
    prepare_isolated_case,
    resource_named,
)

PROCESSING_SOURCE = "../../modules/processing"
RUNTIME_IAM_SOURCE = "../../modules/runtime-iam"
ACCOUNT = "123456789012"
PREFIX = "cnesdata-prod"


def _processing_resources(native: NativePlans, root: Path, name: str, kind: str) -> list[Node]:
    return [
        resource
        for resource in module_resources_for(
            native.plans[name], native.configuration, root, PROCESSING_SOURCE
        )
        if resource["type"] == kind
    ]


def _step_role(native: NativePlans, root: Path, name: str) -> Node:
    roles = [
        resource
        for resource in module_resources_for(
            native.plans[name], native.configuration, root, RUNTIME_IAM_SOURCE
        )
        if resource["type"] == "aws_iam_role"
        and resource.get("change", {}).get("after", {}).get("name") == f"{PREFIX}-step-functions"
    ]
    return one_resource(roles, "step_functions_role")


def _task_from_definition(definition: Node) -> Node:
    assert definition.get("StartAt") == "RunUnits", "state_machine_start"
    states = definition.get("States", {})
    assert set(states) == {"RunUnits"}, f"state_machine_states={states!r}"
    mapping = states["RunUnits"]
    assert mapping["Type"] == "Map", "state_machine_map_type"
    assert mapping["ItemsPath"] == "$.unit_ids", "state_machine_items_path"
    assert mapping["MaxConcurrencyPath"] == "$.max_concurrency", "state_machine_concurrency_path"
    assert mapping["ItemSelector"] == {
        "tenant_id.$": "$.tenant_id",
        "run_id.$": "$.run_id",
        "wave_id.$": "$.wave_id",
        "dispatch_id.$": "$.dispatch_id",
        "unit_id.$": "$$.Map.Item.Value",
    }, f"state_machine_item_selector={mapping.get('ItemSelector')!r}"
    assert mapping["ItemProcessor"]["ProcessorConfig"] == {"Mode": "INLINE"}
    processor = mapping["ItemProcessor"]["States"]["RunProcessor"]
    assert processor["Type"] == "Task", "state_machine_task_type"
    assert processor["Resource"] == "arn:aws:states:::ecs:runTask.sync"
    assert "Catch" not in processor, "state_machine_catch_forbidden"
    return processor


def _run_block(
    shared: str,
    spec: tuple[str, str, str, str, str],
    overrides: list[Node],
) -> str:
    name, command, digest, active, retained = spec
    override_text = _override_hcl(overrides)
    return f'''run "{name}" {{
  command = {command}
{override_text}
  variables {{
    shared_outputs = {shared}
    image_digest = {digest}
    processor_active_unit_task_definition_arn = {active}
    processor_retained_unit_task_definition_arns = {retained}
  }}
}}
'''


def _stateful_suite(
    shared: Node, identities: list[Node], task_identities: dict[str, list[Node]]
) -> str:
    shared_value = json.dumps(shared["shared_outputs"], separators=(",", ":"))
    digest_a, digest_b = json.dumps(DIGEST_A), json.dumps(DIGEST_B)
    candidate_a = "run.phase_a_register_a.processor_task_definition_arns.unit"
    candidate_b = "run.phase_b_create_apply.processor_task_definition_arns.unit"
    route_b = "run.phase_a_register_b.processor_task_definition_arns.unit"
    specs = [
        ("bootstrap", "plan", "null", "null", "[]"),
        ("phase_a_register_a", "apply", digest_a, "null", "[]"),
        ("phase_b_create_plan", "plan", digest_a, candidate_a, f"[{candidate_a}]"),
        ("phase_b_create_apply", "apply", digest_a, candidate_a, f"[{candidate_a}]"),
        ("phase_a_register_b", "apply", digest_b, candidate_b, f"[{candidate_b}]"),
        ("phase_b_route_plan", "plan", digest_b, route_b, f"[{route_b}]"),
    ]
    machine = [item for item in identities if item["type"] == "aws_sfn_state_machine"]
    base = [item for item in identities if item not in machine]
    overrides = [
        base,
        base + task_identities[A_RUN],
        base + task_identities[A_RUN] + machine,
        base + task_identities[B_RUN] + machine,
    ]
    run_overrides = [
        overrides[0],
        overrides[1],
        overrides[2],
        overrides[2],
        overrides[3],
        overrides[3],
    ]
    return MOCK_PROVIDERS + "\n".join(
        _run_block(shared_value, spec, merge_identity_records(current))
        for spec, current in zip(specs, run_overrides, strict=True)
    )


def _sequence_inputs(
    tofu: Tofu, root: Path, native: NativePlans
) -> tuple[Node, list[Node], dict[str, list[Node]]]:
    evidence = Path(tofu[1]["TF_DATA_DIR"])
    try:
        shared = json.loads((evidence / "processing-shared-input.json").read_text())
        identities = json.loads((evidence / "processing-identity-overrides.json").read_text())
        task_identities = json.loads((evidence / "processing-task-identities.json").read_text())
        unit = next(item for item in task_identities[A_RUN] if item["mode_index"] == "unit")
        active = unit["fields"]["arn"]["value"]
        discovery = _processing_runs(
            tofu,
            root,
            (
                "processing-stateful-discovery",
                {
                    A_RUN: RUN_INPUTS[A_RUN]
                    | {
                        "processor_active_unit_task_definition_arn": active,
                        "processor_retained_unit_task_definition_arns": [active],
                    }
                },
                identities + task_identities[A_RUN],
            ),
        )
        schema = json.loads(
            (
                evidence / "processing-provider-schema/data/processing-schema-schema.stdout"
            ).read_text()
        )
        services = _service_identity_overrides(
            discovery[A_RUN], native.configuration, schema, root
        )
        return shared, identities + services, task_identities
    except (FileNotFoundError, KeyError, json.JSONDecodeError, TypeError) as error:
        pytest.fail(f"setup_error stateful_sequence_inputs={error}")


def _sequence_observation(
    evidence: Path, events: list[Node], seeds: list[Node]
) -> tuple[dict[str, Node], dict[str, Node]]:
    plans = {
        run: event_for(events, "test_plan", run)["test_plan"]
        for run in ("bootstrap", "phase_b_create_plan", "phase_b_route_plan")
    }
    for plan in plans.values():
        assert_root_identity_producers(plan, seeds)
    states = {
        run: event_for(events, "test_state", run)["test_state"]
        for run in ("phase_a_register_a", "phase_b_create_apply", "phase_a_register_b")
    }
    (evidence / "processing-slice2-sequence.events.json").write_text(
        json.dumps(events, indent=2) + "\n", encoding="utf-8"
    )
    return plans, states


def _write_sequence_suite(
    root: Path,
    shared: Node,
    identities: list[Node],
    task_identities: dict[str, list[Node]],
) -> Path:
    directory = root / ".terraform" / "processing-slice2-sequence-tests"
    directory.mkdir(parents=True, exist_ok=False)
    suite = _stateful_suite(shared, identities, task_identities)
    (directory / "slice2-sequence.tftest.hcl").write_text(suite, encoding="utf-8")
    return directory


def _stateful_sequence(
    tofu: Tofu, root: Path, native: NativePlans
) -> tuple[Node, dict[str, Node], dict[str, Node]]:
    evidence = Path(tofu[1]["TF_DATA_DIR"])
    shared, identities, task_identities = _sequence_inputs(tofu, root, native)
    directory = _write_sequence_suite(root, shared, identities, task_identities)
    try:
        result = _native_result(
            tofu,
            root,
            (
                "test",
                "-no-color",
                "-json",
                "-verbose",
                "-test-directory=.terraform/processing-slice2-sequence-tests",
            ),
            "processing-slice2-sequence",
        )
        if result.returncode != 0:
            pytest.fail(
                f"setup_error stateful_sequence rc={result.returncode} "
                f"stdout={result.stdout!r} stderr={result.stderr!r}"
            )
        events = native_events(result.stdout)
        runs = [
            "bootstrap",
            "phase_a_register_a",
            "phase_b_create_plan",
            "phase_b_create_apply",
            "phase_a_register_b",
            "phase_b_route_plan",
        ]
        statuses = {
            run: event_for(events, "test_run", run)["test_run"]["status"] for run in runs
        }
        if statuses != dict.fromkeys(runs, "pass"):
            pytest.fail(f"setup_error stateful_sequence_statuses={statuses!r}")
        diagnostics = [
            event["diagnostic"]
            for event in events
            if event.get("type") == "diagnostic"
            and event.get("diagnostic", {}).get("severity") == "error"
        ]
        if diagnostics:
            pytest.fail(f"setup_error stateful_sequence_diagnostics={diagnostics!r}")
        seeds = json.loads((evidence / "root-identity-records.json").read_text())["seeds"]
        plans, states = _sequence_observation(evidence, events, seeds)
        return shared, plans, states
    finally:
        shutil.rmtree(directory)


def _isolated_module_case(
    tofu: Tofu,
    prod_root: Path,
    case: tuple[Path, str, dict[str, object], str],
) -> list[Node]:
    tmp_path, source, variables, label = case
    root = tmp_path / label
    prepare_isolated_case(root, prod_root, (source, variables, label))
    local_tofu = (tofu[0], tofu[1] | {"TF_DATA_DIR": str(root / ".tofu-data")})
    init = _native_result(
        local_tofu,
        root,
        ("init", "-backend=false", "-input=false"),
        f"{label}-init",
    )
    if init.returncode != 0:
        pytest.fail(f"setup_error isolated_init_{label}={init.stderr!r}")
    result = _native_result(
        local_tofu,
        root,
        ("test", "-no-color", "-json", "-verbose", "-test-directory=tests"),
        label,
    )
    events = native_events(result.stdout)
    if not events:
        pytest.fail(f"setup_error isolated_events_missing_{label}={result.stderr!r}")
    return events


def _diagnostics(events: list[Node]) -> list[Node]:
    return [
        event
        for event in events
        if event.get("type") == "diagnostic"
        and event.get("diagnostic", {}).get("severity") == "error"
    ]


def test_bootstrap_expoe_cluster_role_e_saida_nula_da_machine(
    processing_native: NativePlans, prod_root: Path
) -> None:
    plan = processing_native.plans[ROOT_RUN]
    cluster = resource_named(
        plan, "aws_ecs_cluster", f"{PREFIX}-processor", "processor_cluster"
    )
    assert cluster["change"]["actions"] == ["create"]
    settings = cluster["change"]["after"].get("setting", [])
    assert settings == [{"name": "containerInsights", "value": "disabled"}]
    assert all(item["value"] not in {"enabled", "enhanced"} for item in settings)
    processing = _processing_resources(processing_native, prod_root, ROOT_RUN, "aws_ecs_cluster")
    assert len(processing) == 1, f"processing_cluster_count={len(processing)}"
    assert_absent = [
        resource
        for resource in _processing_resources(
            processing_native, prod_root, ROOT_RUN, "aws_sfn_state_machine"
        )
        if resource.get("change", {}).get("after", {}).get("name") == PREFIX
    ]
    assert not assert_absent, f"bootstrap_machine_unexpected={assert_absent!r}"
    assert_output_null(plan, "processor_state_machine_arn")
    assert output_value(plan, "processor_cluster_arn") == cluster["change"]["after"]["arn"]
    role = _step_role(processing_native, prod_root, ROOT_RUN)
    assert role["change"]["actions"] == ["create"]


def test_role_step_functions_tem_trust_exato_sem_policy_do_slice2(
    processing_native: NativePlans, prod_root: Path
) -> None:
    plan = processing_native.plans[ROOT_RUN]
    role = _step_role(processing_native, prod_root, ROOT_RUN)
    exact_step_functions_trust(role, ACCOUNT, expected_machine_arn(plan))
    assert_no_step_functions_policies(plan, role)
    assert "ecs:RunTask" not in actions_in_runtime_iam(
        plan, processing_native.configuration, prod_root
    )
    roles = [
        resource
        for resource in module_resources_for(
            plan, processing_native.configuration, prod_root, RUNTIME_IAM_SOURCE
        )
        if resource["type"] == "aws_iam_role"
    ]
    assert len(roles) == 5, f"runtime_role_count={len(roles)}"


def test_interfaces_publicas_de_processing_runtime_e_raiz_sao_estendidas(
    processing_native: NativePlans, prod_root: Path
) -> None:
    processing = configuration_module(processing_native.configuration, prod_root, PROCESSING_SOURCE)
    runtime = configuration_module(processing_native.configuration, prod_root, RUNTIME_IAM_SOURCE)
    assert "step_functions_role_arn" in configuration_variable_names(processing)
    assert {"cluster_arn", "state_machine_arn"} <= configuration_output_names(processing)
    assert "state_machine_arn" in configuration_variable_names(runtime)
    assert {"step_functions_role_id", "step_functions_role_arn"} <= configuration_output_names(
        runtime
    )
    root_outputs = set(processing_native.configuration["root_module"].get("outputs", {}))
    assert {
        "processor_cluster_arn",
        "processor_state_machine_arn",
        "processor_step_functions_role_id",
        "processor_step_functions_role_arn",
    } <= root_outputs


def _runtime_inputs(state_machine_arn: str | None, name_prefix: str = PREFIX) -> dict[str, object]:
    return {
        "name_prefix": name_prefix,
        "account_id": ACCOUNT,
        "repository_arn": f"arn:aws:ecr:us-east-2:{ACCOUNT}:repository/{PREFIX}",
        "log_group_arn": f"arn:aws:logs:us-east-2:{ACCOUNT}:log-group:/ecs/{PREFIX}-processor:*",
        "state_machine_arn": state_machine_arn,
    }
