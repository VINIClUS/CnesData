from pathlib import Path

import pytest

from tests.production.infra.test_step_functions_iam import (
    ACCOUNT,
    PREFIX,
    PROCESSING_SOURCE,
    ROOT_RUN,
    RUNTIME_IAM_SOURCE,
    NativePlans,
    Tofu,
    _diagnostics,
    _isolated_module_case,
    _runtime_inputs,
    configuration_module,
    configuration_variable_names,
    event_for,
    expected_machine_arn,
)

RUNTIME_INVALIDS = {
    "account": "arn:aws:states:us-east-2:999999999999:stateMachine:cnesdata-prod",
    "region": f"arn:aws:states:us-east-1:{ACCOUNT}:stateMachine:{PREFIX}",
    "name": f"arn:aws:states:us-east-2:{ACCOUNT}:stateMachine:other",
    "service": f"arn:aws:sfn:us-east-2:{ACCOUNT}:stateMachine:{PREFIX}",
    "malformed": "state-machine-not-an-arn",
    "qualified": f"arn:aws:states:us-east-2:{ACCOUNT}:stateMachine:{PREFIX}:alias",
}


@pytest.fixture
def runtime_context(
    processing_native: NativePlans, prod_root: Path, tofu_context: Tofu, tmp_path: Path
) -> tuple[NativePlans, Path, Tofu, Path]:
    return processing_native, prod_root, tofu_context, tmp_path


@pytest.mark.parametrize("case", sorted(RUNTIME_INVALIDS))
def test_validacao_nativa_rejeita_identidade_literal_da_machine(
    runtime_context: tuple[NativePlans, Path, Tofu, Path],
    case: str,
) -> None:
    processing_native, prod_root, tofu_context, tmp_path = runtime_context
    runtime = configuration_module(processing_native.configuration, prod_root, RUNTIME_IAM_SOURCE)
    if "state_machine_arn" not in configuration_variable_names(runtime):
        pytest.fail("setup_error runtime_state_machine_arn_interface_missing")
    events = _isolated_module_case(
        tofu_context,
        prod_root,
        (
            tmp_path,
            RUNTIME_IAM_SOURCE,
            _runtime_inputs(RUNTIME_INVALIDS[case]),
            f"runtime-invalid-{case}",
        ),
    )
    run = event_for(events, "test_run", f"runtime-invalid-{case}")

    assert run["test_run"]["status"] == "error", f"invalid_status={run!r}"
    diagnostics = _diagnostics(events)
    assert diagnostics, "invalid_machine_identity_missing_native_diagnostic"
    assert all(
        item.get("@testfile") == f"tests/runtime-invalid-{case}.tftest.hcl"
        for item in diagnostics
    )
    assert all(
        item.get("@testrun") == f"runtime-invalid-{case}" for item in diagnostics
    )
    assert any(
        "state_machine_arn_identity=expected_own_unqualified_machine"
        in item.get("diagnostic", {}).get("detail", "")
        or "state_machine_arn_identity=expected_own_unqualified_machine"
        in item.get("diagnostic", {}).get("summary", "")
        for item in diagnostics
    )


@pytest.mark.parametrize(
    ("case", "value", "name_prefix"),
    [
        ("null", None, PREFIX),
        ("exact", f"arn:aws:states:us-east-2:{ACCOUNT}:stateMachine:{PREFIX}", PREFIX),
        (
            "metachar_prefix",
            f"arn:aws:states:us-east-2:{ACCOUNT}:stateMachine:cnesdata.prod",
            "cnesdata.prod",
        ),
    ],
)
def test_validacao_nativa_aceita_null_e_arn_literal_exato(
    runtime_context: tuple[NativePlans, Path, Tofu, Path],
    case: str,
    value: str | None,
    name_prefix: str,
) -> None:
    processing_native, prod_root, tofu_context, tmp_path = runtime_context
    runtime = configuration_module(processing_native.configuration, prod_root, RUNTIME_IAM_SOURCE)
    if "state_machine_arn" not in configuration_variable_names(runtime):
        pytest.fail("setup_error runtime_state_machine_arn_interface_missing")
    events = _isolated_module_case(
        tofu_context,
        prod_root,
        (
            tmp_path,
            RUNTIME_IAM_SOURCE,
            _runtime_inputs(value, name_prefix),
            f"runtime-valid-{case}",
        ),
    )
    run = event_for(events, "test_run", f"runtime-valid-{case}")
    assert run["test_run"]["status"] == "pass", f"valid_status={run!r}"
    assert not _diagnostics(events)


def test_active_sem_role_tem_diagnostico_nativo_explicito(
    processing_native: NativePlans,
    prod_root: Path,
    tofu_context: Tofu,
    tmp_path: Path,
) -> None:

    processing = configuration_module(processing_native.configuration, prod_root, PROCESSING_SOURCE)
    if "step_functions_role_arn" not in configuration_variable_names(processing):
        pytest.fail("setup_error processing_step_functions_role_interface_missing")
    active = f"arn:aws:ecs:us-east-2:{ACCOUNT}:task-definition/{PREFIX}-processor-unit:101"
    variables = {
        "image_digest": None,
        "name_prefix": PREFIX,
        "repository_uri": f"{ACCOUNT}.dkr.ecr.us-east-2.amazonaws.com/{PREFIX}",
        "task_role_arns": {
            mode: f"arn:aws:iam::{ACCOUNT}:role/{PREFIX}-{mode}"
            for mode in ("unit", "recovery", "audit")
        },
        "execution_role_arn": f"arn:aws:iam::{ACCOUNT}:role/{PREFIX}-execution",
        "log_group_name": f"/ecs/{PREFIX}-processor",
        "control_plane_table": f"{PREFIX}-control-plane",
        "data_bucket": f"{PREFIX}-data",
        "audit_bucket": f"{PREFIX}-audit",
        "audit_retention_days": 365,
        "state_machine_arn": expected_machine_arn(processing_native.plans[ROOT_RUN]),
        "public_subnet_ids": ["subnet-production"],
        "security_group_id": "sg-production",
        "processor_active_unit_task_definition_arn": active,
        "processor_retained_unit_task_definition_arns": [active],
        "step_functions_role_arn": None,
    }
    events = _isolated_module_case(
        tofu_context,
        prod_root,
        (tmp_path, PROCESSING_SOURCE, variables, "processing-active-without-role"),
    )
    run = event_for(events, "test_run", "processing-active-without-role")
    assert run["test_run"]["status"] == "error"
    assert any(
        "step_functions_role_required" in item.get("diagnostic", {}).get("detail", "")
        or "step_functions_role_required" in item.get("diagnostic", {}).get("summary", "")
        for item in _diagnostics(events)
    )
