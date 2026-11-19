"""Transporte e fronteiras nativas dos inputs IAM opcionais."""

import json
import shutil
from copy import deepcopy
from pathlib import Path

import pytest

from tests.production.infra._processing_native import Node, Tofu, _native_result
from tests.production.infra._step_functions_iam_contracts import (
    native_events,
    prepare_isolated_case,
)
from tests.production.infra.test_step_functions_iam import ACCOUNT, PREFIX, _runtime_inputs

type BoundaryCase = tuple[str, Node, frozenset[str]]
type BoundaryBatch = tuple[int, list[Node]]

MACHINE = f"arn:aws:states:us-east-2:{ACCOUNT}:stateMachine:{PREFIX}"
TABLE = f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/{PREFIX}-control-plane"
ROLE_ID = f"{PREFIX}-step-functions"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/{ROLE_ID}"
MACHINE_DIAG = "runtime_grants_machine=expected_own_machine"
TABLE_DIAG = "runtime_grants_table=expected_own_table"
INDEX_DIAG = "runtime_grants_indexes=expected_exact_gsi1_gsi4_gsi6"
BUCKET_DIAG = "runtime_grants_buckets=expected_own_data_and_audit"
API_DIAG = "api_role_arn_identity=expected_own_iam_role"
API_ENABLE_DIAG = "api_step_functions_enabled=requires_runtime_grants"
ID_DIAG = "step_functions_iam_role_id=expected_own_role_name"
ARN_DIAG = "step_functions_iam_role_arn=expected_own_role_arn"
DIAGNOSTICS = frozenset({MACHINE_DIAG, TABLE_DIAG, INDEX_DIAG, BUCKET_DIAG,
                         API_DIAG, API_ENABLE_DIAG, ID_DIAG, ARN_DIAG})

def _isolated_input_type(tmp_path: Path, name: str, value: object) -> str:
    source = tmp_path / "source"
    source.mkdir()
    (source / ".terraform.lock.hcl").write_text("", encoding="utf-8")
    wrapper = tmp_path / "wrapper"
    prepare_isolated_case(wrapper, source, ("runtime-iam", {name: value}, "transport"))
    main = (wrapper / "main.tf").read_text(encoding="utf-8")
    suite = (wrapper / "tests" / "transport.tftest.hcl").read_text(encoding="utf-8")
    assert f"{name}=var.{name}" in "".join(main.split()), "input_forwarding_missing"
    literal = next(line.split(" = ", 1)[1] for line in suite.splitlines()
                   if line.startswith(f"    {name} = "))
    actual = json.loads(literal)
    assert json.dumps(actual, sort_keys=True) == json.dumps(value, sort_keys=True), (
        "input_literal_changed"
    )
    declaration = main.split(f'variable "{name}" {{', 1)[1].split("}", 1)[0]
    return declaration.split("type =", 1)[1].split("nullable =", 1)[0].strip()


@pytest.mark.parametrize(("name", "value", "expected"), [
    ("state_machine_arn", f"arn:aws:states:us-east-2:{ACCOUNT}:stateMachine:{PREFIX}", "string"),
    ("state_machine_arn", None, "string"),
    ("processor_retained_unit_task_definition_arns", ["revision-a", "revision-b"], "list(string)"),
    ("audit_retention_days", 365, "number"),
    ("api_step_functions_enabled", True, "bool"),
    ("api_step_functions_enabled", False, "bool"),
], ids=["string", "null", "list", "int", "bool-true", "bool-false"])
def test_wrapper_isolado_preserva_tipo_e_valor_do_input(
    tmp_path: Path, name: str, value: object, expected: str,
) -> None:
    actual = _isolated_input_type(tmp_path, name, value)
    assert actual == expected, (
        f"input_transport_type name={name} expected={expected} actual={actual}"
    )


def test_wrapper_isolado_transporta_runtime_grants_aninhado_sem_coercao(tmp_path: Path) -> None:
    table = f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/{PREFIX}-control-plane"
    grants = {
        "table_arn": table,
        "index_arns": {"gsi1": f"{table}/index/gsi1", "gsi4": f"{table}/index/gsi4",
                       "gsi6": f"{table}/index/gsi6"},
        "data_bucket_arn": f"arn:aws:s3:::{PREFIX}-data",
        "audit_bucket_arn": f"arn:aws:s3:::{PREFIX}-audit",
        "api_role_arn": f"arn:aws:iam::{ACCOUNT}:role/service/{PREFIX}-api",
    }
    actual = _isolated_input_type(tmp_path, "runtime_grants", grants)
    assert actual == "any", f"input_transport_type name=runtime_grants expected=any actual={actual}"


def _runtime_boundary_inputs() -> Node:
    return _runtime_inputs(MACHINE) | {
        "runtime_grants": {
            "table_arn": TABLE,
            "index_arns": {name: f"{TABLE}/index/{name}" for name in ("gsi1", "gsi4", "gsi6")},
            "data_bucket_arn": f"arn:aws:s3:::{PREFIX}-data",
            "audit_bucket_arn": f"arn:aws:s3:::{PREFIX}-audit",
            "api_role_arn": f"arn:aws:iam::{ACCOUNT}:role/{PREFIX}-api",
        },
        "api_step_functions_enabled": False,
    }


def _api_invalid_values() -> dict[str, object]:
    role = f"arn:aws:iam::{ACCOUNT}:role/service/{PREFIX}-api"
    return {
        "null": None, "account": role.replace(ACCOUNT, "999999999999"),
        "partition": role.replace("arn:aws:", "arn:aws-cn:"),
        "service": role.replace(":iam:", ":sts:"),
        "region": role.replace("iam::", "iam:us-east-2:"),
        "user": role.replace(":role/", ":user/"),
        "policy": role.replace(":role/", ":policy/"),
        "assumed": role.replace(":role/", ":assumed-role/") + "/session",
        "empty-name": f"arn:aws:iam::{ACCOUNT}:role/",
        "empty-path": role.replace("/service/", "/service//"),
        "trailing": role + "/", "whitespace": role + " ",
        "star": role + "*", "question": role + "?",
        "long-name": f"arn:aws:iam::{ACCOUNT}:role/" + "a" * 65,
    }


def _runtime_field_mutations() -> list[tuple[str, str, object, str]]:
    rows: list[tuple[str, str, object, str]] = [
        ("table-null", "table_arn", None, TABLE_DIAG),
        ("table-account", "table_arn", TABLE.replace(ACCOUNT, "999999999999"), TABLE_DIAG),
        ("table-region", "table_arn", TABLE.replace("us-east-2", "us-east-1"), TABLE_DIAG),
        ("table-name", "table_arn", TABLE + "-foreign", TABLE_DIAG),
        ("indexes-null", "index_arns", None, INDEX_DIAG),
    ]
    for index in ("gsi1", "gsi4", "gsi6"):
        for suffix, value in (("null", None), ("name", f"{TABLE}/index/other"),
                              ("foreign", f"{TABLE}-foreign/index/{index}")):
            rows.append((f"{index}-{suffix}", f"index_arns.{index}", value, INDEX_DIAG))
    for bucket in ("data", "audit"):
        arn = f"arn:aws:s3:::{PREFIX}-{bucket}"
        for suffix, value in (("null", None), ("name", arn + "-foreign"),
                              ("object", arn + "/object"), ("wildcard", arn + "*")):
            rows.append((f"{bucket}-{suffix}", f"{bucket}_bucket_arn", value, BUCKET_DIAG))
    rows.extend((f"api-{label}", "api_role_arn", value, API_DIAG)
                for label, value in _api_invalid_values().items())
    return rows


def _runtime_boundary_cases() -> list[BoundaryCase]:
    base = _runtime_boundary_inputs()
    cases: list[BoundaryCase] = [("runtime-own-disabled", deepcopy(base), frozenset())]
    for label, api in (("simple", f"arn:aws:iam::{ACCOUNT}:role/{PREFIX}-api"),
                       ("path", f"arn:aws:iam::{ACCOUNT}:role/service/team/role-name_+=,.@-"),
                       ("name64", f"arn:aws:iam::{ACCOUNT}:role/" + "a" * 64)):
        inputs = deepcopy(base)
        inputs["api_step_functions_enabled"] = True
        inputs["runtime_grants"]["api_role_arn"] = api
        cases.append((f"runtime-own-enabled-{label}", inputs, frozenset()))
    for label, flag, machine in (("null-grants", False, MACHINE),
                                ("disabled-empty", False, None), ("null-flag", None, None)):
        inputs = base | {"runtime_grants": None, "api_step_functions_enabled": flag,
                         "state_machine_arn": machine}
        cases.append((f"runtime-{label}", inputs, frozenset()))
    for label, path, value, diagnostic in _runtime_field_mutations():
        inputs = deepcopy(base)
        target = inputs["runtime_grants"]
        parts = path.split(".")
        for key in parts[:-1]:
            target = target[key]
        target[parts[-1]] = value
        cases.append((f"runtime-{label}", inputs, frozenset({diagnostic})))
    dependencies = [
        ("grants-without-machine", False, False, None, {MACHINE_DIAG}),
        ("enabled-without-machine", False, True, None, {MACHINE_DIAG, API_ENABLE_DIAG}),
        ("enabled-without-grants", True, True, MACHINE, {API_ENABLE_DIAG}),
        ("enabled-without-both", True, True, None, {API_ENABLE_DIAG}),
    ]
    for label, no_grants, flag, machine, expected in dependencies:
        inputs = deepcopy(base)
        inputs.update(api_step_functions_enabled=flag, state_machine_arn=machine)
        if no_grants:
            inputs["runtime_grants"] = None
        cases.append((f"runtime-{label}", inputs, frozenset(expected)))
    return cases


def _processing_boundary_inputs() -> Node:
    return {
        "image_digest": None, "name_prefix": PREFIX,
        "repository_uri": f"{ACCOUNT}.dkr.ecr.us-east-2.amazonaws.com/{PREFIX}",
        "task_role_arns": {mode: f"arn:aws:iam::{ACCOUNT}:role/{PREFIX}-{mode}"
                           for mode in ("unit", "recovery", "audit")},
        "execution_role_arn": f"arn:aws:iam::{ACCOUNT}:role/{PREFIX}-execution",
        "log_group_name": f"/ecs/{PREFIX}-processor",
        "control_plane_table": f"{PREFIX}-control-plane",
        "data_bucket": f"{PREFIX}-data", "audit_bucket": f"{PREFIX}-audit",
        "audit_retention_days": 365, "state_machine_arn": MACHINE,
        "public_subnet_ids": ["subnet-production"], "security_group_id": "sg-production",
        "processor_active_unit_task_definition_arn": None,
        "processor_retained_unit_task_definition_arns": [],
        "step_functions_role_arn": ROLE_ARN, "step_functions_role_id": ROLE_ID,
        "enable_step_functions_iam": True,
    }


def _processing_boundary_cases() -> list[BoundaryCase]:
    base = _processing_boundary_inputs()
    cases: list[BoundaryCase] = [("processing-own-enabled", deepcopy(base), frozenset())]
    for label, flag, role in (("disabled-null", False, None), ("null-flag", None, None),
                              ("disabled-foreign", False, "foreign")):
        inputs = base | {"enable_step_functions_iam": flag, "step_functions_role_id": role,
                         "step_functions_role_arn": role}
        cases.append((f"processing-{label}", inputs, frozenset()))
    invalid_arns = {"null": None, "account": ROLE_ARN.replace(ACCOUNT, "999999999999"),
                    "partition": ROLE_ARN.replace("arn:aws:", "arn:aws-cn:"),
                    "service": ROLE_ARN.replace(":iam:", ":sts:"),
                    "name": ROLE_ARN + "-foreign", "id": ROLE_ID}
    for label, value in {"null": None, "name": "foreign", "arn": ROLE_ARN}.items():
        inputs = base | {"step_functions_role_id": value}
        cases.append((f"processing-id-{label}", inputs, frozenset({ID_DIAG})))
    for label, value in invalid_arns.items():
        inputs = base | {"step_functions_role_arn": value}
        cases.append((f"processing-arn-{label}", inputs, frozenset({ARN_DIAG})))
    inputs = base | {"step_functions_role_id": None, "step_functions_role_arn": None}
    cases.append(("processing-pair-null", inputs, frozenset({ID_DIAG, ARN_DIAG})))
    return cases


BOUNDARIES = {"runtime-iam": _runtime_boundary_cases(), "processing": _processing_boundary_cases()}
POSITIVE_CASES = [(group, case) for group, cases in BOUNDARIES.items()
                  for case in cases if not case[2]]
NEGATIVE_CASES = [(group, case) for group, cases in BOUNDARIES.items() for case in cases if case[2]]


def _prepare_boundary_suite(root: Path, prod_root: Path, group: str) -> None:
    cases = BOUNDARIES[group]
    source = f"../../modules/{group}"
    prepare_isolated_case(root, prod_root, (source, cases[0][1], "seed"))
    (root / "tests" / "seed.tftest.hcl").unlink()
    for label, inputs, _ in cases:
        scratch = root / "cases" / label
        prepare_isolated_case(scratch, prod_root, (source, inputs, label))
        shutil.copyfile(scratch / "tests" / f"{label}.tftest.hcl",
                        root / "tests" / f"{label}.tftest.hcl")


def _run_boundary_batch(tofu: Tofu, prod_root: Path, root: Path, group: str) -> BoundaryBatch:
    _prepare_boundary_suite(root, prod_root, group)
    local = (tofu[0], tofu[1] | {"TF_DATA_DIR": str(root / ".tofu-data")})
    init = _native_result(local, root, ("init", "-backend=false", "-input=false"), group + "-init")
    assert init.returncode == 0, f"setup_error input_boundary_init={init.stderr!r}"
    result = _native_result(
        local, root, ("test", "-no-color", "-json", "-verbose", "-test-directory=tests"), group,
    )
    events = native_events(result.stdout)
    assert events, "setup_error boundary_events_missing"
    assert result.returncode in {0, 1}, f"setup_error boundary_rc={result.returncode}"
    expected = {(f"tests/{label}.tftest.hcl", label) for label, _, _ in BOUNDARIES[group]}
    runs = [event for event in events if event.get("type") == "test_run"]
    assert len(runs) == len(expected), "setup_error input_boundary_run_count"
    assert {(event.get("@testfile"), event.get("@testrun")) for event in runs} == expected
    for event in events:
        if event.get("type") == "diagnostic":
            assert (event.get("@testfile"), event.get("@testrun")) in expected, (
                f"setup_error unscoped_input_diagnostic={event!r}"
            )
    return result.returncode, events


@pytest.fixture(scope="module")
def input_boundary_batches(
    tofu_context: Tofu, prod_root: Path, tmp_path_factory: pytest.TempPathFactory,
) -> dict[str, BoundaryBatch]:
    root = tmp_path_factory.mktemp("iam-inputs")
    return {group: _run_boundary_batch(tofu_context, prod_root, root / group, group)
            for group in BOUNDARIES}


def _boundary_events(batch: BoundaryBatch, label: str) -> list[Node]:
    return [event for event in batch[1]
            if event.get("@testfile") == f"tests/{label}.tftest.hcl"
            and event.get("@testrun") == label]


def _assert_boundary_pass(events: list[Node], case: BoundaryCase) -> Node:
    label, inputs, _ = case
    runs = [event["test_run"] for event in events if event.get("type") == "test_run"]
    assert len(runs) == 1, f"setup_error input_run_count={label}"
    assert runs[0]["status"] == "pass", f"input_run_not_pass={label}"
    assert not [event for event in events if event.get("type") == "diagnostic"], (
        f"setup_error input_boundary_diagnostics={label}"
    )
    plans = [event["test_plan"] for event in events if event.get("type") == "test_plan"]
    assert len(plans) == 1, f"setup_error input_plan_count={label}"
    assert plans[0].get("resource_changes"), f"setup_error input_plan_empty={label}"
    actual = {name: plans[0]["variables"][name]["value"] for name in inputs}
    assert json.dumps(actual, sort_keys=True) == json.dumps(inputs, sort_keys=True), (
        f"setup_error input_value_coercion={label}"
    )
    return plans[0]


@pytest.mark.parametrize(("group", "case"), POSITIVE_CASES,
                         ids=[case[0] for _, case in POSITIVE_CASES])
def test_inputs_validos_alcancam_child_sem_diagnostico(
    input_boundary_batches: dict[str, BoundaryBatch], group: str, case: BoundaryCase,
) -> None:
    plan = _assert_boundary_pass(_boundary_events(input_boundary_batches[group], case[0]), case)
    flag = "api_step_functions_enabled" if group == "runtime-iam" else "enable_step_functions_iam"
    child = plan["configuration"]["root_module"]["module_calls"]["subject"]["module"]
    assert child["variables"][flag]["type"] == "bool"
    assert child["variables"][flag]["default"] is False


def _input_diagnostic_tokens(events: list[Node]) -> set[str]:
    errors = [event["diagnostic"] for event in events if event.get("type") == "diagnostic"
        and event.get("diagnostic", {}).get("severity") == "error"]
    observed: set[str] = set()
    for error in errors:
        assert error.get("summary") == "Invalid value for variable", (
            f"setup_error unexpected_input_diagnostic={error!r}"
        )
        text = error.get("summary", "") + "\n" + error.get("detail", "")
        tokens = {token for token in DIAGNOSTICS if token in text}
        assert tokens, f"setup_error unexpected_input_diagnostic={error!r}"
        observed.update(tokens)
    return observed


@pytest.mark.parametrize(("group", "case"), NEGATIVE_CASES,
                         ids=[case[0] for _, case in NEGATIVE_CASES])
def test_inputs_invalidos_exigem_diagnosticos_exatos_do_child(
    input_boundary_batches: dict[str, BoundaryBatch], group: str, case: BoundaryCase,
) -> None:
    batch = input_boundary_batches[group]
    events = _boundary_events(batch, case[0])
    observed = _input_diagnostic_tokens(events)
    if not observed:
        _assert_boundary_pass(events, case)
        assert batch[0] == 0, f"setup_error unexpected_batch_rc={batch[0]}"
    assert observed == case[2], (
        f"missing_validation_diagnostic case={case[0]} expected={sorted(case[2])!r} "
        f"observed={sorted(observed)!r}"
    )
    runs = [event["test_run"] for event in events if event.get("type") == "test_run"]
    assert len(runs) == 1
    assert runs[0]["status"] == "error"
    assert batch[0] == 1


def _diagnostic_provenance_case(summary: str) -> tuple[dict[str, BoundaryBatch], BoundaryCase]:
    label = "runtime-enabled-without-machine"
    path = f"tests/{label}.tftest.hcl"
    expected = frozenset({"runtime_grants_machine=expected_own_machine",
                          "api_step_functions_enabled=requires_runtime_grants"})
    events: list[Node] = [
        {"type": "test_run", "@testfile": path, "@testrun": label,
         "test_run": {"path": path, "run": label, "status": "error"}},
        {"type": "diagnostic", "@testfile": path, "@testrun": label,
         "diagnostic": {"severity": "error", "summary": summary,
                        "detail": "runtime_grants_machine=expected_own_machine\n"
                        "api_step_functions_enabled=requires_runtime_grants"}},
    ]
    return {"runtime-iam": (1, events)}, (label, {}, expected)


@pytest.mark.parametrize("summary", [
    "Plugin did not respond", "Invalid value for input variable", "Unsupported argument",
], ids=["provider", "schema", "unsupported"])
def test_oraculo_rejeita_diagnostico_generico_que_ecoa_tokens_do_contrato(summary: str) -> None:
    batches, case = _diagnostic_provenance_case(summary)
    with pytest.raises(AssertionError, match="setup_error unexpected_input_diagnostic"):
        test_inputs_invalidos_exigem_diagnosticos_exatos_do_child(batches, "runtime-iam", case)


def test_oraculo_aceita_validacao_legitima_com_dois_tokens_no_mesmo_evento() -> None:
    batches, case = _diagnostic_provenance_case("Invalid value for variable")
    test_inputs_invalidos_exigem_diagnosticos_exatos_do_child(batches, "runtime-iam", case)


API_LITERAL_ARNS = {
    name: f"arn:aws:iam::{ACCOUNT}:role/{path}{PREFIX}-api"
    for name, path in {
        "plain": "service/team/",
        "colon": "service:tenant/team/",
        "interpolation": "service/${1+1}/",
        "directive": "service/%{if(true)}segment%{endif}/",
    }.items()
}


def _prepare_api_path_suite(root: Path, prod_root: Path, cases: tuple[str, ...]) -> None:
    source = "../../modules/runtime-iam"
    prepare_isolated_case(root, prod_root, (source, _runtime_boundary_inputs(), "seed"))
    (root / "tests" / "seed.tftest.hcl").unlink()
    for case in cases:
        label = f"api-path-{case}"
        inputs = _runtime_boundary_inputs()
        inputs["runtime_grants"]["api_role_arn"] = API_LITERAL_ARNS[case]
        scratch = root / "literal-cases" / case
        prepare_isolated_case(scratch, prod_root, (source, inputs, label))
        shutil.copyfile(scratch / "tests" / f"{label}.tftest.hcl",
                        root / "tests" / f"{label}.tftest.hcl")


def _run_api_path_group(tofu: Tofu, prod_root: Path, root: Path, cases: tuple[str, ...]) -> Node:
    _prepare_api_path_suite(root, prod_root, cases)
    data = root / ".tofu-data"
    data.mkdir()
    local = (tofu[0], tofu[1] | {"TF_DATA_DIR": str(data)})
    stage = "init"
    result = _native_result(
        local, root, ("init", "-backend=false", "-input=false"), "api-path-init",
    )
    if result.returncode == 0:
        stage = "test"
        result = _native_result(
            local, root, ("test", "-no-color", "-json", "-verbose", "-test-directory=tests"),
            "api-path",
        )
    return {"stage": stage, "rc": result.returncode, "events": native_events(result.stdout),
            "stdout": result.stdout, "stderr": result.stderr}


@pytest.fixture(scope="module")
def api_path_observations(
    tofu_context: Tofu, prod_root: Path, tmp_path_factory: pytest.TempPathFactory,
) -> dict[str, Node]:
    root = tmp_path_factory.mktemp("api-paths")
    observations = {}
    for cases in (("plain", "colon"), ("interpolation",), ("directive",)):
        batch = _run_api_path_group(tofu_context, prod_root, root / cases[0], cases)
        observations.update(dict.fromkeys(cases, batch))
    return observations


@pytest.mark.parametrize("case", list(API_LITERAL_ARNS))
def test_path_api_literal_alcanca_child_sem_interpretacao(
    api_path_observations: dict[str, Node], prod_root: Path, case: str,
) -> None:
    batch = api_path_observations[case]
    label = f"api-path-{case}"
    assert batch["rc"] == 0, (
        f"transport_expected_native_pass case={label} stage={batch['stage']} rc={batch['rc']} "
        f"stdout={batch['stdout']!r} stderr={batch['stderr']!r}"
    )
    assert batch["stage"] == "test", f"setup_error api_path_stage={batch['stage']}"
    events = _boundary_events((batch["rc"], batch["events"]), label)
    assert not [event for event in events if event.get("type") == "diagnostic"]
    runs = [event["test_run"] for event in events if event.get("type") == "test_run"]
    assert len(runs) == 1, f"setup_error api_path_run_count={label}"
    assert runs[0]["status"] == "pass", f"transport_expected_native_pass case={label}"
    plans = [event["test_plan"] for event in events if event.get("type") == "test_plan"]
    assert len(plans) == 1, f"setup_error api_path_plan_count={label}"
    plan = plans[0]
    assert plan.get("resource_changes"), f"setup_error api_path_plan_empty={label}"
    call = plan["configuration"]["root_module"]["module_calls"]["subject"]
    assert call["source"] == str((prod_root / "../../modules/runtime-iam").resolve())
    assert call["expressions"]["runtime_grants"] == {"references": ["var.runtime_grants"]}
    child_type = call["module"]["variables"]["runtime_grants"]["type"]
    assert child_type[0] == "object"
    assert child_type[1]["api_role_arn"] == "string"
    values = plan["variables"]
    assert values["state_machine_arn"]["value"] == MACHINE
    assert values["api_step_functions_enabled"]["value"] is False
    actual = values["runtime_grants"]["value"]["api_role_arn"]
    expected = API_LITERAL_ARNS[case]
    assert actual == expected, f"api_path_literal_changed expected={expected!r} actual={actual!r}"
