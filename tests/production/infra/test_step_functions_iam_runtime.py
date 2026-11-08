import json
from copy import deepcopy
from pathlib import Path

import pytest

from tests.production.infra._step_functions_iam_contracts import (
    Grants,
    Node,
    assert_no_step_functions_policies,
    assert_policy_grants,
    exact_step_functions_trust,
    literal_grants,
    managed_resources,
    policy_grants,
    resource_at,
)
from tests.production.infra.test_ecr_ecs import (
    _assert_ecs_trust,
    _assert_execution_grants,
    _role_policy_bindings,
)
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
    _stateful_sequence,
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


def _runtime_resources(plan: Node) -> Node:
    resources = {
        "table": "module.control_plane.aws_dynamodb_table.control_plane",
        "data": "module.data_buckets.aws_s3_bucket.data",
        "audit": "module.data_buckets.aws_s3_bucket.audit",
    }
    result = {key: resource_at(plan, address)["change"]["after"]["arn"]
              for key, address in resources.items()}
    result["machine"] = expected_machine_arn(plan)
    account = plan["variables"]["shared_outputs"]["value"]["account_id"]
    result["executions"] = f"arn:aws:states:us-east-2:{account}:execution:{PREFIX}:*"
    return result


def _leading_keys(values: tuple[str, ...], operator: str = "StringLike") -> Node:
    return {f"ForAllValues:{operator}": {"dynamodb:LeadingKeys": list(values)},
            "Null": {"dynamodb:LeadingKeys": "false"}}


def _object_grants(bucket: str, prefixes: tuple[str, ...], actions: tuple[str, ...]) -> Grants:
    return literal_grants(actions, tuple(f"{bucket}/{prefix}" for prefix in prefixes), {})


def _list_grants(bucket: str, prefixes: tuple[str, ...]) -> Grants:
    condition = {"StringLike": {"s3:prefix": list(prefixes)}}
    return literal_grants(("s3:ListBucket",), (bucket,), condition)


def sfn_grants(resources: Node) -> Grants:
    return (
        literal_grants(("states:DescribeStateMachine", "states:StartExecution"),
                       (resources["machine"],), {})
        | literal_grants(("states:DescribeExecution", "states:StopExecution"),
                         (resources["executions"],), {})
    )


def _runtime_grants(resources: Node) -> dict[str, Grants]:
    table, data, audit = resources["table"], resources["data"], resources["audit"]
    publish = ("tmp/*", "normalized/*", "reconciliation/*", "serving/*")
    readable = ("raw/*", *publish)
    keys = ("TENANT#*", "TENANT#*#RUN#*", "TENANT#_GLOBAL",
            "ENV#prod#FENCE", "ENV#prod#SEMAPHORE")
    ddb = ("dynamodb:GetItem", "dynamodb:Query", "dynamodb:ConditionCheckItem",
           "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem")
    shared = (
        sfn_grants(resources)
        | literal_grants(ddb, (table,), _leading_keys(keys))
        | literal_grants(("dynamodb:UpdateItem",), (table,),
                         _leading_keys(("ENV#prod#ATTEMPTS",), "StringEquals"))
    )
    return {
        "unit": shared | _object_grants(data, readable, ("s3:GetObject",))
        | _object_grants(data, publish, ("s3:PutObject",)) | _list_grants(data, readable),
        "recovery": shared | literal_grants(("dynamodb:Query",), (table + "/index/gsi4",),
                                            _leading_keys(("RUN_RECOVERABLE",), "StringEquals"))
        | _object_grants(data, publish, ("s3:GetObject", "s3:PutObject"))
        | _list_grants(data, publish),
        "audit": literal_grants(("dynamodb:Query",), (table + "/index/gsi6",),
                                _leading_keys(("OUTBOX#PENDING",), "StringEquals"))
        | literal_grants(("dynamodb:GetItem", "dynamodb:UpdateItem"), (table,),
                         _leading_keys(("TENANT#_GLOBAL", "OUTBOX#CURSOR"), "StringEquals"))
        | literal_grants(("s3:GetBucketObjectLockConfiguration",), (audit,), {})
        | _object_grants(audit, ("audit/*",), ("s3:PutObject", "s3:PutObjectRetention",
                                             "s3:GetObject", "s3:GetObjectRetention")),
        "api": literal_grants(("dynamodb:Query",), (table + "/index/gsi1",),
                              _leading_keys(("USER#*",)))
        | literal_grants(("dynamodb:GetItem",), (table,), _leading_keys(("TENANT#*",)))
        | _object_grants(data, ("reconciliation/*", "serving/*"), ("s3:GetObject",))
        | _list_grants(data, ("reconciliation/*", "serving/*")),
    }


def _runtime_actor(plan: Node, mode: str) -> tuple[Node, Node]:
    if mode == "api":
        arn = plan["variables"]["shared_outputs"]["value"]["roles_anywhere"]["api_role_arn"]
        role = {"change": {"after": {"name": arn.rsplit("/", 1)[1], "arn": arn}}}
        owned = managed_resources(plan, "aws_iam_role")
        assert not any(item["change"]["after"]["arn"] == arn for item in owned), (
            "binding api_role_owned"
        )
        address = "module.runtime_iam.aws_iam_role_policy.api[0]"
        name = f"{PREFIX}-api-runtime"
    else:
        role = resource_at(plan, f'module.runtime_iam.aws_iam_role.task["{mode}"]')
        address = f'module.runtime_iam.aws_iam_role_policy.runtime["{mode}"]'
        name = f"{PREFIX}-processor-{mode}-runtime"
    return role, {"address": address, "name": name, "allowed": [address]}


@pytest.mark.parametrize("mode", ["unit", "recovery", "audit", "api"])
def test_runtime_e_api_recebem_grants_literais_exatos_no_bootstrap(
    processing_native: NativePlans, mode: str,
) -> None:
    plan = processing_native.plans[ROOT_RUN]
    active = plan["variables"]["processor_active_unit_task_definition_arn"]["value"]
    assert active is None, "bootstrap_active_expected_null"
    expected = _runtime_grants(_runtime_resources(plan))[mode]
    role, specification = _runtime_actor(plan, mode)
    assert_policy_grants(plan, role, specification, expected)


def test_runtime_isolado_com_machine_sem_grants_mantem_role_sem_policy(
    runtime_context: tuple[NativePlans, Path, Tofu, Path],
) -> None:
    native, root, tofu, tmp_path = runtime_context
    machine = expected_machine_arn(native.plans[ROOT_RUN])
    label = "runtime-role-only"
    events = _isolated_module_case(
        tofu, root, (tmp_path, RUNTIME_IAM_SOURCE, _runtime_inputs(machine), label),
    )
    assert event_for(events, "test_run", label)["test_run"]["status"] == "pass"
    assert not _diagnostics(events)
    plan = event_for(events, "test_plan", label)["test_plan"]
    roles = managed_resources(plan, "aws_iam_role")
    assert len(roles) == 5
    policies = managed_resources(plan, "aws_iam_role_policy")
    addresses = [item["address"] for item in policies]
    assert addresses == ["module.subject.aws_iam_role_policy.execution"]
    role = resource_at(plan, "module.subject.aws_iam_role.step_functions[0]")
    exact_step_functions_trust(role, ACCOUNT, machine)
    assert_no_step_functions_policies(plan, role)
    ecs_roles = [item for item in roles if item is not role]
    assert {item["change"]["after"]["name"] for item in ecs_roles} == {
        f"{PREFIX}-processor-{mode}" for mode in ("execution", "unit", "recovery", "audit")
    }
    for ecs_role in ecs_roles:
        _assert_ecs_trust(ecs_role, ACCOUNT)
        values = ecs_role["change"]["after"]
        assert not values.get("inline_policy"), "isolated_runtime_inline_policies"
        assert not values.get("managed_policy_arns"), "isolated_runtime_managed_policies"
        documents, arns = _role_policy_bindings(ecs_role, plan, plan["configuration"])
        assert not arns, f"isolated_runtime_managed_policies={arns!r}"
        if values["name"] == f"{PREFIX}-processor-execution":
            _assert_execution_grants(
                documents,
                f"arn:aws:ecr:us-east-2:{ACCOUNT}:repository/{PREFIX}",
                f"arn:aws:logs:us-east-2:{ACCOUNT}:log-group:/ecs/{PREFIX}-processor:*",
            )
        else:
            assert not documents, f"isolated_runtime_task_policies={ecs_role['address']}"


def _policy_document(statements: list[Node]) -> str:
    return json.dumps({"Version": "2012-10-17", "Statement": statements})


@pytest.mark.parametrize("field", [
    "NotAction", "NotResource", "Principal", "NotPrincipal", "Extra",
])
def test_oraculo_rejeita_campos_de_policy_fora_do_contrato(field: str) -> None:
    statement = {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:fixture"}
    statement[field] = "*"
    with pytest.raises(AssertionError, match="policy_shape unexpected_fields"):
        policy_grants(_policy_document([statement]))


@pytest.mark.parametrize("case", ["statement", "action", "resource", "overlap"])
def test_oraculo_rejeita_duplicatas_antes_de_converter_grants_em_set(case: str) -> None:
    statement: Node = {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": ["arn:fixture"]}
    other = deepcopy(statement)
    statements = [statement]
    reason = "duplicate_grant"
    if case == "statement":
        statements.append(other)
        reason = "duplicate_statement"
    elif case == "overlap":
        other["Action"].append("s3:PutObject")
        statements.append(other)
    else:
        field = "Action" if case == "action" else "Resource"
        statement[field] *= 2
    with pytest.raises(AssertionError, match=reason):
        policy_grants(_policy_document(statements))


def test_oraculo_rejeita_json_invalido_e_chaves_json_repetidas() -> None:
    for raw in ("{invalid", '{"Version":"2012-10-17","Version":"2012-10-17","Statement":[]}'):
        with pytest.raises(AssertionError, match="policy_shape"):
            policy_grants(raw)


def test_oraculo_normaliza_ordem_sem_inferir_permissoes() -> None:
    statement = {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObject"],
                 "Resource": ["arn:second", "arn:first"], "Condition": {}}
    expected = frozenset({("s3:GetObject", "arn:first", "{}"),
                          ("s3:GetObject", "arn:second", "{}"),
                          ("s3:PutObject", "arn:first", "{}"),
                          ("s3:PutObject", "arn:second", "{}")})
    assert policy_grants(_policy_document([statement])) == expected


def test_oraculo_preserva_condicao_literal_e_rejeita_valor_repetido() -> None:
    statement: Node = {
        "Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "arn:table",
        "Condition": {"Null": {"dynamodb:LeadingKeys": "false"},
                      "ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["TENANT#*"]}},
    }
    condition = ('{"ForAllValues:StringLike":{"dynamodb:LeadingKeys":["TENANT#*"]},'
                 '"Null":{"dynamodb:LeadingKeys":"false"}}')
    expected = frozenset({("dynamodb:GetItem", "arn:table", condition)})
    assert policy_grants(_policy_document([statement])) == expected
    statement["Condition"]["ForAllValues:StringLike"]["dynamodb:LeadingKeys"] *= 2
    with pytest.raises(AssertionError, match="duplicate_grant"):
        policy_grants(_policy_document([statement]))


def _active_api_grants(plan: Node) -> Grants:
    resources = _runtime_resources(plan)
    table = resource_at(plan, "module.control_plane.aws_dynamodb_table.control_plane")
    indexes = table["change"]["after"]["global_secondary_index"]
    assert "gsi1" in {item["name"] for item in indexes}, "api_gsi1_producer_missing"
    machine = resource_at(plan, "module.processing.aws_sfn_state_machine.processor[0]")
    assert machine["change"]["after"]["arn"] == resources["machine"]
    return _runtime_grants(resources)["api"] | sfn_grants(resources)


def test_api_ativa_recebe_quatro_acoes_exatas_e_preserva_ddb_s3_e_role_externa(
    processing_native: NativePlans, tofu_context: Tofu, prod_root: Path,
) -> None:
    _, plans, _ = _stateful_sequence(tofu_context, prod_root, processing_native)
    for run in ("phase_b_create_plan", "phase_b_route_plan"):
        plan = plans[run]
        active = plan["variables"]["processor_active_unit_task_definition_arn"]["value"]
        assert active is not None, "api_active_expected_non_null"
        expected = _active_api_grants(plan)
        role, specification = _runtime_actor(plan, "api")
        assert_policy_grants(plan, role, specification, expected)
    address = "module.runtime_iam.aws_iam_role_policy.api[0]"
    create_plan = plans["phase_b_create_plan"]
    create_change = resource_at(create_plan, address)["change"]
    assert create_change["actions"] == ["update"]
    expected_bootstrap = _runtime_grants(_runtime_resources(create_plan))["api"]
    assert policy_grants(create_change["before"]["policy"]) == expected_bootstrap
    for field in ("name", "role"):
        assert create_change["before"][field] == create_change["after"][field]
    route_change = resource_at(plans["phase_b_route_plan"], address)["change"]
    assert route_change["actions"] == ["no-op"]
    assert route_change["before"] == route_change["after"]
