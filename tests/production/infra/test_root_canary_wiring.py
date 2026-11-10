"""Wiring canary observado no plan nativo da raiz, sem override de módulo."""

import json

import pytest

from tests.production.infra._athena_root_plan import (
    CANARY_ID,
    CANARY_PRINCIPAL,
    CLUSTER_ARN,
    PREFIX,
    REVISION,
    after_of,
    fixture_canary_plan,  # noqa: F401
    resources,
)


def test_raiz_com_canary_release_cria_roles_de_canary_e_fence(canary_plan):
    root = canary_plan["configuration"]["root_module"]
    calls = root["module_calls"]
    references = set(calls["runtime_iam"]["expressions"]["canary_release"]["references"])
    assert references == {
        "var.canary_release_id",
        "var.canary_trusted_principal_arn",
        "module.processing",
        "module.processing.canary_table_arn",
        "module.processing.canary_gsi6_arn",
        "module.processing.canary_bucket_arn",
    }
    assert root["variables"]["canary_release_id"]["default"] is None
    assert root["variables"]["canary_trusted_principal_arn"]["default"] is None
    assert not calls["runtime_iam"].get("depends_on")
    assert not calls["processing"].get("depends_on")
    roles = {
        item["address"]: item["change"]["after"] for item in resources(canary_plan, "aws_iam_role")
    }
    expected = (
        "canary_task",
        "canary_execution",
        "canary_seeder",
        "canary_verifier",
        "promotion_fence_operator",
    )
    for name in expected:
        address = f"module.runtime_iam.aws_iam_role.{name}[0]"
        assert address in roles, f"canary_role_missing={name}"
        role = roles[address]
        assert role["tags"] == {"ReleaseId": CANARY_ID}
        assert role["max_session_duration"] == 3600
    seeder = roles["module.runtime_iam.aws_iam_role.canary_seeder[0]"]
    statement = json.loads(seeder["assume_role_policy"])["Statement"]
    assert statement == [
        {
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Principal": {"AWS": CANARY_PRINCIPAL},
            "Condition": {"StringEquals": {"sts:RoleSessionName": "canary-seeder-" + CANARY_ID}},
        }
    ]


def test_raiz_com_canary_storage_cria_tabela_e_bucket(canary_plan):
    root = canary_plan["configuration"]["root_module"]
    expressions = root["module_calls"]["processing"]["expressions"]
    assert expressions["canary_storage_enabled"] == {"references": ["var.canary_storage_enabled"]}
    assert root["variables"]["canary_storage_enabled"]["default"] is False
    table = after_of(canary_plan, "module.processing.aws_dynamodb_table.canary[0]")
    assert table["name"] == PREFIX + "-canary"
    assert table["deletion_protection_enabled"] is True
    assert table["point_in_time_recovery"][0]["enabled"] is True
    bucket = after_of(canary_plan, "module.processing.aws_s3_bucket.canary[0]")
    assert bucket["bucket"] == PREFIX + "-canary"
    assert bucket["object_lock_enabled"] is True
    lock = after_of(
        canary_plan, "module.processing.aws_s3_bucket_object_lock_configuration.canary[0]"
    )
    assert lock["rule"][0]["default_retention"] == [
        {"mode": "COMPLIANCE", "days": 1, "years": None}
    ]


def test_raiz_liga_grant_run_task_do_verifier(canary_plan):
    root = canary_plan["configuration"]["root_module"]
    references = root["module_calls"]["processing"]["expressions"]["canary_verifier_role_id"][
        "references"
    ]
    assert references == ["module.runtime_iam.canary_verifier_role_id", "module.runtime_iam"]
    policy = after_of(
        canary_plan, "module.processing.aws_iam_role_policy.canary_verifier_run_task[0]"
    )
    assert policy["role"] == PREFIX + "-canary-verifier"
    assert json.loads(policy["policy"])["Statement"] == [
        {
            "Effect": "Allow",
            "Action": "ecs:RunTask",
            "Resource": REVISION % ("audit", 17),
            "Condition": {"ArnEquals": {"ecs:cluster": CLUSTER_ARN}},
        },
        {
            "Effect": "Allow",
            "Action": "ecs:DescribeTasks",
            "Resource": CLUSTER_ARN.replace(":cluster/", ":task/") + "/*",
            "Condition": {"ArnEquals": {"ecs:cluster": CLUSTER_ARN}},
        },
    ]


def test_resolver_prova_cadeia_real_e_policy_integra_do_canary(canary_plan):
    from scripts.promotion._iam_source import SourceResolver
    from tests.production.infra._athena_root_plan import ACCOUNT, canary_policy_sources

    inputs = {
        "image_digest": "sha256:" + "a" * 64,
        "canary_release_id": CANARY_ID,
        "canary_trusted_principal_arn": CANARY_PRINCIPAL,
        "canary_storage_enabled": True,
        "processor_active_unit_task_definition_arn": None,
        "processor_retained_unit_task_definition_arns": [],
    }
    resolver = SourceResolver(
        canary_plan,
        canary_policy_sources(),
        {
            "generated_inputs": inputs,
            "account_id": ACCOUNT,
            "region": "us-east-2",
        },
    )
    profiles = {
        f"module.runtime_iam.aws_iam_role_policy.{name}[0]": name
        for name in (
            "canary_task",
            "canary_execution",
            "canary_seeder",
            "canary_verifier",
            "promotion_fence_operator",
        )
    }
    profiles |= {
        "module.processing.aws_iam_role_policy.step_functions_ecs[0]": "step_functions",
        "module.processing.aws_iam_role_policy.canary_verifier_run_task[0]": "canary_verifier",
    }
    for policy, role in profiles.items():
        resolver.precheck(policy)
        assert resolver.role_producer(policy) == f"module.runtime_iam.aws_iam_role.{role}[0]"
        expected = json.loads(after_of(canary_plan, policy)["policy"])
        assert resolver.policy(policy) == expected


@pytest.mark.parametrize(
    ("count", "candidate", "expected_count"),
    [(63, 999, 64), (64, 999, 65), (127, 999, 128), (128, 128, 128), (128, 999, None)],
)
def test_uniao_ordenada_preserva_ate_128_revisoes_retidas(count, candidate, expected_count):
    from scripts.promotion._iam_expression import Budget, Evaluator, parse

    prefix = REVISION.split("%s:%d")[0] + "unit:"
    values = {
        "candidate": [prefix + str(candidate)],
        "retained": [prefix + str(index) for index in range(1, count + 1)],
    }
    budget = Budget()
    expression = parse("sort(setunion(var.candidate, var.retained))", budget)
    evaluator = Evaluator(lambda path, scope: values[path[1]], budget)
    if expected_count is None:
        with pytest.raises(ValueError, match="iam=collection_size"):
            evaluator.value(expression)
    else:
        expected = sorted(set(values["candidate"] + values["retained"]))
        assert evaluator.value(expression) == expected
        assert len(expected) == expected_count


@pytest.mark.parametrize(
    ("expression", "count", "error"),
    [
        ("sort(var.retained)", 129, "iam=collection_type"),
        ("concat(var.retained, [])", 65, "iam=collection_type"),
        ("[for item in var.retained : item]", 65, "iam=iterations"),
    ],
)
def test_operacoes_de_colecao_preservam_os_limites_aprovados(expression, count, error):
    from scripts.promotion._iam_expression import Budget, Evaluator, parse

    budget = Budget()
    node = parse(expression, budget)
    evaluator = Evaluator(lambda path, scope: [str(index) for index in range(count)], budget)
    with pytest.raises(ValueError, match=error):
        evaluator.value(node)


def test_phase_b_nativa_muda_somente_rota_e_grants_iniciais(
    canary_plan, tofu_context, prod_root, tofu_artifacts
):
    from tests.production.infra._athena_root_plan import phase_b_root_plans

    plans = phase_b_root_plans(tofu_context, prod_root, tofu_artifacts)
    for name, plan in plans.items():
        changed = {
            item["address"]
            for item in plan["resource_changes"]
            if item["mode"] == "managed" and item["change"]["actions"] != ["no-op"]
        }
        expected = {"module.processing.aws_sfn_state_machine.processor[0]"}
        if name == "phase_b_initial":
            expected.add("module.runtime_iam.aws_iam_role_policy.api[0]")
        assert changed == expected

        from scripts.promotion.phase_b import review_phase_b_plan

        active, context = _phase_b_context(plan, name, prod_root)
        assert review_phase_b_plan(plan, active, context) == (
            "initial" if active is None else "established"
        )


def _phase_b_context(plan, name, prod_root):
    variables = plan["variables"]
    unit = variables["processor_active_unit_task_definition_arn"]["value"]
    subnet = next(
        item["change"]["after"]["id"]
        for item in plan["resource_changes"]
        if item.get("type") == "aws_subnet"
    )
    security_group = next(
        item["change"]["after"]["id"]
        for item in plan["resource_changes"]
        if item.get("type") == "aws_security_group"
    )
    template = (prod_root.parents[1] / "modules/processing/state_machine.asl.json").read_text()
    values = {
        "cluster_arn": CLUSTER_ARN,
        "task_definition_arn": unit,
        "public_subnet_ids": json.dumps([subnet]),
        "security_group_id": security_group,
    }
    for key, value in values.items():
        template = template.replace("${" + key + "}", value)
    machine = {
        "name": PREFIX,
        "type": "STANDARD",
        "definition": template,
        "role_arn": "arn:aws:iam::123456789012:role/cnesdata-prod-step-functions",
    }
    active = None if name == "phase_b_initial" else REVISION % ("unit", 7)
    context = {
        "active_before": active,
        "unit_revision": unit,
        "machine_after": machine,
        "state_machine_arn": "arn:aws:states:us-east-2:123456789012:stateMachine:cnesdata-prod",
    }
    return active, context
