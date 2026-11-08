"""Wiring canary observado no plan nativo da raiz, sem override de módulo."""

import json

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
        }
    ]


def test_resolver_prova_cadeia_real_e_policy_integra_do_canary(canary_plan):
    from scripts.promotion._iam_source import SourceResolver
    from tests.production.delivery._phase_a_symbolic_fixtures import _symbolic_sources
    from tests.production.infra._athena_root_plan import ACCOUNT

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
        _symbolic_sources(),
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
