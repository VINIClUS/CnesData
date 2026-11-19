"""Replay ECS e interseção de policies offline; nenhuma execução AWS."""

from __future__ import annotations

import copy
import json
from datetime import timedelta
from fnmatch import fnmatchcase

import botocore.session
import pytest
from botocore.validate import validate_parameters

from scripts.promotion import run_canary as runner
from scripts.promotion import verify_canary as verifier
from tests.production.delivery.test_canary import Clock
from tests.production.delivery.test_canary import activation as activation_fixture
from tests.production.delivery.test_canary import case as case_fixture
from tests.production.delivery.test_canary import github as github_fixture

activation = activation_fixture
case = case_fixture
github = github_fixture


def stopped(api, original):
    task = copy.deepcopy(api.responses["run_task"]["tasks"][0])
    task.update(
        lastStatus="STOPPED",
        desiredStatus="STOPPED",
        stoppedAt=task["createdAt"] + timedelta(seconds=1, microseconds=800000),
    )
    api.responses["describe_tasks"] = {"tasks": [task], "failures": []}
    api.responses["run_task"] = {
        "tasks": [
            {"taskArn": original["task_arn"], "lastStatus": "STOPPED", "desiredStatus": "STOPPED"}
        ],
        "failures": [],
    }


def test_replay_stopped_usa_origem_nativa_da_mesma_task(case):
    contract, api, clock, original = case
    assert verifier.verify_canary(contract, api, clock, original)["status"] == "success"
    stopped(api, original)
    clock.elapsed += 10
    replay = runner.run_canary(contract, api, clock, lambda arn: api)
    assert replay == original
    request = next(r for _, n, r in api.calls if n == "describe_tasks")
    assert request == {"cluster": contract["cluster"], "tasks": [original["task_arn"]]}
    assert verifier.verify_canary(contract, api, clock, replay)["status"] == "success"


@pytest.mark.parametrize("mode", ["full", "stopped"])
@pytest.mark.parametrize(
    "field",
    [
        "taskArn",
        "taskDefinitionArn",
        "clusterArn",
        "createdAt",
        "startedBy",
        "taskRoleArn",
        "executionRoleArn",
        "environment",
    ],
)
def test_recusa_metadata_misturada_ou_origem_ausente(case, mode, field):
    contract, api, clock, original = case
    if mode == "stopped":
        stopped(api, original)
    operation = "describe_tasks" if mode == "stopped" else "run_task"
    task = api.responses[operation]["tasks"][0]
    if field == "createdAt":
        del task[field]
    elif field in {"taskRoleArn", "executionRoleArn"}:
        task["overrides"][field] += "-foreign"
    elif field == "environment":
        task["overrides"]["containerOverrides"][0]["environment"][0]["value"] += "-foreign"
    else:
        task[field] += "-foreign"
    with pytest.raises(ValueError):
        runner.run_canary(contract, api, clock, lambda arn: api)


@pytest.mark.parametrize("mutation", ["missing", "partial", "timeout", "sts-timeout"])
def test_readback_incompleto_ou_lento_falha_fechado(case, mutation):
    contract, api, clock, original = case
    stopped(api, original)
    if mutation == "missing":
        api.responses["describe_tasks"] = {"tasks": [], "failures": [{"reason": "MISSING"}]}
    elif mutation == "partial":
        api.responses["describe_tasks"]["failures"] = [{"reason": "capacity"}]
    elif mutation == "timeout":
        api.duration = 31

    def reader(arn):
        if mutation == "sts-timeout":
            clock.elapsed += 60
        return api

    with pytest.raises(ValueError):
        runner.run_canary(contract, api, clock, reader)


def test_ttl_curta_e_task_nova_nao_herdam_marcador_antigo(case):
    contract, api, clock, original = case
    assert verifier.verify_canary(contract, api, clock, original)["status"] == "success"

    class NextClock(Clock):
        def utcnow(self):
            return super().utcnow() + timedelta(seconds=3601)

    clock = api.clock = NextClock()
    task = api.responses["run_task"]["tasks"][0]
    task["taskArn"] = task["taskArn"].rsplit("/", 1)[0] + "/" + "b" * 32
    task["createdAt"] = clock.utcnow()
    new = runner.run_canary(contract, api, clock, lambda arn: api)
    assert new["task_arn"] != original["task_arn"]
    with pytest.raises(ValueError, match="old_marker"):
        verifier.verify_canary(contract, api, clock, new)


def test_policies_sts_tem_tamanho_real_e_dois_objetos_exatos(case):
    contract, _, _, original = case
    launch = verifier.session_policy(contract, "verifier", None)
    readback = verifier.session_policy(contract, "verifier", original["task_arn"])
    assert 0 < len(launch) <= 2048
    assert 0 < len(readback) <= 2048
    statements = json.loads(readback)["Statement"]
    s3 = next(s for s in statements if "s3:GetObject" in s["Action"])
    assert s3["Resource"] == [
        "arn:aws:s3:::" + contract["bucket"] + "/" + k for k in verifier.audit_keys(contract)
    ]
    assert "*" not in json.dumps(statements)


@pytest.mark.parametrize("suffix", ["*", "a/b", "a" * 31, "g" * 32])
def test_policy_recusa_arn_de_task_nao_canonico(case, suffix):
    contract, _, _, original = case
    arn = original["task_arn"].rsplit("/", 1)[0] + "/" + suffix
    with pytest.raises(ValueError):
        verifier.session_policy(contract, "verifier", arn)


def _condition(conditions, context):
    for operator, entries in conditions.items():
        for key, expected in entries.items():
            actual = context.get(key)
            if operator == "Null":
                if (actual in (None, [], "")) != (expected == "true"):
                    return False
                continue
            values = actual if isinstance(actual, list) else [actual]
            wanted = expected if isinstance(expected, list) else [expected]
            if operator not in {"ArnEquals", "StringEquals", "ForAllValues:StringEquals"}:
                return False
            if not values or not all(value in wanted for value in values):
                return False
    return True


def _allows(document, request):
    verdict = False
    for statement in document["Statement"]:
        actions = statement["Action"]
        resources = statement["Resource"]
        actions = [actions] if isinstance(actions, str) else actions
        resources = [resources] if isinstance(resources, str) else resources
        matched = (
            any(fnmatchcase(request["action"], a) for a in actions)
            and any(fnmatchcase(request["resource"], r) for r in resources)
            and _condition(statement.get("Condition", {}), request["context"])
        )
        if matched and statement["Effect"] == "Deny":
            return False
        verdict |= matched and statement["Effect"] == "Allow"
    return verdict


def effective(base, session, request):
    return (
        request["principal"] == base["principal"]
        and _allows(base["policy"], request)
        and _allows(json.loads(session), request)
    )


def assert_session_intersection(contract, runtime, processing):
    principal = contract["verifier_role"]
    assert runtime["role"] == processing["role"] == principal.rsplit("/", 1)[1]
    base = {
        "principal": principal,
        "policy": {
            "Statement": (
                json.loads(runtime["policy"])["Statement"]
                + json.loads(processing["policy"])["Statement"]
            )
        },
    }
    task = contract["cluster"].replace(":cluster/", ":task/") + "/" + "a" * 32
    other = task[:-32] + "b" * 32
    launch = verifier.session_policy(contract, "verifier", None)
    readback = verifier.session_policy(contract, "verifier", task)
    request = {
        "principal": principal,
        "action": "ecs:DescribeTasks",
        "resource": task,
        "context": {"ecs:cluster": contract["cluster"]},
    }
    assert _allows(base["policy"], request)
    assert effective(base, readback, request)
    assert not effective(base, launch, request)
    request["resource"] = other
    assert _allows(base["policy"], request)
    assert not effective(base, readback, request)
    request.update(action="ecs:RunTask", resource=contract["revisions"]["audit"])
    assert effective(base, launch, request)
    assert not effective(base, readback, request)
    request.update(
        action="iam:PassRole",
        resource=contract["task_role"],
        context={"iam:PassedToService": "ecs-tasks.amazonaws.com"},
    )
    assert effective(base, launch, request)
    assert not effective(base, readback, request)
    assert_partition_intersection(base, readback, contract)
    request["context"] = {"iam:PassedToService": "lambda.amazonaws.com"}
    assert not effective(base, launch, request)
    request.update(action="ecs:DescribeTasks", resource=task, context={"ecs:cluster": "other"})
    assert not effective(base, readback, request)
    request.update(principal=contract["seeder_role"], context={"ecs:cluster": contract["cluster"]})
    assert not effective(base, readback, request)


def assert_dispatch_composition(plan, contract):
    from cnes_infra.aws.processor_settings import AwsAuditDispatchSettings
    from tests.production.infra import test_canary_fence_iam as iam

    task = iam.resource_at(plan, 'module.subject.aws_ecs_task_definition.processor["audit"]')
    container = json.loads(task["change"]["after"]["container_definitions"])[0]
    assert container["command"] == ["dispatch-outbox-once"]
    production = {e["name"]: e["value"] for e in container["environment"]}
    baseline = AwsAuditDispatchSettings.from_mapping(production)
    assert baseline.production_policy is True
    assert baseline.audit_retention_days == 365
    request = runner._run_request(contract)
    roles = request["overrides"]
    assert (roles["taskRoleArn"], roles["executionRoleArn"]) == (
        contract["task_role"],
        contract["execution_role"],
    )
    overrides = roles["containerOverrides"][0]["environment"]
    canary = AwsAuditDispatchSettings.from_mapping(
        production | {e["name"]: e["value"] for e in overrides}
    )
    assert canary.production_policy is False
    assert canary.region == contract["region"]
    assert canary.audit_retention_days == 1
    assert (canary.control_plane_table, canary.audit_bucket) == (
        contract["table"],
        contract["bucket"],
    )
    assert canary.dynamodb_endpoint_url is None
    assert canary.service_endpoint_url is None


def native_processing(contract, context, root, factory):
    from tests.production.infra import test_canary_fence_iam as iam

    overrides = iam._processing_overrides().replace(iam.AUDIT_TASK, contract["revisions"]["audit"])
    request = (
        factory.mktemp("delivery-canary-ecs"),
        root,
        ("../../modules/processing", "delivery-canary-ecs"),
        overrides,
    )
    plan = iam._plan(iam._execute(context, request, iam.PROCESSING_BASE, {"ativo": {}}), "ativo")
    assert_dispatch_composition(plan, contract)
    return plan


def assert_partition_intersection(base, readback, contract):
    table = (
        f"arn:aws:dynamodb:{contract['region']}:{contract['account_id']}:table/" + contract["table"]
    )
    request = {
        "principal": contract["verifier_role"],
        "action": "dynamodb:Query",
        "resource": table + "/index/gsi6",
        "context": {"dynamodb:LeadingKeys": ["OUTBOX#PENDING"]},
    }
    assert effective(base, readback, request)
    request["context"]["sort-key"] = "IAM-does-not-scope-sort-keys"
    assert effective(base, readback, request)
    for key in ([], ["other"], None):
        request["context"] = {"dynamodb:LeadingKeys": key}
        assert not effective(base, readback, request)
    request.update(
        action="dynamodb:GetItem",
        resource=table,
        context={"dynamodb:LeadingKeys": ["TENANT#_GLOBAL"]},
    )
    assert effective(base, readback, request)
    request["context"] = {"dynamodb:LeadingKeys": ["TENANT#other"]}
    assert not effective(base, readback, request)
    request.update(
        action="s3:GetObject",
        context={},
        resource="arn:aws:s3:::" + contract["bucket"] + "/" + verifier.audit_keys(contract)[0],
    )
    assert effective(base, readback, request)
    request["resource"] += "-other"
    assert not effective(base, readback, request)


@pytest.mark.parametrize("mode", ["full", "stopped"])
@pytest.mark.parametrize("part", ["account", "region", "cluster", "wildcard"])
def test_recusa_arn_de_outra_identidade_no_sdk(case, mode, part):
    contract, api, clock, original = case
    if mode == "stopped":
        stopped(api, original)
    task = api.responses["describe_tasks" if mode == "stopped" else "run_task"]["tasks"][0]
    before, after = {
        "account": (contract["account_id"], "999999999999"),
        "region": (":us-east-2:", ":us-west-2:"),
        "cluster": ("/cnesdata-prod-processor/", "/other/"),
        "wildcard": ("a" * 32, "*"),
    }[part]
    task["taskArn"] = task["taskArn"].replace(before, after)
    with pytest.raises(ValueError):
        runner.run_canary(contract, api, clock, lambda arn: api)


@pytest.mark.parametrize("task", [None, "readback"])
def test_sts_recebe_policy_real_com_tamanho_modelo_e_sem_escalacao(case, task):
    from tests.production.delivery.test_activation_manifest import NOW
    from tests.production.delivery.test_canary import Client

    contract, api, _, original = case
    arn = original["task_arn"] if task is not None else None
    api.responses["assume_role"] = {
        "Credentials": {
            "AccessKeyId": "a" * 20,
            "SecretAccessKey": "b" * 40,
            "SessionToken": "c" * 32,
            "Expiration": NOW + timedelta(seconds=900),
        },
        "AssumedRoleUser": {
            "AssumedRoleId": "synthetic:id",
            "Arn": "arn:aws:sts::123456789012:assumed-role/cnesdata-prod-canary-verifier/test",
        },
    }
    shape = (
        botocore.session.get_session()
        .get_service_model("sts")
        .operation_model("AssumeRole")
        .output_shape
    )
    assert shape is not None
    validate_parameters(api.responses["assume_role"], shape)
    runner.assume_canary(contract, Client(api, "sts"), "verifier", arn)
    request = api.calls[0][2]
    assert request["Policy"] == verifier.session_policy(contract, "verifier", arn)
    assert len(request["Policy"].encode("ascii")) <= 2048
    assert request["RoleArn"] == contract["verifier_role"]
    assert request["RoleSessionName"] == "canary-verifier-" + contract["canary_id"]


@pytest.mark.parametrize("mutation", ["resource", "condition", "action", "statement", "source"])
def test_phase_a_recusa_describe_widening_e_source_metadata(mutation):
    from scripts.promotion.phase_a import review_phase_a_plan, verify_delta
    from tests.production.delivery._phase_a_symbolic_fixtures import symbolic_phase_a
    from tests.production.delivery.test_phase_a_contract import _expected_symbolic_policy

    plan, context, observed, address = symbolic_phase_a("verifier")
    policy = _expected_symbolic_policy("verifier")
    if mutation == "source":
        key = "infra/opentofu/modules/processing/canary.tf"
        context["source_files"][key] = context["source_files"][key].replace(
            b":task/${var.name_prefix}-processor/*", b":task/*"
        )
    elif mutation == "statement":
        policy["Statement"].append(copy.deepcopy(policy["Statement"][1]))
    elif mutation == "condition":
        policy["Statement"][1].pop("Condition")
    else:
        policy["Statement"][1][mutation.title()] = "*"
    observed[address]["policy"] = json.dumps(policy)
    with pytest.raises(ValueError):
        verify_delta(
            plan,
            context
            | {"graph_proof": review_phase_a_plan(plan, context), "readback_resources": observed},
        )


def assert_native_processing_policy(plan, contract, runtime):
    from tests.production.infra import test_canary_fence_iam as iam

    policy = iam.resource_at(plan, "module.subject.aws_iam_role_policy.canary_verifier_run_task[0]")
    assert_session_intersection(contract, runtime, policy["change"]["after"])
    assert iam.policy_grants(policy["change"]["after"]["policy"]) == iam.literal_grants(
        ("ecs:RunTask",),
        (contract["revisions"]["audit"],),
        {"ArnEquals": {"ecs:cluster": contract["cluster"]}},
    ) | iam.literal_grants(
        ("ecs:DescribeTasks",),
        (contract["cluster"].replace(":cluster/", ":task/") + "/*",),
        {"ArnEquals": {"ecs:cluster": contract["cluster"]}},
    )


@pytest.mark.parametrize("mode", ["full", "stopped"])
@pytest.mark.parametrize(
    "mutation", ["ordered", "cpu-zero", "duplicate-env", "extra-command", "cpu-nonzero"]
)
def test_overrides_sdk_preservam_material_sem_depender_da_ordem(case, mode, mutation):
    contract, api, clock, original = case
    if mode == "stopped":
        stopped(api, original)
    task = api.responses["describe_tasks" if mode == "stopped" else "run_task"]["tasks"][0]
    container = task["overrides"]["containerOverrides"][0]
    if mutation == "ordered":
        container["environment"].reverse()
    elif mutation == "duplicate-env":
        container["environment"].append(copy.deepcopy(container["environment"][0]))
    elif mutation == "extra-command":
        container["command"] = ["other"]
    else:
        container["cpu"] = 0 if mutation == "cpu-zero" else 1
    if mutation in {"ordered", "cpu-zero"}:
        assert runner.run_canary(contract, api, clock, lambda arn: api) == original
    else:
        with pytest.raises(ValueError):
            runner.run_canary(contract, api, clock, lambda arn: api)


@pytest.mark.parametrize("mode", ["full", "stopped"])
def test_timezone_nativa_preserva_o_mesmo_instante(case, mode):
    from datetime import timezone

    contract, api, clock, original = case
    if mode == "stopped":
        stopped(api, original)
    task = api.responses["describe_tasks" if mode == "stopped" else "run_task"]["tasks"][0]
    task["createdAt"] = task["createdAt"].astimezone(timezone(-timedelta(hours=3)))
    assert runner.run_canary(contract, api, clock, lambda arn: api) == original
