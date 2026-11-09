"""Canary offline: APIs/modelos reais; fixtures não provam execução AWS."""

from __future__ import annotations

import copy
import io
import json
from datetime import timedelta
from typing import Any, cast

import botocore.session
import pytest
from botocore.exceptions import ClientError
from botocore.validate import validate_parameters

from scripts.promotion import run_canary as runner
from scripts.promotion import seed_canary as seeder
from scripts.promotion import verify_canary as verifier
from tests.production.delivery.test_activation_manifest import (
    NOW,
    _reseal_phase,
    admit,
    prepare,
)
from tests.production.delivery.test_activation_manifest import (
    activation as activation_fixture,
)
from tests.production.delivery.test_activation_manifest import (
    github as github_fixture,
)

activation = activation_fixture
github = github_fixture


class Clock:
    def __init__(self):
        self.elapsed = 0.0

    def monotonic(self):
        return self.elapsed

    def sleep(self, delay):
        self.elapsed += delay

    def jitter(self):
        return 0.1

    def utcnow(self):
        return NOW + timedelta(seconds=2 + self.elapsed)


class Boundary:
    def __init__(self, clock):
        self.clock = clock
        self.calls = []
        self.responses = {}
        self.duration = 0

    def client(self, service, **kwargs):
        config = kwargs["config"]
        assert config.retries["total_max_attempts"] == 1
        assert config.connect_timeout + config.read_timeout <= 60 - self.clock.elapsed
        return Client(self, service)


class Client:
    def __init__(self, boundary, service):
        self.boundary, self.service = boundary, service

    def __getattr__(self, name):
        def invoke(**kwargs):
            model = botocore.session.get_session().get_service_model(self.service)
            operation = next(
                n
                for n in model.operation_names
                if model.operation_model(n).name == "".join(p.title() for p in name.split("_"))
            )
            shape = model.operation_model(operation).input_shape
            assert shape is not None
            validate_parameters(kwargs, shape)
            self.boundary.calls.append((self.service, name, kwargs))
            self.boundary.clock.elapsed += self.boundary.duration
            result = self.boundary.responses[name]
            if callable(result):
                result = result(kwargs)
            if isinstance(result, Exception):
                raise result
            return copy.deepcopy(result)

        return invoke


def object_response(contract):
    body = json.dumps(contract["event"], sort_keys=True, separators=(",", ":")).encode()
    return {
        "Body": io.BytesIO(body),
        "VersionId": "v-current",
        "ContentLength": len(body),
        "LastModified": NOW + timedelta(seconds=1),
    }


@pytest.fixture
def case(activation):
    from scripts.promotion._canonical import object_hash

    network = {
        "cluster": "arn:aws:ecs:us-east-2:123456789012:cluster/cnesdata-prod-processor",
        "subnets": ["subnet"],
        "security_group": "sg",
        "public_ip": "ENABLED",
        "platform": "1.4.0",
    }
    payload = json.loads(prepare(activation))
    activation["proof"]["network_sha256"] = object_hash(network)
    admitted = admit(activation, _reseal_phase(activation, payload))
    governed = {"account_id": "123456789012", "region": "us-east-2", "network": network}
    contract = runner._contract(admitted, governed)
    clock = Clock()
    boundary = Boundary(clock)
    item = seeder.pending_item(contract)
    delivered = copy.deepcopy(item)
    event = json.loads(delivered["payload"]["S"])
    event["delivered_at"] = (NOW + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
    delivered["payload"] = {"S": json.dumps(event, separators=(",", ":"))}
    for key in ("gsi6pk", "gsi6sk"):
        del delivered[key]
    boundary.responses = {
        "put_item": {},
        "query": {"Items": [item]},
        "get_item": {"Item": delivered},
    }
    boundary.responses["run_task"] = {
        "tasks": [
            {
                "taskArn": "arn:aws:ecs:us-east-2:123456789012:task/cnesdata-prod-processor/abc",
                "taskDefinitionArn": contract["revisions"]["audit"],
                "clusterArn": contract["cluster"],
                "createdAt": NOW,
            }
        ]
    }
    run = runner.run_canary(contract, boundary, clock)
    boundary.calls.clear()

    boundary.responses["get_object"] = lambda request: object_response(contract)
    boundary.responses["get_object_retention"] = {
        "Retention": {"Mode": "COMPLIANCE", "RetainUntilDate": NOW + timedelta(days=1)}
    }
    return contract, boundary, clock, run


def test_seeder_nao_le_nem_atualiza(case):
    contract, api, clock, _ = case
    receipt = seeder.seed_canary(contract, api, clock)
    assert receipt["status"] == "created"
    assert [name for _, name, _ in api.calls] == ["put_item"]
    request = api.calls[0][2]
    assert request["ConditionExpression"] == "attribute_not_exists(pk)"
    assert "ReturnValuesOnConditionCheckFailure" not in request


def test_replay_da_mesma_release_e_idempotente(case):
    contract, api, clock, _ = case
    api.responses["put_item"] = ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem"
    )
    assert seeder.seed_canary(contract, api, clock)["status"] == "exists-unverified"
    assert [name for _, name, _ in api.calls] == ["put_item"]


def test_verifier_nao_faz_scan_nem_lista(case):
    contract, api, clock, run = case
    result = verifier.verify_canary(contract, api, clock, run)
    assert result["status"] == "success"
    assert {n for _, n, _ in api.calls} == {
        "query",
        "get_item",
        "get_object",
        "get_object_retention",
    }
    assert len(result["objects"]) == 2
    assert all(o["version_id"] == "v-current" for o in result["objects"])
    get = next(k for _, n, k in api.calls if n == "get_item")
    assert get["ConsistentRead"] is True
    assert "ConditionExpression" not in get
    query = api.calls[0][2]
    assert query["IndexName"] == "gsi6"
    assert query["Limit"] == 1
    assert "gsi6sk = :sort" in query["KeyConditionExpression"]


def test_entrega_pode_sair_do_gsi_antes_da_primeira_consulta(case):
    contract, api, clock, run = case
    api.responses["query"] = {"Items": []}
    assert verifier.verify_canary(contract, api, clock, run)["status"] == "success"


def test_timeout_de_visibilidade_bloqueia_promocao(case):
    contract, api, clock, run = case
    api.responses["query"] = {"Items": []}
    api.responses["get_item"] = {}
    with pytest.raises(ValueError, match="timeout"):
        verifier.verify_canary(contract, api, clock, run)
    assert clock.elapsed == 60
    assert not any(n.startswith("get_object") for _, n, _ in api.calls)


def test_orcamento_inclui_duracao_das_chamadas(case):
    contract, api, clock, run = case
    api.duration = 31
    with pytest.raises(ValueError, match="timeout"):
        verifier.verify_canary(contract, api, clock, run)
    assert not any(n.startswith("get_object") for _, n, _ in api.calls)


@pytest.mark.parametrize(
    "mutation",
    [
        "governance",
        "expired",
        "old",
        "body",
        "version",
        "retention",
        "marker",
        "query",
        "extra-index",
        "base-mixed",
        "duplicate-json",
    ],
)
def test_exige_retencao_compliance_e_marcador_da_release(case, mutation):
    contract, api, clock, run = case
    if mutation == "governance":
        api.responses["get_object_retention"]["Retention"]["Mode"] = "GOVERNANCE"
    elif mutation == "expired":
        api.responses["get_object_retention"]["Retention"]["RetainUntilDate"] = NOW
    elif mutation in {"old", "body", "version"}:
        original = api.responses["get_object"]

        def changed(request):
            result = original(request)
            field, value = {
                "old": ("LastModified", NOW - timedelta(seconds=1)),
                "body": ("Body", io.BytesIO(b"another-release")),
                "version": ("VersionId", "null"),
            }[mutation]
            result[field] = value
            return result

        api.responses["get_object"] = changed
    elif mutation == "retention":
        api.responses["get_object_retention"] = {}
    elif mutation == "marker":
        item = api.responses["get_item"]["Item"]
        event = json.loads(item["payload"]["S"])
        event["delivered_at"] = (NOW - timedelta(seconds=1)).isoformat()
        item["payload"]["S"] = json.dumps(event)
    elif mutation == "query":
        api.responses["query"]["Items"][0]["payload"]["S"] += " "
    elif mutation == "base-mixed":
        item = api.responses["get_item"]["Item"]
        item["payload"]["S"] = item["payload"]["S"].replace(contract["release_id"], "other")
    elif mutation == "duplicate-json":
        item = api.responses["get_item"]["Item"]
        item["payload"]["S"] = item["payload"]["S"].replace("{", '{"tenant_id":"other",', 1)
    else:
        api.responses["get_item"]["Item"]["gsi6pk"] = {"S": "OUTBOX#PENDING"}
    with pytest.raises(ValueError):
        verifier.verify_canary(contract, api, clock, run)


def test_falha_parcial_s3_nao_emite_veredito(case):
    contract, api, clock, run = case
    api.responses["get_object_retention"] = ClientError(
        {"Error": {"Code": "AccessDenied"}}, "GetObjectRetention"
    )
    with pytest.raises(ClientError):
        verifier.verify_canary(contract, api, clock, run)


def test_entrada_publica_exige_activation_autenticada():
    with pytest.raises(ValueError, match="receipt_required"):
        runner.authenticate_canary(
            None, {"github": None, "s3": None}, {"activation": {}, "now": NOW}
        )


def test_runner_so_passa_roles_recursos_e_revisao_canary(case):
    contract, api, clock, _ = case
    task = "arn:aws:ecs:us-east-2:123456789012:task/cnesdata-prod-processor/abc"
    api.responses["run_task"] = {
        "tasks": [
            {
                "taskArn": task,
                "taskDefinitionArn": contract["revisions"]["audit"],
                "clusterArn": contract["cluster"],
                "createdAt": NOW,
            }
        ]
    }
    result = runner.run_canary(contract, api, clock)
    request = api.calls[0][2]
    assert result["task_arn"] == task
    assert request["taskDefinition"] == contract["revisions"]["audit"]
    assert request["cluster"] == contract["cluster"]
    assert request["overrides"]["taskRoleArn"].endswith("/cnesdata-prod-canary-task")
    assert request["overrides"]["executionRoleArn"].endswith("/cnesdata-prod-canary-execution")
    env = request["overrides"]["containerOverrides"][0]["environment"]
    assert {e["name"]: e["value"] for e in env} == {
        "AWS_CONTROL_PLANE_TABLE": "cnesdata-prod-canary",
        "AWS_AUDIT_BUCKET": "cnesdata-prod-canary",
        "AWS_AUDIT_RETENTION_DAYS": "1",
    }


@pytest.mark.parametrize(
    "field",
    [
        "table",
        "index",
        "bucket",
        "cluster",
        "task_role",
        "execution_role",
        "seeder_role",
        "verifier_role",
        "release_id",
        "candidate_sha256",
        "binding_sha256",
        "canary_id",
    ],
)
def test_recusa_release_ou_role_de_outra_ativacao(case, field):
    contract, api, clock, run = case
    contract[field] += "-foreign"
    for action in (
        lambda: seeder.seed_canary(contract, api, clock),
        lambda: runner.run_canary(contract, api, clock),
        lambda: verifier.verify_canary(contract, api, clock, run),
    ):
        with pytest.raises(ValueError):
            action()
    assert not api.calls


def test_retencao_e_da_mesma_versao_lida(case):
    contract, api, clock, run = case
    verifier.verify_canary(contract, api, clock, run)
    reads = [request for _, name, request in api.calls if name == "get_object_retention"]
    assert all(request.get("VersionId") == "v-current" for request in reads)


def test_replay_apos_delivered_preserva_origem_real_da_task(case):
    contract, api, clock, _ = case
    task = "arn:aws:ecs:us-east-2:123456789012:task/cnesdata-prod-processor/abc"
    api.responses["run_task"] = {
        "tasks": [
            {
                "taskArn": task,
                "createdAt": NOW,
                "taskDefinitionArn": contract["revisions"]["audit"],
                "clusterArn": contract["cluster"],
            }
        ]
    }
    api.responses["put_item"] = ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem"
    )
    first = runner.run_canary(contract, api, clock)
    assert verifier.verify_canary(contract, api, clock, first)["status"] == "success"
    clock.elapsed += 10
    assert seeder.seed_canary(contract, api, clock)["status"] == "exists-unverified"
    replay = runner.run_canary(contract, api, clock)
    assert replay == first
    assert verifier.verify_canary(contract, api, clock, replay)["status"] == "success"


def test_orcamento_interrompe_chamada_bloqueada_sem_esperar_timeout_sdk():
    import time

    from scripts.promotion.verify_task_revisions import SystemClock

    clock = SystemClock()
    started = time.monotonic()
    with pytest.raises(ValueError, match="timeout"):
        runner.bounded(lambda: time.sleep(0.2), clock, started + 0.03)
    assert time.monotonic() - started < 0.15


def test_recusa_inicio_autodeclarado_por_chamador(case):
    contract, api, clock, run = case
    with pytest.raises(ValueError, match="run_authority"):
        verifier.verify_canary(contract, api, clock, dict(run))


def native_runtime(case, tmp_path_factory):
    from tests.production.infra import conftest as native
    from tests.production.infra import test_canary_fence_iam as iam

    contract, _, _, _ = case
    context = cast("Any", native.tofu_context).__wrapped__(tmp_path_factory)
    root = cast("Any", native.prod_root).__wrapped__()
    request = (
        tmp_path_factory.mktemp("delivery-canary-iam"),
        root,
        ("../../modules/runtime-iam", "delivery-canary"),
        iam._role_overrides(),
    )
    base = iam.RUNTIME_BASE | {"canary_release": iam._release(contract["canary_id"])}
    plan = iam._plan(iam._execute(context, request, base, {"ativo": {}}), "ativo")
    return contract, context, root, plan


def test_politicas_renderizadas_negam_scan_recursos_e_roles_alheios(case, tmp_path_factory):
    from fnmatch import fnmatchcase

    from tests.production.infra import test_canary_fence_iam as iam

    contract, context, root, plan = native_runtime(case, tmp_path_factory)
    seeder_grants = iam._observed(plan, "canary_seeder")
    verifier_grants = iam._observed(plan, "canary_verifier")
    assert seeder_grants == iam.literal_grants(
        ("dynamodb:PutItem",), (iam.TABLE,), iam._lk(("TENANT#_GLOBAL",))
    )
    assert verifier_grants == iam._expected(contract["canary_id"])["canary_verifier"]
    forbidden = ("dynamodb:Scan", "s3:ListBucket", "dynamodb:UpdateItem", "dynamodb:PutItem")
    assert not any(
        fnmatchcase(action, pattern) for action in forbidden for pattern, _, _ in verifier_grants
    )
    objects = [resource for action, resource, _ in verifier_grants if action == "s3:GetObject"]
    wrong = [
        iam.BUCKET + "/audit/.event-id/canary-other.json",
        "arn:aws:s3:::other/audit/.event-id/canary-" + contract["canary_id"] + ".json",
    ]
    assert not any(fnmatchcase(key, pattern) for key in wrong for pattern in objects)
    roles = {
        (resource, condition)
        for action, resource, condition in verifier_grants
        if action == "iam:PassRole"
    }
    assert {resource for resource, _ in roles} == {
        contract["task_role"],
        contract["execution_role"],
    }
    assert all(
        json.loads(condition)
        == {"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}}
        for _, condition in roles
    )
    overrides = iam._processing_overrides().replace(iam.AUDIT_TASK, contract["revisions"]["audit"])
    request = (
        tmp_path_factory.mktemp("delivery-canary-ecs"),
        root,
        ("../../modules/processing", "delivery-canary-ecs"),
        overrides,
    )
    plan = iam._plan(iam._execute(context, request, iam.PROCESSING_BASE, {"ativo": {}}), "ativo")
    policy = iam.resource_at(plan, "module.subject.aws_iam_role_policy.canary_verifier_run_task[0]")
    assert iam.policy_grants(policy["change"]["after"]["policy"]) == iam.literal_grants(
        ("ecs:RunTask",),
        (contract["revisions"]["audit"],),
        {"ArnEquals": {"ecs:cluster": contract["cluster"]}},
    )


@pytest.mark.parametrize("mutation", ["partial", "revision", "origin"])
def test_runner_recusa_falha_parcial_ou_task_misturada(case, mutation):
    contract, api, clock, _ = case
    if mutation == "partial":
        api.responses["run_task"]["failures"] = [{"reason": "capacity"}]
    elif mutation == "revision":
        api.responses["run_task"]["tasks"][0]["taskDefinitionArn"] += "9"
    else:
        del api.responses["run_task"]["tasks"][0]["createdAt"]
    with pytest.raises((ValueError, KeyError)):
        runner.run_canary(contract, api, clock)


@pytest.mark.parametrize("actor", ["seeder", "verifier"])
def test_assume_sessao_exata_da_release(case, actor):
    contract, api, _, _ = case
    api.responses["assume_role"] = {
        "Credentials": {
            "AccessKeyId": "synthetic",
            "SecretAccessKey": "synthetic",
            "SessionToken": "synthetic",
        }
    }
    runner.assume_canary(contract, Client(api, "sts"), actor)
    request = api.calls[0][2]
    assert request["RoleArn"] == contract[actor + "_role"]
    assert request["RoleSessionName"] == f"canary-{actor}-{contract['canary_id']}"
    assert request["DurationSeconds"] == 900
